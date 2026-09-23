"""Relay hardening regressions (security review 2026-09-22, MeshSocket round 2).

Each test here started life as a case in the review's probe script that
CONFIRMED a weakness against 0.1.2. They must keep failing closed.

    python -m pytest test_server_hardening.py -q
"""
import asyncio
import json
import uuid

import pytest
import websockets

from conftest import TOKEN, expect_close, recv_type


def _frame(msg_type, payload=None, reply_to=None, msg_id=None):
    return json.dumps({"id": msg_id or str(uuid.uuid4()), "type": msg_type,
                       "payload": payload, "reply_to": reply_to})


def _table(server):
    return {cid: (c.name, c.channel, c.can_monitor, c.broadcast_scope, c.can_cross_channel_route)
            for cid, c in server.clients.items()}


# --- WP1.1: identify is one-shot, welcome is client-only -----------------------

@pytest.mark.asyncio
async def test_reidentify_with_valid_token_closes_1008_and_leaves_tables_unchanged(relay):
    victim, wv = await relay.join("victim", channel="chA")
    attacker, wa = await relay.join("attacker", channel="chB")
    before = _table(relay.server)
    by_name_before = {k: v.id for k, v in relay.server.clients_by_name.items()}

    await relay.identify(attacker, "victim", channel="chA", can_monitor=True,
                         broadcast_scope="global", can_cross_channel_route=True)
    code = await expect_close(attacker, 1008)
    assert code == 1008
    await asyncio.sleep(0.2)

    # The attacker's own entry is reaped on close; nothing else moved.
    after = _table(relay.server)
    assert wa["id"] not in after
    assert {k: v for k, v in after.items()} == {k: v for k, v in before.items() if k != wa["id"]}
    assert relay.server.clients_by_name["victim"].id == wv["id"]
    assert "attacker" not in relay.server.clients_by_name
    assert by_name_before["victim"] == wv["id"]

    # The victim is still connected and served.
    await victim.send(_frame("ping"))
    reply = await recv_type(victim, "ping")
    assert reply["payload"] == "pong"


@pytest.mark.asyncio
async def test_reidentify_with_invalid_token_also_closes(relay):
    ws, _ = await relay.join("x", channel="chX")
    await relay.identify(ws, "x2", token="bad", channel="chY", can_monitor=True)
    await expect_close(ws, 1008)
    await asyncio.sleep(0.1)
    assert "x2" not in relay.server.clients_by_name
    assert "x" not in relay.server.clients_by_name


@pytest.mark.asyncio
async def test_bad_token_identify_closes_immediately(relay):
    ws = await relay.raw()
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    await relay.identify(ws, "nope", token="wrong")
    await expect_close(ws, 1008, timeout=2.0)
    assert loop.time() - t0 < 1.0, "bad token must close right away, not after the 5 s auth timer"
    assert not relay.server.clients


@pytest.mark.asyncio
async def test_client_sent_welcome_does_not_change_id_or_evict_peer(relay):
    peer, wp = await relay.join("peer", channel="chC")
    bad, wb = await relay.join("bad", channel="chC")
    server_bad = relay.server.clients[wb["id"]]

    await bad.send(_frame("welcome", {"id": wp["id"]}))
    await asyncio.sleep(0.2)
    assert server_bad.id == wb["id"], "a peer's welcome must not rewrite the server-side id"

    await bad.close()
    await asyncio.sleep(0.3)
    assert wp["id"] in relay.server.clients, "peer must remain routable"
    assert wb["id"] not in relay.server.clients
    await peer.send(_frame("ping"))
    assert (await recv_type(peer, "ping"))["payload"] == "pong"


@pytest.mark.asyncio
async def test_server_side_id_is_frozen_after_registration(relay):
    _, w = await relay.join("frozen")
    client = relay.server.clients[w["id"]]
    with pytest.raises(AttributeError):
        client.id = "hijack"
    assert client.id == w["id"]


@pytest.mark.asyncio
async def test_joiner_is_in_its_own_roster_push(relay):
    """Registration happens before welcome + roster, so the joiner's peers see it."""
    monitor, _ = await relay.join("mon", channel="r", can_monitor=True)
    # drain the roster push triggered by our own join
    await recv_type(monitor, "server_client_list")
    _, wj = await relay.join("joiner", channel="r")
    roster = await recv_type(monitor, "server_client_list")
    names = {c["name"] for c in roster["payload"]["clients"]}
    assert "joiner" in names


# --- WP1.2: pre-auth gate, node_status capability, resilient fan-out -----------

@pytest.mark.asyncio
async def test_unidentified_node_status_is_not_delivered(relay):
    listener, _ = await relay.join("listener")
    stranger = await relay.raw()
    await stranger.send(_frame("node_status", {"evil": 1}))
    with pytest.raises(asyncio.TimeoutError):
        await recv_type(listener, "node_status", timeout=0.8)


@pytest.mark.asyncio
async def test_unidentified_status_request_and_ping_are_not_answered(relay):
    stranger = await relay.raw()
    await stranger.send(_frame("status_request"))
    await stranger.send(_frame("ping"))
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(stranger.recv(), 0.8)


@pytest.mark.asyncio
async def test_node_status_requires_can_broadcast(relay):
    listener, _ = await relay.join("listener2", channel="ns")
    muted, _ = await relay.join("muted", channel="ns", can_broadcast=False)
    await muted.send(_frame("node_status", {"x": 1}, msg_id="q1"))
    reply = await recv_type(muted, "node_status")
    assert reply["reply_to"] == "q1"
    assert reply["payload"]["status"] == "failed"
    with pytest.raises(asyncio.TimeoutError):
        await recv_type(listener, "node_status", timeout=0.5)


@pytest.mark.asyncio
async def test_dead_monitor_does_not_stop_roster_delivery(relay):
    live, _ = await relay.join("live-mon", channel="rr", can_monitor=True)
    await recv_type(live, "server_client_list")
    dead, wd = await relay.join("dead-mon", channel="rr", can_monitor=True)
    await recv_type(live, "server_client_list")
    await recv_type(dead, "server_client_list")

    # Make the dead monitor's server-side send blow up the way a half-open
    # socket does (ConnectionError out of MeshSocket.send).
    server_dead = relay.server.clients[wd["id"]]

    async def boom(*a, **k):
        raise ConnectionError("half-open")
    server_dead.send = boom

    _, wj = await relay.join("joiner2", channel="rr")
    roster = await recv_type(live, "server_client_list", timeout=3)
    assert "joiner2" in {c["name"] for c in roster["payload"]["clients"]}
    await asyncio.sleep(0.3)
    assert wd["id"] not in relay.server.clients, "the dead monitor must be reaped"
    assert wj["id"] in relay.server.clients


@pytest.mark.asyncio
async def test_stalled_peer_does_not_block_broadcast(relay):
    relay.server.SEND_TIMEOUT = 0.3
    sender, _ = await relay.join("bsender", channel="bb")
    fast, _ = await relay.join("bfast", channel="bb")
    slow, ws_ = await relay.join("bslow", channel="bb")
    server_slow = relay.server.clients[ws_["id"]]

    async def hang(*a, **k):
        await asyncio.sleep(10)
    server_slow.send = hang

    await sender.send(_frame("broadcast_request", {"msg": "hi"}, msg_id="b1"))
    got = await recv_type(fast, "broadcast", timeout=1.5)
    assert got["payload"] == {"msg": "hi"}
    ack = await recv_type(sender, "broadcast_request", timeout=1.5)
    assert ack["payload"] == {"status": "sent"}
    await asyncio.sleep(0.2)
    assert ws_["id"] not in relay.server.clients, "the stalled peer must be reaped"
