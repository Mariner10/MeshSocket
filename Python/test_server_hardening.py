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
