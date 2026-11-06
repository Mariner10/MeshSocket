"""The relay stamps the sender on routed payloads (0.4.0, aif-qs8b0.24).

A routed payload (`route_msg`, `route_msg_noreply`) reaches its target with `_from`
set to the name the sender identified as, whatever the sender wrote there. On an
E2EE envelope the stamp sits on the outer object beside the ciphertext, which is
where CAR-TER's HubSurfaceGate reads it. Broadcasts are delivered as sent.

    python -m pytest test_relay_sender.py -q
"""
import asyncio
import json
import uuid

import pytest

from conftest import recv_type
from socket_server import SENDER_KEY, stamp_sender
from socketCore import MeshSocket

ENVELOPE = {"e2ee": 2, "s": "c2FsdHNhbHRzYWx0c2FsdA==", "n": 7,
            "ct": "Y2lwaGVydGV4dCtwb2x5MTMwNXRhZw=="}


def _frame(msg_type, payload=None, reply_to=None, msg_id=None):
    return json.dumps({"id": msg_id or str(uuid.uuid4()), "type": msg_type,
                       "payload": payload, "reply_to": reply_to})


async def _routed(relay, payload, sender="hub-a", target="phone", **sender_fields):
    """`sender` routes `payload` to `target` with route_msg_noreply; returns what the
    target received."""
    hub, _ = await relay.join(sender, **sender_fields)
    phone, _ = await relay.join(target)
    await hub.send(_frame("route_msg_noreply",
                          {"target_name": target, "type": "toast", "payload": payload}))
    return (await recv_type(phone, "toast"))["payload"]


# --- spoofing ---------------------------------------------------------------

@pytest.mark.asyncio
async def test_spoofed_from_is_overwritten_on_route_msg(relay):
    hub, _ = await relay.join("hub-a")
    phone, welcome = await relay.join("phone")
    await hub.send(_frame("route_msg", {
        "target_id": welcome["id"], "type": "prompt",
        "payload": {"id": "p1", "title": "Reboot?", "_from": "other-hub"},
    }, msg_id="r1"))

    request = await recv_type(phone, "prompt")
    assert request["payload"] == {"id": "p1", "title": "Reboot?", "_from": "hub-a"}

    # The reply path is unchanged: the target answers, the sender gets it.
    await phone.send(_frame("prompt", {"ok": True}, reply_to=request["id"]))
    reply = await recv_type(hub, "route_msg")
    assert reply["reply_to"] == "r1"
    assert reply["payload"] == {"ok": True}


@pytest.mark.asyncio
async def test_spoofed_from_is_overwritten_on_route_msg_noreply(relay):
    got = await _routed(relay, {"title": "Done", "_from": "other-hub"})
    assert got == {"title": "Done", "_from": "hub-a"}


@pytest.mark.asyncio
async def test_unstamped_payload_gains_the_sender(relay):
    assert await _routed(relay, {"title": "Done"}) == {"title": "Done", "_from": "hub-a"}


@pytest.mark.asyncio
@pytest.mark.parametrize("claim", [None, 7, ["other-hub"], {"name": "other-hub"}, "", "hub-a "])
async def test_any_supplied_from_value_is_replaced(relay, claim):
    assert (await _routed(relay, {"_from": claim}))["_from"] == "hub-a"


@pytest.mark.asyncio
async def test_stamp_is_the_registered_name(relay):
    # Behind the carter-relay gateway the relay sees `<account>.<principal>_<name>`;
    # the stamp is exactly that registry name.
    got = await _routed(relay, {"_from": "190003614929046.o_home_other"},
                        sender="190003614929046.o_home_rig")
    assert got["_from"] == "190003614929046.o_home_rig"


@pytest.mark.asyncio
async def test_identify_fields_do_not_choose_the_stamp(relay):
    got = await _routed(relay, {}, **{"_from": "other-hub", "from": "other-hub"})
    assert got == {"_from": "hub-a"}


# --- E2EE envelope placement ------------------------------------------------

@pytest.mark.asyncio
async def test_envelope_is_stamped_outside_the_ciphertext(relay):
    got = await _routed(relay, {**ENVELOPE, "_from": "other-hub"})
    # Outer `_from` beside the ciphertext; salt, counter and ciphertext untouched,
    # no other key added.
    assert got == {**ENVELOPE, "_from": "hub-a"}


@pytest.mark.asyncio
async def test_envelope_without_from_gains_the_outer_stamp(relay):
    assert await _routed(relay, dict(ENVELOPE)) == {**ENVELOPE, "_from": "hub-a"}


@pytest.mark.asyncio
async def test_route_msg_envelope_is_stamped(relay):
    hub, _ = await relay.join("hub-a")
    phone, welcome = await relay.join("phone")
    await hub.send(_frame("route_msg", {"target_id": welcome["id"], "type": "sheet",
                                        "payload": {**ENVELOPE, "_from": "other-hub"}}))
    assert (await recv_type(phone, "sheet"))["payload"] == {**ENVELOPE, "_from": "hub-a"}


# --- broadcast ----------------------------------------------------------------

@pytest.mark.asyncio
async def test_broadcast_payload_is_delivered_as_sent(relay):
    # Not stamped, on purpose: peers match acks against the `_from` they wrote
    # themselves (see MeshServer.broadcast).
    phone, _ = await relay.join("phone")
    hub, _ = await relay.join("hub-a")
    sent = {"msg_type": "light.power", "_cmd": "c-1", "_from": "Phone"}
    await phone.send(_frame("broadcast_request", sent, msg_id="b1"))
    assert (await recv_type(hub, "broadcast"))["payload"] == sent
    sealed = {**ENVELOPE, "_from": "Phone"}
    await phone.send(_frame("broadcast_request", sealed, msg_id="b2"))
    assert (await recv_type(hub, "broadcast"))["payload"] == sealed


# --- backward compatibility -------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [None, "text", 42, True, ["a", {"_from": "x"}]])
async def test_non_dict_payloads_pass_unchanged(relay, payload):
    assert await _routed(relay, payload) == payload


@pytest.mark.asyncio
async def test_existing_client_handler_sees_the_extra_key_and_still_replies(relay):
    # A 0.3.0-era MeshSocket client: its handler reads the keys it knows and
    # replies as before; the extra `_from` changes nothing for it.
    target = MeshSocket(url=relay.url, name="old-hub", auth_token="tok")
    seen = []

    @target.on("set_level")
    async def on_level(payload):
        seen.append(payload)
        return {"level": payload["level"], "status": "ok"}

    await target.start()
    try:
        await asyncio.wait_for(target.wait_until_ready(), 5)
        ws, _ = await relay.join("phone")
        await ws.send(_frame("route_msg", {"target_id": target.id, "type": "set_level",
                                           "payload": {"level": 3}}, msg_id="r1"))
        reply = await recv_type(ws, "route_msg", timeout=5)
        assert reply["payload"] == {"level": 3, "status": "ok"}
        assert seen == [{"level": 3, "_from": "phone"}]
    finally:
        await target.stop()


def test_stamp_sender_copies_and_ignores_non_dicts():
    original = {"a": 1, "_from": "other-hub"}
    stamped = stamp_sender(original, "hub-a")
    assert stamped == {"a": 1, SENDER_KEY: "hub-a"}
    assert original == {"a": 1, "_from": "other-hub"}       # caller's dict untouched
    for value in (None, "s", 3, [1, 2]):
        assert stamp_sender(value, "hub-a") is value
