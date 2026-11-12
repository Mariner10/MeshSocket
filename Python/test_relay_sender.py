"""The relay stamps the sender on routed payloads (0.4.0 aif-qs8b0.24, 0.4.1 aif-qs8b0.27).

A routed payload (`route_msg`, `route_msg_noreply`) reaches its target with `_from`
and the relay-only `_relayFrom` set to the name the sender identified as, whatever
the sender wrote there. On an E2EE envelope the stamp sits on the outer object
beside the ciphertext. `_relayFrom` is the key a receiver trusts (CAR-TER's
HubSurfaceGate): the relay removes a peer-written copy from every payload it
forwards, so a broadcast (otherwise delivered as sent, `_from` included) and a
routed reply never carry one.

    python -m pytest test_relay_sender.py -q
"""
import asyncio
import json
import uuid

import pytest

from conftest import recv_type
from socket_server import RELAY_SENDER_KEY, SENDER_KEY, stamp_sender, strip_relay_sender
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
    assert request["payload"] == {"id": "p1", "title": "Reboot?",
                                  "_from": "hub-a", "_relayFrom": "hub-a"}

    # The reply path is unchanged: the target answers, the sender gets it.
    await phone.send(_frame("prompt", {"ok": True}, reply_to=request["id"]))
    reply = await recv_type(hub, "route_msg")
    assert reply["reply_to"] == "r1"
    assert reply["payload"] == {"ok": True}


@pytest.mark.asyncio
async def test_spoofed_from_is_overwritten_on_route_msg_noreply(relay):
    got = await _routed(relay, {"title": "Done", "_from": "other-hub"})
    assert got == {"title": "Done", "_from": "hub-a", "_relayFrom": "hub-a"}


@pytest.mark.asyncio
async def test_unstamped_payload_gains_the_sender(relay):
    assert await _routed(relay, {"title": "Done"}) == {"title": "Done", "_from": "hub-a",
                                                       "_relayFrom": "hub-a"}


@pytest.mark.asyncio
@pytest.mark.parametrize("claim", [None, 7, ["other-hub"], {"name": "other-hub"}, "", "hub-a "])
async def test_any_supplied_from_value_is_replaced(relay, claim):
    assert (await _routed(relay, {"_from": claim}))["_from"] == "hub-a"


@pytest.mark.asyncio
async def test_stamp_is_the_registered_name(relay):
    # Behind the carter-relay gateway the relay sees `<account>.<principal>_<name>`;
    # the stamp is exactly that registry name.
    got = await _routed(relay, {"_from": "190003614929046.o_home_other",
                                "_relayFrom": "190003614929046.o_home_other"},
                        sender="190003614929046.o_home_rig")
    assert got["_from"] == got["_relayFrom"] == "190003614929046.o_home_rig"


@pytest.mark.asyncio
async def test_identify_fields_do_not_choose_the_stamp(relay):
    got = await _routed(relay, {}, **{"_from": "other-hub", "from": "other-hub",
                                      "_relayFrom": "other-hub"})
    assert got == {"_from": "hub-a", "_relayFrom": "hub-a"}


# --- E2EE envelope placement ------------------------------------------------

@pytest.mark.asyncio
async def test_envelope_is_stamped_outside_the_ciphertext(relay):
    got = await _routed(relay, {**ENVELOPE, "_from": "other-hub"})
    # Outer `_from` and `_relayFrom` beside the ciphertext; salt, counter and
    # ciphertext untouched, no other key added.
    assert got == {**ENVELOPE, "_from": "hub-a", "_relayFrom": "hub-a"}


@pytest.mark.asyncio
async def test_envelope_without_from_gains_the_outer_stamp(relay):
    assert await _routed(relay, dict(ENVELOPE)) == {**ENVELOPE, "_from": "hub-a",
                                                    "_relayFrom": "hub-a"}


@pytest.mark.asyncio
async def test_route_msg_envelope_is_stamped(relay):
    hub, _ = await relay.join("hub-a")
    phone, welcome = await relay.join("phone")
    await hub.send(_frame("route_msg", {"target_id": welcome["id"], "type": "sheet",
                                        "payload": {**ENVELOPE, "_from": "other-hub",
                                                    "_relayFrom": "other-hub"}}))
    assert (await recv_type(phone, "sheet"))["payload"] == {**ENVELOPE, "_from": "hub-a",
                                                           "_relayFrom": "hub-a"}


# --- the relay-only key ------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("claim", ["other-hub", "hub-a", None, 7, {"name": "hub-a"}])
async def test_forged_relay_key_on_a_routed_payload_is_restamped(relay, claim):
    # Whatever the sender writes under `_relayFrom` (even its own true name), the
    # target sees the relay's value.
    got = await _routed(relay, {"title": "Done", "_relayFrom": claim})
    assert got == {"title": "Done", "_from": "hub-a", "_relayFrom": "hub-a"}


@pytest.mark.asyncio
async def test_forged_relay_key_on_route_msg_is_restamped(relay):
    hub, _ = await relay.join("hub-a")
    phone, welcome = await relay.join("phone")
    await hub.send(_frame("route_msg", {"target_id": welcome["id"], "type": "prompt",
                                        "payload": {"id": "p1", "_relayFrom": "other-hub"}}))
    got = (await recv_type(phone, "prompt"))["payload"]
    assert got == {"id": "p1", "_from": "hub-a", "_relayFrom": "hub-a"}


@pytest.mark.asyncio
async def test_relay_key_sits_on_the_outer_envelope_only(relay):
    # A forged outer copy is replaced; nothing is added inside or beside the
    # ciphertext beyond the two stamps.
    got = await _routed(relay, {**ENVELOPE, "_relayFrom": "other-hub"})
    assert got == {**ENVELOPE, "_from": "hub-a", "_relayFrom": "hub-a"}
    assert {k: got[k] for k in ENVELOPE} == ENVELOPE


@pytest.mark.asyncio
async def test_a_routed_reply_never_carries_the_relay_key(relay):
    # The target writes the reply; a `_relayFrom` in it is the target's claim.
    hub, _ = await relay.join("hub-a")
    phone, welcome = await relay.join("phone")
    await hub.send(_frame("route_msg", {"target_id": welcome["id"], "type": "prompt",
                                        "payload": {"id": "p1"}}, msg_id="r1"))
    request = await recv_type(phone, "prompt")
    await phone.send(_frame("prompt", {"ok": True, "_relayFrom": "other-hub", "_from": "x"},
                            reply_to=request["id"]))
    reply = await recv_type(hub, "route_msg")
    assert reply["reply_to"] == "r1"
    assert reply["payload"] == {"ok": True, "_from": "x"}


# --- broadcast ----------------------------------------------------------------

@pytest.mark.asyncio
async def test_broadcast_payload_is_delivered_as_sent(relay):
    # Not stamped, on purpose: peers match acks against the `_from` they wrote
    # themselves (see MeshServer.broadcast). `_from` is never touched.
    phone, _ = await relay.join("phone")
    hub, _ = await relay.join("hub-a")
    sent = {"msg_type": "light.power", "_cmd": "c-1", "_from": "Phone"}
    await phone.send(_frame("broadcast_request", sent, msg_id="b1"))
    assert (await recv_type(hub, "broadcast"))["payload"] == sent
    sealed = {**ENVELOPE, "_from": "Phone"}
    await phone.send(_frame("broadcast_request", sealed, msg_id="b2"))
    assert (await recv_type(hub, "broadcast"))["payload"] == sealed


@pytest.mark.asyncio
@pytest.mark.parametrize("request_type,delivered_type", [
    ("broadcast_request", "broadcast"),
    ("iCloud_data_Broadcast", "iCloudListen"),
    ("place_visit_Broadcast", "placeVisitListen"),
    ("request_prediction", "request_prediction"),
    ("prediction_result", "prediction_result"),
    ("service_log", "service_log"),
])
async def test_forged_relay_key_is_stripped_from_every_broadcast(relay, request_type,
                                                                 delivered_type):
    # The hole 0.4.1 closes: a hub writes `_relayFrom` (and `_from`) on a sealed
    # broadcast to pass as another hub. The relay key is removed; `_from` stays
    # exactly as the sender wrote it, so acks keep working.
    phone, _ = await relay.join("phone")
    hub, _ = await relay.join("hub-a")
    forged = {**ENVELOPE, "_from": "WorkshopHub", "_relayFrom": "WorkshopHub"}
    await hub.send(_frame(request_type, forged))
    got = (await recv_type(phone, delivered_type))["payload"]
    assert got == {**ENVELOPE, "_from": "WorkshopHub"}
    assert RELAY_SENDER_KEY not in got

    plain = {"msg_type": "light.power", "_cmd": "c-1", "_from": "Phone",
             "_relayFrom": "WorkshopHub"}
    await hub.send(_frame(request_type, plain))
    got = (await recv_type(phone, delivered_type))["payload"]
    assert got == {"msg_type": "light.power", "_cmd": "c-1", "_from": "Phone"}


@pytest.mark.asyncio
async def test_node_status_broadcast_is_stripped_too(relay):
    # node_status is a relay control event for the app; a peer's copy still goes.
    phone, _ = await relay.join("phone")
    hub, _ = await relay.join("hub-a")
    await hub.send(_frame("node_status", {"online": True, "_relayFrom": "other-hub"}))
    assert (await recv_type(phone, "node_status"))["payload"] == {"online": True}


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
        assert seen == [{"level": 3, "_from": "phone", "_relayFrom": "phone"}]
    finally:
        await target.stop()


@pytest.mark.asyncio
async def test_an_old_client_never_reads_the_relay_key(relay):
    # A 0.4.0-era receiver that knows only `_from` gets the same `_from` as before
    # on a routed payload, and a broadcast's `_from` as the sender wrote it.
    assert (await _routed(relay, {"_from": "other-hub"}))["_from"] == "hub-a"
    phone, _ = await relay.join("phone2")
    hub, _ = await relay.join("hub-b")
    await phone.send(_frame("broadcast_request", {"_cmd": "c-1", "_from": "Phone"}))
    assert (await recv_type(hub, "broadcast"))["payload"]["_from"] == "Phone"


def test_strip_relay_sender_copies_and_ignores_non_dicts():
    original = {"a": 1, "_from": "x", RELAY_SENDER_KEY: "other-hub",
                "nested": {RELAY_SENDER_KEY: "kept"}}
    stripped = strip_relay_sender(original)
    assert stripped == {"a": 1, "_from": "x", "nested": {RELAY_SENDER_KEY: "kept"}}
    assert RELAY_SENDER_KEY in original                      # caller's dict untouched
    clean = {"a": 1}
    assert strip_relay_sender(clean) is clean
    for value in (None, "s", 3, [{RELAY_SENDER_KEY: "x"}]):
        assert strip_relay_sender(value) is value


def test_stamp_sender_copies_and_ignores_non_dicts():
    original = {"a": 1, "_from": "other-hub"}
    stamped = stamp_sender(original, "hub-a")
    assert stamped == {"a": 1, SENDER_KEY: "hub-a", RELAY_SENDER_KEY: "hub-a"}
    assert original == {"a": 1, "_from": "other-hub"}       # caller's dict untouched
    for value in (None, "s", 3, [1, 2]):
        assert stamp_sender(value, "hub-a") is value


@pytest.mark.asyncio
async def test_route_msg_by_target_name_gets_the_reply(relay):
    """route_msg may name its target like route_msg_noreply (CAR-TER hub audio asks the
    hub by name: `{target_name, type: audio_command, payload: {verb: hello}}`)."""
    phone, _ = await relay.join("phone")
    hub, _ = await relay.join("music-hub")
    await phone.send(_frame("route_msg", {
        "target_name": "music-hub", "type": "audio_command", "payload": {"verb": "hello"},
    }, msg_id="r-name"))

    request = await recv_type(hub, "audio_command")
    assert request["payload"]["verb"] == "hello"
    assert request["payload"]["_relayFrom"] == "phone"
    await hub.send(_frame("audio_command", {"msg_type": "audio_state", "rev": 1}, reply_to=request["id"]))
    reply = await recv_type(phone, "route_msg")
    assert reply["reply_to"] == "r-name"
    assert reply["payload"]["msg_type"] == "audio_state"


@pytest.mark.asyncio
async def test_route_msg_unknown_target_name_is_not_found(relay):
    phone, _ = await relay.join("phone")
    await phone.send(_frame("route_msg", {
        "target_name": "nobody", "type": "audio_command", "payload": {}}, msg_id="r-none"))
    reply = await recv_type(phone, "route_msg")
    assert reply["payload"] == {"error": "Target not found", "status": "failed"}
