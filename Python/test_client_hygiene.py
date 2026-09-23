"""Fail-closed defaults and client hygiene (WP1.4 of the 2026-09 hardening).

    python -m pytest test_client_hygiene.py -q
"""
import asyncio
import json
import os
import stat

import pytest

import meshsocket
from conftest import start_relay, stop_relay
from socket_server import MeshServer, MeshServerConfigError
from socketCore import MeshSocket, sanitize_identity, valid_ident


# --- fail closed: no token, no start -------------------------------------------

@pytest.mark.asyncio
async def test_start_without_token_raises(monkeypatch):
    monkeypatch.delenv("MESH_AUTH_TOKEN", raising=False)
    server = MeshServer(host="127.0.0.1", port=0)
    with pytest.raises(MeshServerConfigError, match="WITHOUT authentication"):
        await server.start()
    assert not server.started.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("kwargs, env", [
    ({"allow_anonymous": True}, None),
    ({}, "tok"),
    ({"auth_handler": lambda t, ip: True}, None),
])
async def test_start_succeeds_when_opted_in_or_authenticated(monkeypatch, kwargs, env):
    if env is None:
        monkeypatch.delenv("MESH_AUTH_TOKEN", raising=False)
    else:
        monkeypatch.setenv("MESH_AUTH_TOKEN", env)
    server = MeshServer(host="127.0.0.1", port=0, **kwargs)
    task = asyncio.create_task(server.start())
    await asyncio.wait_for(server.started.wait(), 5)
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass


def test_default_host_is_loopback(monkeypatch):
    monkeypatch.delenv("MESH_HOST", raising=False)
    assert MeshServer().host == "127.0.0.1"
    monkeypatch.setenv("MESH_HOST", "0.0.0.0")
    assert MeshServer().host == "0.0.0.0"
    assert MeshServer(host="192.168.1.5").host == "192.168.1.5"


# --- sanitize_identity: a helper for CLIENTS (the relay never sanitizes) --------

@pytest.mark.parametrize("raw, kind, expected", [
    ("Carter's iPhone", "name", "Carter-s-iPhone"),
    ("CAR-TER Remote", "name", "CAR-TER-Remote"),
    ("123.Carter's iPhone", "name", "123.Carter-s-iPhone"),
    ("phone", "name", "phone"),
    ("190003614929046.hub", "name", "190003614929046.hub"),
    ("  --weird__name--  ", "name", "weird__name"),
    ("_lead", "name", "lead"),
    ("!!!", "name", "node"),
    ("!!!", "channel", "default"),
    ("home lights", "channel", "home-lights"),
    ("a.b.c", "channel", "a-b-c"),
    ("x" * 100, "name", "x" * 64),
    ("\U0001F986 duck", "name", "duck"),
])
def test_sanitize_identity(raw, kind, expected):
    out = sanitize_identity(raw, kind)
    assert out == expected
    assert valid_ident(out)


@pytest.mark.parametrize("raw", ["", "N" * 900_000, "x" * 257, None, 5, ["a"]])
def test_sanitize_identity_rejects_non_strings_and_bad_lengths(raw):
    with pytest.raises(ValueError):
        sanitize_identity(raw)


def test_sanitize_identity_is_exported_from_the_package():
    assert meshsocket.sanitize_identity is sanitize_identity
    assert meshsocket.valid_ident is valid_ident
    assert meshsocket.__version__ == "0.2.0"


@pytest.mark.asyncio
async def test_relay_does_not_sanitize_it_closes(relay):
    from conftest import expect_close
    ws = await relay.raw()
    await relay.identify(ws, "Carter's iPhone")
    await expect_close(ws, 1008)
    ok, w = await relay.join(sanitize_identity("Carter's iPhone"))
    assert w["name"] == "Carter-s-iPhone"


# --- URL logging ---------------------------------------------------------------

@pytest.mark.parametrize("url, expected", [
    ("wss://connect.example.net/mesh?token=SECRET", "wss://connect.example.net"),
    ("ws://10.0.0.5:8765/SECRET-in-path", "ws://10.0.0.5:8765"),
    ("wss://[::1]:9000/x", "wss://[::1]:9000"),
    ("garbage", "<unparseable url>"),
])
def test_safe_url_strips_path_and_query(url, expected):
    assert MeshSocket(url=url, name="t").safe_url == expected


@pytest.mark.asyncio
async def test_connect_log_never_contains_the_token(caplog):
    relay = await start_relay()
    try:
        url = f"{relay.url}/path?token=SUPERSECRET"
        sock = MeshSocket(url=url, name="quiet", auth_token="tok")
        with caplog.at_level("INFO"):
            await sock.start()
            await asyncio.wait_for(sock.connected_event.wait(), 5)
            await sock.stop()
            await asyncio.sleep(0.1)
        joined = "\n".join(r.getMessage() for r in caplog.records)
        assert "SUPERSECRET" not in joined
        assert "token=" not in joined
        assert relay.url in joined  # scheme://host:port is still logged
    finally:
        await stop_relay(relay)


# --- offline buffer ------------------------------------------------------------

@pytest.mark.asyncio
async def test_offline_buffer_written_0600_and_validated_on_replay(tmp_path):
    path = tmp_path / "buffer.jsonl"
    sock = MeshSocket(url="ws://127.0.0.1:1", name="buf", max_offline_buffer=1,
                      offline_file_path=str(path))
    # First send lands in RAM, the second spills RAM to disk and appends.
    await sock.send("reading", {"seq": 1})
    await sock.send("reading", {"seq": 2})
    assert path.exists()
    mode = stat.S_IMODE(os.stat(path).st_mode)
    assert mode == 0o600, oct(mode)

    # Tamper: append junk, a non-object, and a forged identify.
    with open(path, "a") as f:
        f.write("not json\n")
        f.write(json.dumps([1, 2]) + "\n")
        f.write(json.dumps({"id": "x", "type": "identify", "payload": {"token": "steal"}}) + "\n")
        f.write(json.dumps({"type": "no-id"}) + "\n")

    class FakeConn:
        def __init__(self):
            self.sent = []

        async def send(self, data):
            self.sent.append(json.loads(data))

    conn = FakeConn()
    sock.connection = conn
    await sock._flush_offline_queue()
    types = [f["type"] for f in conn.sent]
    assert types == ["reading", "reading"], types
    assert not path.exists()
