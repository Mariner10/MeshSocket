"""Relay counters, limits and validation (WP1.3 of the 2026-09 hardening).

    python -m pytest test_server_limits.py -q
"""
import asyncio
import json
import uuid

import pytest
import websockets

from conftest import TOKEN, expect_close, recv_type, start_relay, stop_relay
from socket_server import MeshServer, valid_ident
from socketCore import MeshSocket


def _frame(msg_type, payload=None, reply_to=None, msg_id=None):
    return json.dumps({"id": msg_id or str(uuid.uuid4()), "type": msg_type,
                       "payload": payload, "reply_to": reply_to})


# --- identity grammar --------------------------------------------------------

@pytest.mark.parametrize("value", [
    "phone", "hub", "tmux-bridge", "cider-bridge-v2", "Client-4402", "a" * 64,
    "CAR-TER-Remote-ab12f", "190003614929046.hub", "1.phone-ab12f", "0" * 32 + ".x",
])
def test_valid_idents(value):
    assert valid_ident(value)


@pytest.mark.parametrize("value", [
    None, 5, "", "a.b c\n", "\U0001F986", ["l"], {"d": 1}, "-lead", "_lead",
    "a" * 65, "N" * 900_000, "1.2.3", "abc.def", "0" * 33 + ".x", "12.", "12.-x",
    "Carter's iPhone", "CAR-TER Remote",
])
def test_invalid_idents(value):
    assert not valid_ident(value)


@pytest.mark.asyncio
@pytest.mark.parametrize("name", [None, 5, "", "a.b c\n", "\U0001F986", ["l"], {"d": 1}, "x" * 65])
async def test_bad_name_closes_1008(relay, name):
    ws = await relay.raw()
    await relay.identify(ws, name)
    await expect_close(ws, 1008)
    await asyncio.sleep(0.05)
    assert not relay.server.clients


@pytest.mark.asyncio
@pytest.mark.parametrize("channel", [["l"], 7, "bad channel", "a.b.c"])
async def test_bad_channel_closes_1008(relay, channel):
    ws = await relay.raw()
    await relay.identify(ws, "okname", channel=channel)
    await expect_close(ws, 1008)


@pytest.mark.asyncio
async def test_empty_or_missing_channel_means_default(relay):
    _, w1 = await relay.join("nochan")
    ws2 = await relay.raw()
    await relay.identify(ws2, "emptychan", channel="")
    w2 = (await recv_type(ws2, "welcome"))["payload"]
    assert relay.server.clients[w1["id"]].channel == "default"
    assert relay.server.clients[w2["id"]].channel == "default"


@pytest.mark.asyncio
async def test_namespaced_gateway_names_are_accepted(relay):
    _, w = await relay.join("190003614929046.phone-1a2b3", channel="190003614929046.home")
    assert relay.server.clients[w["id"]].name == "190003614929046.phone-1a2b3"


@pytest.mark.asyncio
async def test_huge_frame_is_refused_by_max_size(relay):
    assert relay.server.max_size == 256 * 1024
    ws = await relay.raw()
    await relay.identify(ws, "N" * 900_000)
    code = await expect_close(ws, timeout=5)
    assert code == 1009, "a 900 KB identify must exceed max_size (256 KiB)"
    assert not relay.server.clients


@pytest.mark.asyncio
async def test_role_and_scope_are_sanitized(relay):
    _, w = await relay.join("roley", role=["not", "a", "string"], broadcast_scope="everything")
    c = relay.server.clients[w["id"]]
    assert c.role == "node"
    assert c.broadcast_scope == "channel"


# --- counters ----------------------------------------------------------------

@pytest.mark.asyncio
async def test_pending_counter_releases_at_auth_so_many_live_sockets_from_one_ip_work(relay):
    assert relay.server.max_pending_per_ip == 10
    socks = []
    for i in range(12):
        ws, _ = await relay.join(f"same-ip-{i}")
        socks.append(ws)
    assert len(relay.server.clients) == 12
    assert relay.server._pending_by_ip.get("127.0.0.1", 0) == 0
    # all still alive
    for ws in socks:
        await ws.send(_frame("ping"))
        assert (await recv_type(ws, "ping"))["payload"] == "pong"


@pytest.mark.asyncio
async def test_eleventh_unidentified_socket_from_one_ip_gets_4429(relay):
    idle = [await relay.raw() for _ in range(10)]
    await asyncio.sleep(0.1)
    extra = await relay.raw()
    code = await expect_close(extra, 4429)
    assert code == 4429
    # once the idle ones go away, the slot frees immediately
    for ws in idle:
        await ws.close()
    await asyncio.sleep(0.3)
    assert relay.server._pending_by_ip.get("127.0.0.1", 0) == 0
    _, w = await relay.join("after-idle")
    assert w["name"] == "after-idle"


@pytest.mark.asyncio
async def test_max_connections_cap():
    r = await start_relay(max_connections=2)
    try:
        await r.join("one")
        await r.join("two")
        third = await r.raw()
        assert await expect_close(third, 4429) == 4429
        assert r.server._open_conns == 2
    finally:
        await stop_relay(r)


@pytest.mark.asyncio
async def test_env_knobs(monkeypatch):
    monkeypatch.setenv("MESH_MAX_CONNECTIONS", "77")
    monkeypatch.setenv("MESH_MAX_PENDING_PER_IP", "3")
    monkeypatch.setenv("MESH_RATE_LIMIT", "9")
    monkeypatch.setenv("MESH_MAX_SIZE", "1024")
    monkeypatch.setenv("MESH_TRUSTED_PROXIES", "10.0.0.1, 127.0.0.1")
    s = MeshServer()
    assert (s.max_connections, s.max_pending_per_ip, s.rate_limit, s.max_size) == (77, 3, 9, 1024)
    assert s.trusted_proxies == {"10.0.0.1", "127.0.0.1"}
    assert s.ping_interval == 20 and s.ping_timeout == 20


def test_defaults(monkeypatch):
    for k in ("MESH_MAX_CONNECTIONS", "MESH_MAX_PENDING_PER_IP", "MESH_RATE_LIMIT",
              "MESH_MAX_SIZE", "MESH_TRUSTED_PROXIES"):
        monkeypatch.delenv(k, raising=False)
    s = MeshServer()
    assert s.max_connections == 2000
    assert s.max_pending_per_ip == 10
    assert s.rate_limit == 50
    assert s.max_size == 256 * 1024
    assert s.trusted_proxies == set()


# --- forwarded IP headers ----------------------------------------------------

class _FakeWS:
    def __init__(self, peer, headers):
        self.remote_address = (peer, 1234)
        self.request_headers = headers


def test_forwarded_headers_ignored_unless_trusted(monkeypatch):
    monkeypatch.delenv("MESH_TRUSTED_PROXIES", raising=False)
    s = MeshServer()
    ws = _FakeWS("203.0.113.9", {"X-Forwarded-For": "1.1.1.1, 2.2.2.2", "X-Real-IP": "3.3.3.3"})
    assert s._client_ip(ws) == "203.0.113.9"


def test_forwarded_headers_honored_from_trusted_proxy():
    s = MeshServer(trusted_proxies={"127.0.0.1"})
    assert s._client_ip(_FakeWS("127.0.0.1", {"X-Forwarded-For": "1.1.1.1, 2.2.2.2"})) == "1.1.1.1"
    assert s._client_ip(_FakeWS("127.0.0.1", {"X-Real-IP": "3.3.3.3", "X-Forwarded-For": "1.1.1.1"})) == "3.3.3.3"
    assert s._client_ip(_FakeWS("127.0.0.1", {})) == "127.0.0.1"
    assert s._client_ip(_FakeWS("10.9.9.9", {"X-Forwarded-For": "1.1.1.1"})) == "10.9.9.9"


# --- origin ------------------------------------------------------------------

def test_origin_exact_match(monkeypatch):
    monkeypatch.delenv("MESH_ALLOWED_ORIGINS", raising=False)
    s = MeshServer()
    assert s._origin_allowed(None)
    assert s._origin_allowed("http://localhost")
    assert s._origin_allowed("http://LOCALHOST/")
    assert not s._origin_allowed("http://localhost.evil.com")
    assert not s._origin_allowed("http://localhost:3000")
    assert not s._origin_allowed("https://localhost")
    monkeypatch.setenv("MESH_ALLOWED_ORIGINS", "https://app.example.com:8443")
    assert s._origin_allowed("https://app.example.com:8443")
    assert not s._origin_allowed("https://app.example.com")


@pytest.mark.asyncio
async def test_bad_origin_is_closed_on_the_wire(relay):
    ws = await websockets.connect(relay.url, additional_headers={"Origin": "http://localhost.evil.com"})
    try:
        await expect_close(ws, 1008)
    finally:
        await ws.close()
    ok = await websockets.connect(relay.url, additional_headers={"Origin": "http://localhost"})
    try:
        await relay.identify(ok, "browser")
        await recv_type(ok, "welcome")
    finally:
        await ok.close()


# --- auth --------------------------------------------------------------------

def test_default_auth_uses_env_token_constant_time(monkeypatch):
    monkeypatch.setenv("MESH_AUTH_TOKEN", "s3cret")
    assert MeshServer._default_auth("s3cret", "1.2.3.4")
    assert not MeshServer._default_auth("s3cre", "1.2.3.4")
    assert not MeshServer._default_auth(None, "1.2.3.4")
    assert not MeshServer._default_auth(["s3cret"], "1.2.3.4")
    monkeypatch.delenv("MESH_AUTH_TOKEN")
    assert MeshServer._default_auth("anything", "1.2.3.4")


# --- replies and routing -----------------------------------------------------

@pytest.mark.asyncio
async def test_unmatched_reply_is_dropped_not_dispatched(relay):
    ws, _ = await relay.join("stray")
    await ws.send(_frame("ping", "pong", reply_to="never-pending"))
    with pytest.raises(asyncio.TimeoutError):
        await recv_type(ws, "ping", timeout=0.6)
    # and a real request still works afterwards
    await ws.send(_frame("ping"))
    assert (await recv_type(ws, "ping"))["payload"] == "pong"


@pytest.mark.asyncio
async def test_route_target_timeout_returns_error_reply(relay):
    relay.server.ROUTE_TIMEOUT = 0.3
    a, wa = await relay.join("router-a", channel="rt")
    b, wb = await relay.join("router-b", channel="rt")   # never answers "slow"
    await a.send(_frame("route_msg", {"target_id": wb["id"], "type": "slow", "payload": {}}, msg_id="r1"))
    reply = await recv_type(a, "route_msg", timeout=2)
    assert reply["reply_to"] == "r1"
    assert reply["payload"] == {"error": "target timeout", "status": "failed"}


@pytest.mark.asyncio
async def test_outstanding_route_cap(relay):
    relay.server.ROUTE_TIMEOUT = 2.0
    a, wa = await relay.join("cap-a", channel="cap")
    b, wb = await relay.join("cap-b", channel="cap")
    for i in range(MeshServer.MAX_OUTSTANDING_ROUTES):
        await a.send(_frame("route_msg", {"target_id": wb["id"], "type": "slow", "payload": i}, msg_id=f"r{i}"))
    await asyncio.sleep(0.2)
    await a.send(_frame("route_msg", {"target_id": wb["id"], "type": "slow", "payload": "x"}, msg_id="over"))
    reply = await recv_type(a, "route_msg", timeout=1.0)
    assert reply["reply_to"] == "over"
    assert reply["payload"]["error"] == "too many outstanding routes"


@pytest.mark.asyncio
async def test_malformed_route_payloads_get_error_replies(relay):
    a, _ = await relay.join("mal")
    await a.send(_frame("route_msg", [1], msg_id="m1"))
    assert (await recv_type(a, "route_msg"))["payload"]["status"] == "failed"
    await a.send(_frame("route_msg", {"target_id": [1], "type": "x"}, msg_id="m2"))
    assert (await recv_type(a, "route_msg"))["payload"]["status"] == "failed"
    await a.send(_frame("route_msg_noreply", "str", msg_id="m3"))
    assert (await recv_type(a, "route_msg_noreply"))["payload"]["status"] == "failed"


# --- rate limit and inbound concurrency ---------------------------------------

@pytest.mark.asyncio
async def test_rate_limit_closes_1008():
    r = await start_relay(rate_limit=5)
    try:
        ws, _ = await r.join("fast")
        for _ in range(10):
            await ws.send(_frame("ping"))
        assert await expect_close(ws, 1008, timeout=3) == 1008
    finally:
        await stop_relay(r)


class _ScriptedConnection:
    """Async-iterable stand-in for a websocket that yields canned frames."""
    def __init__(self, frames):
        self._frames = list(frames)
        self.sent = []

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._frames:
            raise StopAsyncIteration
        return self._frames.pop(0)

    async def send(self, data):
        self.sent.append(data)

    async def close(self, *a, **k):
        pass


@pytest.mark.asyncio
async def test_inbound_handlers_are_bounded_by_semaphore():
    n = 200
    conn = _ScriptedConnection(_frame("slow", i) for i in range(n))
    node = MeshSocket(connection=conn, name="bounded")
    peak = 0
    active = 0
    done = 0

    async def slow(_payload):
        nonlocal peak, active, done
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01)
        active -= 1
        done += 1
        return None

    node.on("slow", slow)
    await node._listen_loop()
    for _ in range(100):
        if done == n:
            break
        await asyncio.sleep(0.02)
    assert done == n
    assert peak <= MeshSocket.MAX_INFLIGHT
    assert peak > 1
