"""Client keepalive (WP1.5): a relay that stops answering pings is detected by
the client's own ping/pong timer, and the supervisor reconnects.

    python -m pytest test_client_keepalive.py -q
"""
import asyncio
import json

import pytest
import websockets

from socketCore import MeshSocket


@pytest.mark.asyncio
async def test_server_that_stops_answering_pings_triggers_reconnect():
    connections = 0
    welcomed = asyncio.Event()
    release = asyncio.Event()

    async def relay(ws):
        nonlocal connections
        connections += 1
        mine = connections
        async for raw in ws:
            if json.loads(raw).get("type") == "identify":
                await ws.send(json.dumps({"id": "m1", "type": "welcome",
                                          "payload": {"id": f"srv-{mine}"}, "reply_to": None}))
                if mine == 1:
                    # Go deaf: nothing is read from this socket any more, so the
                    # client's pings are never answered (the pong is sent by the
                    # frame parser, which no longer runs).
                    ws.transport.pause_reading()
                    welcomed.set()
                    await release.wait()

    # The server's own keepalive is off so only the CLIENT timer can notice.
    async with websockets.serve(relay, "127.0.0.1", 0, ping_interval=None) as server:
        port = server.sockets[0].getsockname()[1]
        ups, downs = [], []
        sock = MeshSocket(name="ka", url=f"ws://127.0.0.1:{port}", auth_token="t",
                          on_reconnect=lambda: ups.append(1), on_disconnect=lambda: downs.append(1),
                          ping_interval=0.3, ping_timeout=0.3, close_timeout=0.5)
        await sock.start()
        await asyncio.wait_for(sock.connected_event.wait(), 5)
        await asyncio.wait_for(welcomed.wait(), 5)
        assert len(ups) == 1

        # 0.3 s until the first ping + 0.3 s pong timeout + 0.5 s close wait,
        # then a 2 s backoff before the redial.
        for _ in range(150):
            if connections >= 2 and len(ups) >= 2:
                break
            await asyncio.sleep(0.1)
        assert downs, "the deaf server must be detected as a disconnect"
        assert connections >= 2, "the supervisor must dial again"
        assert len(ups) >= 2, "on_reconnect must fire for the fresh connection"
        await sock.stop()
        release.set()


def test_keepalive_defaults():
    s = MeshSocket(url="ws://127.0.0.1:1", name="d")
    assert (s.ping_interval, s.ping_timeout, s.close_timeout) == (30.0, 10.0, 5.0)
    off = MeshSocket(url="ws://127.0.0.1:1", name="d", ping_interval=None)
    assert off.ping_interval is None
