"""Shared fixtures for the MeshSocket test suite.

`relay` starts a real `MeshServer` on 127.0.0.1 (ephemeral port) with the auth
token "tok" and tears it down after the test. `raw` opens plain `websockets`
clients against it so tests can send arbitrary frames (the probe cases from the
2026-09 security review live in test_server_hardening.py).
"""
import asyncio
import json
import logging
import uuid

import pytest
import pytest_asyncio
import websockets

from socket_server import MeshServer

TOKEN = "tok"


class Relay:
    def __init__(self, server: MeshServer, task: asyncio.Task):
        self.server = server
        self.task = task
        self.url = f"ws://127.0.0.1:{server.bound_port}"
        self._conns = []

    async def raw(self):
        ws = await websockets.connect(self.url, open_timeout=5)
        self._conns.append(ws)
        return ws

    async def identify(self, ws, name, token=TOKEN, **fields):
        payload = {"name": name, "token": token, **fields}
        await ws.send(json.dumps({"id": str(uuid.uuid4()), "type": "identify",
                                  "payload": payload, "reply_to": None}))

    async def join(self, name, **fields):
        """Connect + identify + wait for welcome. Returns (ws, welcome_payload)."""
        ws = await self.raw()
        await self.identify(ws, name, **fields)
        welcome = await recv_type(ws, "welcome")
        return ws, welcome["payload"]

    async def close_all(self):
        for ws in self._conns:
            try:
                await ws.close()
            except Exception:
                pass
        self._conns.clear()


async def recv_type(ws, msg_type, timeout=2.0):
    """Read frames until one of `msg_type` arrives (or raise TimeoutError)."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise asyncio.TimeoutError(f"no {msg_type!r} frame within {timeout}s")
        frame = json.loads(await asyncio.wait_for(ws.recv(), remaining))
        if frame.get("type") == msg_type:
            return frame


async def expect_close(ws, code=None, timeout=2.0):
    """Assert the server closes `ws` (optionally with `code`); returns the close code."""
    try:
        while True:
            await asyncio.wait_for(ws.recv(), timeout)
    except websockets.exceptions.ConnectionClosed as exc:
        rcvd = exc.rcvd
        got = rcvd.code if rcvd is not None else None
        if code is not None:
            assert got == code, f"expected close {code}, got {got} ({exc})"
        return got
    raise AssertionError("socket was not closed")


async def start_relay(**kwargs) -> Relay:
    kwargs.setdefault("host", "127.0.0.1")
    kwargs.setdefault("port", 0)
    kwargs.setdefault("auth_handler", lambda token, ip: token == TOKEN)
    server = MeshServer(**kwargs)
    task = asyncio.create_task(server.start())
    await asyncio.wait_for(server.started.wait(), timeout=5)
    return Relay(server, task)


async def stop_relay(relay: Relay):
    await relay.close_all()
    relay.task.cancel()
    try:
        await relay.task
    except (asyncio.CancelledError, Exception):
        pass


@pytest_asyncio.fixture
async def relay():
    logging.getLogger("websockets").setLevel(logging.CRITICAL)
    r = await start_relay()
    try:
        yield r
    finally:
        await stop_relay(r)
