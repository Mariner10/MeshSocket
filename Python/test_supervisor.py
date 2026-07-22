"""The connection supervisor must never die quietly.

`_maintain_connection` is the only thing keeping a client reconnecting. If it
exits, the client is dead until something calls start() again — so every exit
path has to leave a log line, and a fault-stop has to be distinguishable from
an ordinary shutdown. A silent exit here once left a "connected" service that
was never coming back, with nothing in 1.2M log lines to say so.

    python -m pytest test_supervisor.py -q
"""
import asyncio
import json

import pytest
import websockets

from socketCore import MeshSocket


async def _relay(ws):
    """Admit anything that identifies."""
    async for raw in ws:
        if json.loads(raw).get("type") == "identify":
            await ws.send(json.dumps({"id": "m1", "type": "welcome",
                                      "payload": {"id": "srv-1"}, "reply_to": None}))


async def _connected_client(port):
    sock = MeshSocket(name="T", url=f"ws://127.0.0.1:{port}", auth_token="t")
    await sock.start()
    await asyncio.wait_for(sock.connected_event.wait(), timeout=5)
    return sock


@pytest.mark.asyncio
async def test_fault_stop_logs_error_and_records_reason(caplog):
    async with websockets.serve(_relay, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        sock = await _connected_client(port)
        with caplog.at_level("INFO"):
            await sock.stop(reason="device revoked by validator")
            await asyncio.sleep(0.2)

        assert sock.stopped_reason == "device revoked by validator"
        assert sock.is_running is False
        errors = [r for r in caplog.records if r.levelname == "ERROR"]
        assert any("STOPPED" in r.getMessage() for r in errors), \
            "a fault-stop must be logged at ERROR"
        assert any("device revoked by validator" in r.getMessage() for r in errors), \
            "the fault reason must reach the log"


@pytest.mark.asyncio
async def test_clean_shutdown_still_logs_but_not_as_error(caplog):
    async with websockets.serve(_relay, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        sock = await _connected_client(port)
        with caplog.at_level("INFO"):
            await sock.stop()                      # no reason == intentional
            await asyncio.sleep(0.2)

        assert sock.stopped_reason is None
        msgs = [r.getMessage() for r in caplog.records]
        assert any("supervisor stopped (shutdown)" in m for m in msgs), \
            "even an intentional shutdown must leave a line"
        assert not [r for r in caplog.records
                    if r.levelname == "ERROR" and "supervisor" in r.getMessage()], \
            "an intentional shutdown must not be logged as a fault"


@pytest.mark.asyncio
async def test_supervisor_task_is_referenced():
    """An unreferenced task can be GC'd mid-flight, killing reconnects silently."""
    async with websockets.serve(_relay, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        sock = await _connected_client(port)
        assert sock._maintain_task is not None
        assert not sock._maintain_task.done()
        await sock.stop()


@pytest.mark.asyncio
async def test_cancelled_supervisor_is_still_reported(caplog):
    """Cancellation is not an excuse to die quietly either."""
    async with websockets.serve(_relay, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        sock = await _connected_client(port)
        with caplog.at_level("INFO"):
            sock._maintain_task.cancel()
            await asyncio.sleep(0.2)

        # is_running is still True -> the exit was NOT requested -> loud.
        assert any(r.levelname == "ERROR" and "exited unexpectedly" in r.getMessage()
                   for r in caplog.records), \
            "a supervisor cancelled while still running must be logged at ERROR"
        await sock.stop()
