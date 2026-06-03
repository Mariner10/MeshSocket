import asyncio
import os
import sys
import logging

sys.path.insert(0, "/app/lib")
from socketCore import MeshSocket

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")


async def main():
    url = os.getenv("MESH_SERVER_URL", "ws://mesh-server:8765")
    token = os.getenv("MESH_AUTH_TOKEN", "test-token")

    socket = MeshSocket(
        url=url,
        name="EchoClient",
        auth_token=token,
        channel="default",
        role="node",
        can_broadcast=True,
        can_route=True,
    )

    @socket.on("echo")
    async def handle_echo(payload):
        return payload

    @socket.on("echo_test")
    async def handle_echo_test(payload):
        if isinstance(payload, dict):
            return {"echoed": payload.get("data", "none")}
        return {"echoed": payload}

    await socket.start()
    await socket.wait_until_ready()
    logging.info("Echo client connected and ready")

    try:
        await asyncio.Future()
    except asyncio.CancelledError:
        await socket.stop()


if __name__ == "__main__":
    asyncio.run(main())
