# MeshSocket (Python)

[![PyPI](https://img.shields.io/pypi/v/meshsocket.svg)](https://pypi.org/project/meshsocket/)
[![Downloads](https://static.pepy.tech/badge/meshsocket)](https://pepy.tech/project/meshsocket)
[![Python versions](https://img.shields.io/pypi/pyversions/meshsocket.svg)](https://pypi.org/project/meshsocket/)

A lightweight WebSocket **mesh networking** library. Nodes connect to a central relay and
communicate by **broadcast**, **direct routing**, and **request/response** — all over a
simple JSON wire protocol.

## Install

```bash
pip install meshsocket
```

## Quickstart (client)

```python
import asyncio
from meshsocket import MeshSocket

async def main():
    sock = MeshSocket(url="ws://localhost:8765", name="my-node",
                      channel="demo", role="device", can_broadcast=True)

    async def on_toggle(payload):
        print("got", payload)
        return {"ok": True}            # a returned value is delivered as the reply

    sock.on("toggle", on_toggle)

    await sock.start()                 # connect + identify
    await sock.wait_until_ready()
    await sock.send("reading", {"temp_c": 21.4})   # broadcast to the channel
    await asyncio.sleep(60)
    await sock.stop()

asyncio.run(main())
```

## Wire protocol

Every message is a JSON object:

```json
{ "id": "uuid", "type": "message_type", "payload": "<any JSON or null>", "reply_to": "uuid or null" }
```

If a handler returns a value, the receiver sends a reply with `reply_to` set to the
original `id`, enabling request/response round-trips.

## Compatibility

`from meshsocket import MeshSocket` is the supported import. The historical
`from socketCore import MeshSocket` still works (that module is published alongside the
package) so existing code keeps running while it migrates.

## Other language clients

Swift (SPM, zero-dependency) and a planned JavaScript client live in the same repo:
<https://github.com/Mariner10/MeshSocket>
