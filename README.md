# MeshSocket

A lightweight WebSocket mesh networking library. Nodes connect to a central server and communicate via broadcast, direct routing, and request/response patterns — all over a simple JSON wire protocol.

## Wire Protocol

Every message is a JSON object with four fields:

```json
{
  "id": "uuid",
  "type": "message_type",
  "payload": "<any JSON value or null>",
  "reply_to": "uuid or null"
}
```

If a handler returns a value, the receiver sends a new message with `reply_to` set to the original `id`, enabling request/response round-trips.

## Implementations

| Language | Path | Role |
|----------|------|------|
| Python | `Python/` | Server + client library |
| Swift | `Swift/` | Client library (SPM package, zero dependencies) |
| JavaScript | `JavaScript/` | Planned |

### Python

`socketCore.py` — the core `MeshSocket` class (client and server-side node).
`socket_server.py` — the mesh server with auth, broadcasting, routing, and channel isolation.

```python
from socketCore import MeshSocket

socket = MeshSocket(url="ws://localhost:8765", name="MyNode", auth_token="...")
await socket.start()
await socket.wait_until_ready()

socket.on("my_event", handler)
await socket.send("my_event", {"key": "value"})
response = await socket.request("ping")
```

### Swift

A single-file actor-based client targeting macOS 13+ / iOS 16+ with no external dependencies.

```swift
import MeshSocket

let socket = MeshSocket(url: "ws://localhost:8765", name: "MyNode", authToken: "...")
await socket.start()
await socket.waitUntilReady()

await socket.on("my_event") { payload in
    // handle
    return nil
}
try await socket.send("my_event", payload: ["key": "value"])
let response = await socket.request("ping")
```

Add via Swift Package Manager:

```swift
.package(path: "Swift/")  // local
```

## Features

- **Request/response** — `request()` sends a message and awaits a reply with configurable timeout
- **Broadcasting** — send to all nodes (scoped by channel or global)
- **Direct routing** — send to a specific node by ID and get a response back
- **Channel isolation** — nodes on different channels don't see each other's broadcasts
- **Auth** — token-based authentication on connect
- **Offline buffering** — RAM buffer with optional disk spill, auto-flush on reconnect
- **Auto-reconnect** — exponential backoff (2s initial, 30s cap)
- **Capability flags** — `can_broadcast`, `can_route`, `can_cross_channel_route`, `can_monitor`

## Running the Server

```bash
pip install websockets
MESH_AUTH_TOKEN=your-token python Python/socket_server.py
```

The server listens on `0.0.0.0:8765` by default. Set `MESH_ALLOWED_ORIGINS` to restrict WebSocket origins.

## Testing

Cross-language integration tests verify the Swift client against the real Python server using Docker.

```bash
./test.sh
```

This runs:
1. `docker compose up` — starts the Python server + echo client
2. `swift test` — runs 11 integration tests from the host
3. `docker compose down` — tears down

Requirements: Docker, Swift 5.9+.

## License

MIT
