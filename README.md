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
pip install websockets certifi
MESH_AUTH_TOKEN=your-token python Python/socket_server.py
```

The server listens on `0.0.0.0:8765` by default. Starting **without** `MESH_AUTH_TOKEN`
admits every socket that can reach the port; since 0.2.0 that emits a `DeprecationWarning`
unless you construct `MeshServer(allow_anonymous=True)`, and 0.3.0 will refuse to start
(and bind `127.0.0.1` by default).

Limits and knobs (environment variable, default):

| Variable | Default | Meaning |
|---|---|---|
| `MESH_AUTH_TOKEN` | unset | Shared token clients must present in `identify` (constant-time compare) |
| `MESH_ALLOWED_ORIGINS` | `http://127.0.0.1,http://localhost` | Exact scheme+host+port match for browser `Origin` headers |
| `MESH_MAX_CONNECTIONS` | `2000` | Open sockets, identified or not (4429 beyond) |
| `MESH_MAX_PENDING_PER_IP` | `10` | Sockets per client IP that have not finished `identify` |
| `MESH_RATE_LIMIT` | `50` | Inbound frames per second per connection (1008 beyond) |
| `MESH_MAX_SIZE` | `262144` | Largest inbound frame in bytes (1009 beyond) |
| `MESH_TRUSTED_PROXIES` | empty | Comma list of proxy IPs whose `X-Forwarded-For` / `X-Real-IP` are believed |

Client `name` and `channel` must match `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$` (or the
gateway's `<digits>.<ident>` form); anything else is closed with 1008. `identify` is
accepted once per connection. See `CHANGELOG.md` for the full 0.2.0 behavior list.

## Testing

Cross-language integration tests verify the Swift client against the real Python server using Docker.

```bash
./test.sh
```

This runs:
1. `docker compose up` — starts the Python server + echo client
2. `swift test` — runs the unit tests plus the integration tests from the host (the integration tests skip themselves when no relay is listening)
3. `docker compose down` — tears down

Requirements: Docker, Swift 5.9+.

## License

MIT
