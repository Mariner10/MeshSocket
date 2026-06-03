# Swift MeshSocket Port — Design Spec

**Date:** 2026-06-03
**Scope:** Client-only Swift port of Python MeshSocket + cross-language test harness

## 1. Wire Protocol Contract

Every message is a JSON object with exactly four fields:

```json
{
  "id": "uuid-string",
  "type": "message_type_string",
  "payload": "<any JSON value or null>",
  "reply_to": "uuid-string or null"
}
```

**Request/Response pattern:** A sender includes its `id`. If the receiver's handler returns a non-nil value, the receiver sends a new message with `reply_to` set to the original `id`. The sender matches `reply_to` against pending continuations to resolve them.

**Identity handshake:** On connect, the client sends `type: "identify"` with a payload containing `name`, `id`, `token`, and optional `channel`, `role`, capability flags (`can_broadcast`, `can_route`, `can_cross_channel_route`, `can_monitor`), and `broadcast_scope`.

**Built-in message types:** `handshake`, `ping`, `status_request`, `identify`, `broadcast_request`, `route_msg`, `route_msg_noreply`.

Both implementations must produce and consume identical JSON. No changes to the Python side.

## 2. Swift MeshSocket Architecture

The Swift library contains one primary type: **`MeshSocket` as an `actor`**. Using an actor provides thread-safe mutable state (connection, handlers, buffers, pending requests) without manual locking. All public methods are `async`.

### API Mapping

| Python | Swift |
|--------|-------|
| `MeshSocket(url=..., name=..., auth_token=..., channel=..., ...)` | `MeshSocket(url:name:authToken:channel:role:...)` |
| `await socket.start()` | `await socket.start()` |
| `await socket.stop()` | `await socket.stop()` |
| `await socket.wait_until_ready()` | `await socket.waitUntilReady()` |
| `socket.on("type", handler)` | `socket.on("type") { payload in ... }` |
| `await socket.send("type", payload)` | `await socket.send("type", payload:)` |
| `await socket.request("type", payload, timeout=5)` | `await socket.request("type", payload:, timeout:)` |
| `await socket.report_status(metrics)` | `await socket.reportStatus(metrics:)` |
| `socket.emit(...)` (alias) | `socket.emit(...)` (alias) |

### Key Internals

- **Transport:** `URLSessionWebSocketTask` — zero dependencies
- **`_maintainConnection()`** — reconnect loop with exponential backoff (2s initial, 30s cap), mirrors Python's `_maintain_connection`
- **`_listenLoop()`** — reads messages via recursive `.receive()`, spawns `Task` per packet (mirrors `asyncio.create_task`)
- **`_processPacket()`** — resolves pending requests via `reply_to`, or dispatches to registered handlers
- **Handlers:** stored as `[String: (Any?) async -> Any?]` dictionary
- **Pending requests:** `CheckedContinuation` wrapped in a dictionary keyed by message ID, with timeout via `withThrowingTaskGroup`
- **Connected event:** `AsyncStream`-based signaling replacing Python's `asyncio.Event`

### Configuration Options (same as Python)

`maxOfflineBuffer`, `offlineFilePath`, `onReconnect`, `onDisconnect`, `authToken`, `channel`, `role`, `canBroadcast`, `canRoute`, `canCrossChannelRoute`, `canMonitor`, `broadcastScope`

### No Server Mode

The Python `MeshSocket` has dual-mode (client via `start()`, server-side via `listen()` with an existing connection). The Swift version only supports client mode. The `listen()` / server-wrapping path stays Python-only.

## 3. Offline Buffering

Direct port of the Python two-tier buffering strategy.

**RAM buffer:** An `[String]` array holding serialized JSON packets. When `send()` is called while disconnected and `maxOfflineBuffer > 0`, packets accumulate here. If the array exceeds `maxOfflineBuffer`, behavior depends on whether a file path is configured.

**Disk spill:** If `offlineFilePath` is set and RAM buffer is full, the entire RAM buffer is dumped to disk (appended, one JSON packet per line), then cleared. New packets go directly to disk. File I/O runs on a background thread to avoid blocking the actor.

**Flush on reconnect:** When connection is re-established (after `identify` is sent), disk buffer is read and sent first (oldest data), then RAM buffer is flushed. Disk file is deleted after successful flush. Same ordering as Python.

**No buffering (default):** `maxOfflineBuffer = 0` means `send()` throws `MeshSocketError.notConnected` when disconnected (equivalent to Python's `ConnectionError`).

**`request()` is never buffered** — it requires an active connection. If disconnected, it awaits `waitUntilReady()` first (same as Python).

## 4. Testing Apparatus

Cross-language verification system proving the Swift client works against the real Python server.

### Infrastructure

**Docker Compose** with two services:
- `mesh-server` — runs Python `socket_server.py` on port 8765. Mounts `Python/` as `/app/lib/` so the `from lib.socketCore import` path resolves correctly. Always tests current code.
- `echo-client` — a Python helper node that connects to the server and echoes back payloads, enabling round-trip tests.

Swift tests run on the host machine (`swift test`) and connect to `localhost:8765`.

### Test Lifecycle

1. `docker compose up -d` starts the Python server + echo client
2. `swift test` runs the Swift test suite
3. `docker compose down` tears down
4. A shell script (`test.sh`) wraps all three steps

### Test Cases

| Test | What it verifies |
|------|-----------------|
| Connection & auth | Swift client connects, sends `identify`, server accepts. Also tests bad token rejection. |
| Ping/pong | Built-in `ping` handler returns `"pong"` via request/response. |
| Handshake | Latency measurement round-trip — client sends timestamp, server responds with delta. |
| Send & broadcast | Swift client A sends `broadcast_request`, Swift client B receives the `broadcast` message. Two concurrent MeshSocket instances. |
| Request/response | `request()` with timeout — happy path returns payload, timeout path returns nil. |
| Direct routing | Client A sends `route_msg` targeting Client B by ID, gets response back through the server. |
| Offline buffering | Disconnect the server, queue messages, reconnect, verify buffered messages arrive in order. |
| Channel isolation | Two clients on different channels — broadcasts don't cross. |
| Reconnection | Kill and restart the server container — verify the client reconnects and fires the `onReconnect` callback. |

### Python Echo Client

`test_helpers/echo_client.py` — connects as a node to the mesh, registers handlers that echo payloads back. Enables round-trip tests where Swift sends and Python responds.

### Maintainability

When the Python version gets updates, re-run `./test.sh`. If the wire protocol changes, tests fail and show exactly which message type broke. Docker setup mounts live source — always tests current code.

## 5. Package Structure

```
MeshSocket/
├── Python/
│   ├── socketCore.py              # (existing)
│   └── socket_server.py           # (existing)
├── Swift/
│   ├── Package.swift              # SPM manifest, macOS 13+ / iOS 16+, zero deps
│   ├── Sources/
│   │   └── MeshSocket/
│   │       └── MeshSocket.swift   # Full client implementation (~400-500 lines)
│   └── Tests/
│       └── MeshSocketTests/
│           └── MeshSocketIntegrationTests.swift
├── test_helpers/
│   └── echo_client.py             # Python echo node for cross-language tests
├── docker-compose.yml             # Server + echo client services
├── Dockerfile.python              # Python image for server & echo client
└── test.sh                        # Orchestrates: docker up → swift test → docker down
```

One Swift source file. One test file. Zero external dependencies. `Package.swift` targets macOS 13+ / iOS 16+.
