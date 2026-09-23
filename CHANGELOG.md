# Changelog

All notable changes to MeshSocket (the Python `meshsocket` package, the Python
relay `socket_server.py`, and the Swift `MeshSocket` package) are recorded here.

## Unreleased (0.2.0)

Hardening release driven by the 2026-09-22 security review of the CAR-TER stack.
Clean slate: no compatibility shims for 0.1.x behavior. Clients that identify once
with a valid name and never forge `welcome` keep working; everything else is closed,
and a relay without a token no longer starts.

### Breaking / behavior changes (relay)

- **identify is one-shot.** A second `identify` on an authenticated socket closes
  it with 1008 instead of re-running name eviction and capability assignment.
- **Bad-token identify closes immediately** (1008) instead of hanging until the
  5 s auth timer.
- **Name and channel validation.** `name` and `channel` must match
  `^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$` or the gateway's namespaced form
  `^[0-9]{1,32}\.[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`. Anything else (empty names,
  whitespace, non-strings, more than 64 chars) closes 1008. A missing or empty
  channel still means `default`. Names with spaces or punctuation (for example a
  device name such as `Carter's iPhone`) are no longer accepted; sanitize them
  client-side.
- **Pre-auth gate.** Until identify succeeds, no frame other than `identify` is
  processed (no `ping`, `status_request`, `node_status`, replies).
- `node_status` requires `can_broadcast` like every other broadcast verb.
- **Stray replies are dropped.** A frame whose `reply_to` matches no pending
  request is discarded instead of being dispatched as a fresh request of its type.
  Same rule in both clients.
- `route_msg` target timeouts now return `{"error": "target timeout",
  "status": "failed"}` instead of no reply; at most 16 routed requests may be
  outstanding per client; malformed route payloads get an error reply.
- Origin check is an exact scheme+host+port match against `MESH_ALLOWED_ORIGINS`
  (previously prefix matching, which let `http://localhost.evil.com` through).
- Defaults tightened: `max_size` 256 KiB (was 1 MiB), `rate_limit` 50 frames/s per
  connection (was unlimited), explicit `ping_interval=20`/`ping_timeout=20`.
- **Fail closed.** `MeshServer.start()` raises `MeshServerConfigError` when it would
  run with the default auth handler and no `MESH_AUTH_TOKEN`, unless constructed with
  `allow_anonymous=True` (which logs a WARNING). Set the token, pass `auth_handler=`,
  or opt in explicitly.
- **Loopback by default.** `MeshServer()` binds `127.0.0.1` (was `0.0.0.0`). Pass
  `host="0.0.0.0"` or set `MESH_HOST` to serve a LAN or a container network; the shipped
  `Dockerfile.python` sets `MESH_HOST=0.0.0.0`.

### Fixed (relay)

- A client-sent `welcome` could rewrite a server-side node's id and knock a peer
  out of the routing table. `welcome`, `handshake` and `status_request` handlers are
  now registered in client mode only, and a node's id is frozen once registered.
- One dead monitor socket aborted roster delivery for everyone (and, with a ghost
  entry, permanently). Roster and broadcast fan-outs now use
  `gather(return_exceptions=True)` with a 2 s per-send timeout and reap peers whose
  send fails or stalls.
- The joiner is registered in `clients`/`clients_by_name` before its `welcome` and
  the roster push, so peers' rosters include it (the "roster arrives before the
  joiner is registered" bug).
- The per-IP pending counter was released at disconnect, so it counted live
  connections and became a global 10-socket ceiling behind a proxy. It is now
  released when auth resolves; a separate `MESH_MAX_CONNECTIONS` cap bounds open
  sockets. A socket that closes before identifying releases its slot at once.
- Table pops are identity-checked everywhere (`clients.get(id) is client`).
- Default token comparison uses `hmac.compare_digest`.

### Added

- Relay env knobs: `MESH_MAX_CONNECTIONS` (2000), `MESH_MAX_PENDING_PER_IP` (10),
  `MESH_RATE_LIMIT` (50), `MESH_MAX_SIZE` (262144), `MESH_TRUSTED_PROXIES`
  (comma-separated proxy IPs whose `X-Forwarded-For`/`X-Real-IP` are believed;
  default empty = never). All are also `MeshServer(...)` arguments.
- `MeshServer.started` (asyncio.Event) and `MeshServer.bound_port` for embedding
  and tests (`port=0` works).
- `MeshSocket.authorize` pre-dispatch hook, `MeshSocket.freeze_id()`,
  `MeshSocket.safe_url`, `MeshSocket.MAX_INFLIGHT` (64 concurrent inbound handlers
  per connection).
- `meshsocket.sanitize_identity(value, kind)` and `meshsocket.valid_ident(value)`:
  the identity grammar as importable helpers. The relay does not sanitize (it closes
  1008); clients call `sanitize_identity` before identifying, e.g.
  `"Carter's iPhone" -> "Carter-s-iPhone"`, `"123.Carter's iPhone" -> "123.Carter-s-iPhone"`.
  Same rule for the gateway and the app.
- `MESH_HOST` env knob for the bind address.
- Swift: `pingInterval`/`pongTimeout` keepalive with `pauseKeepalive()` /
  `resumeKeepalive()`; `init(validating:)` throwing initializer for bad URLs; one
  `URLSession` per socket.
- Python client: explicit, configurable `ping_interval`/`ping_timeout`
  (defaults 30/10) on `websockets.connect`.
- Tests: `Python/conftest.py` relay fixture, `test_server_hardening.py`,
  `test_server_limits.py`, `test_client_hygiene.py`, `test_client_keepalive.py`;
  Swift `ReplyMatchingTests`, `KeepaliveTests`.

### Changed (clients)

- Python client logs `scheme://host[:port]` only when connecting (never the path or
  query, which may carry a token).
- Offline buffer files are created 0600 and each line is validated as a frame this
  client wrote before it is replayed.
- Python `request()` registers the pending id before sending, so a fast reply is
  never lost.

### Docker / CI

- `Dockerfile.python`: non-root user, pinned `websockets`/`certifi`, `HEALTHCHECK`.
- `publish.yml`: SHA-pinned actions, `environment: pypi`, pytest job gating the
  build/publish.

### Upgrading a relay from 0.1.x

1. Set `MESH_AUTH_TOKEN` (or pass `auth_handler=`); the server no longer starts open.
2. If the relay must be reachable off-box, set `MESH_HOST=0.0.0.0` (containers: the
   shipped Dockerfile does this).
3. Make sure every client's `name`/`channel` matches the grammar; run device names
   through `meshsocket.sanitize_identity` first.

## 0.1.2

- fix: the connection supervisor must never die quietly.

## 0.1.1

- Refresh PyPI page (README badges); CI on Node-24 action majors.

## 0.1.0

- First PyPI release of the Python client as `meshsocket`.
