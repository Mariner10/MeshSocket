import asyncio
import hmac
import re
import uuid
import warnings
import websockets
from socketCore import MeshSocket, LogColors
import logging
import os
from typing import Any, Callable, Dict, Optional, Set

# Identity grammar shared with the carter-relay gateway. Raw client names and
# channels are plain identifiers; the gateway emits `<account digits>.<ident>`.
# Anything else is closed with 1008 — names are echoed into every roster push,
# the welcome frame and the logs, so they must be short and printable.
IDENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
NAMESPACED_RE = re.compile(r"^[0-9]{1,32}\.[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")


def valid_ident(value: Any) -> bool:
    return isinstance(value, str) and bool(IDENT_RE.match(value) or NAMESPACED_RE.match(value))


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        logging.warning(f"{name}={raw!r} is not an integer; using {default}")
        return default


def _env_set(name: str) -> Set[str]:
    return {item.strip() for item in os.getenv(name, "").split(",") if item.strip()}


class MeshServer:
    # Per-peer send budget for fan-outs (roster + broadcast). A peer that cannot
    # take a frame within this window is stalled or gone and gets reaped.
    SEND_TIMEOUT = 2.0
    # How long a routed request may wait on its target before the requester
    # gets an explicit error reply.
    ROUTE_TIMEOUT = 5.0
    # Outstanding `route_msg` requests one client may have in flight at once.
    MAX_OUTSTANDING_ROUTES = 16
    # How long an unidentified socket may sit before it is closed.
    AUTH_TIMEOUT = 5.0

    def __init__(self,
                 host: str = "0.0.0.0",
                 port: int = 8765,
                 rate_limit: Optional[int] = None,
                 max_size: Optional[int] = None,
                 auth_handler: Optional[Callable] = None,
                 on_startup: Optional[Callable] = None,
                 on_authenticated: Optional[Callable] = None,
                 max_connections: Optional[int] = None,
                 max_pending_per_ip: Optional[int] = None,
                 trusted_proxies: Optional[Set[str]] = None,
                 ping_interval: Optional[float] = 20.0,
                 ping_timeout: Optional[float] = 20.0,
                 allow_anonymous: bool = False):
        self.host = host
        self.port = port
        # Explicit opt-in for running without any token. 0.2.0 warns when the
        # default auth handler has no MESH_AUTH_TOKEN and this is False;
        # 0.3.0 will refuse to start (see CHANGELOG).
        self.allow_anonymous = allow_anonymous
        # Limits: constructor argument, else MESH_* environment knob, else default.
        #   MESH_RATE_LIMIT          inbound frames per second per connection (50)
        #   MESH_MAX_SIZE            largest inbound frame in bytes (256 KiB)
        #   MESH_MAX_CONNECTIONS     open sockets, identified or not (2000)
        #   MESH_MAX_PENDING_PER_IP  unidentified sockets per client IP (10)
        #   MESH_TRUSTED_PROXIES     comma list of proxy IPs whose X-Forwarded-For /
        #                            X-Real-IP is believed (default: none)
        self.rate_limit = rate_limit if rate_limit is not None else _env_int("MESH_RATE_LIMIT", 50)
        self.max_size = max_size if max_size is not None else _env_int("MESH_MAX_SIZE", 256 * 1024)
        self.max_connections = (max_connections if max_connections is not None
                                else _env_int("MESH_MAX_CONNECTIONS", 2000))
        self.max_pending_per_ip = (max_pending_per_ip if max_pending_per_ip is not None
                                   else _env_int("MESH_MAX_PENDING_PER_IP", 10))
        self.trusted_proxies = (set(trusted_proxies) if trusted_proxies is not None
                                else _env_set("MESH_TRUSTED_PROXIES"))
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout

        # Counters. `_pending_by_ip` counts sockets that have NOT yet resolved
        # auth (decremented the moment `authenticated` resolves, not at
        # disconnect); `_open_conns` counts every accepted socket until it closes.
        self._pending_by_ip: Dict[str, int] = {}
        self._open_conns = 0

        # auth_handler(token, remote_ip) → bool
        # Default: compare against MESH_AUTH_TOKEN env var.
        self._auth_handler = auth_handler or self._default_auth
        # on_startup() — called once before the server starts accepting connections.
        self._on_startup = on_startup
        # on_authenticated(client, remote_ip, token) — called after a client passes auth.
        self._on_authenticated = on_authenticated

        self.clients: Dict[str, MeshSocket] = {}
        self.clients_by_name: Dict[str, MeshSocket] = {}

        # Set once the listening socket is bound; `bound_port` is the real port
        # (useful when constructed with port=0).
        self.started = asyncio.Event()
        self.bound_port: int = port
        self._server = None

    @staticmethod
    def _default_auth(token: Any, remote_ip: str) -> bool:
        server_token = os.getenv("MESH_AUTH_TOKEN")
        if not server_token:
            return True
        if not isinstance(token, str):
            return False
        return hmac.compare_digest(token.encode("utf-8"), server_token.encode("utf-8"))

    def _parse_bool(self, value: Any, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return default

    def _same_channel(self, source: MeshSocket, target: MeshSocket) -> bool:
        return getattr(source, "channel", "default") == getattr(target, "channel", "default")

    def _can_receive_broadcast(self, source: MeshSocket, target: MeshSocket) -> bool:
        if getattr(source, "broadcast_scope", "channel") == "global":
            return True
        return self._same_channel(source, target)

    def _client_summary(self, client: MeshSocket) -> Dict[str, str]:
        return {
            "id": client.id,
            "name": client.name,
            "channel": getattr(client, "channel", "default"),
            "role": getattr(client, "role", "node"),
        }

    @staticmethod
    def _request_headers(websocket):
        request_headers = getattr(websocket, "request_headers", None)
        if request_headers is None and getattr(websocket, "request", None) is not None:
            request_headers = websocket.request.headers
        return request_headers

    def _client_ip(self, websocket) -> str:
        peer = websocket.remote_address[0] if getattr(websocket, "remote_address", None) else "unknown"

        # Forwarded-IP headers are attacker-controlled unless the socket comes
        # from a proxy we run. Only then do they override the TCP peer address.
        if peer in self.trusted_proxies:
            request_headers = self._request_headers(websocket)
            if request_headers:
                x_real_ip = request_headers.get("X-Real-IP")
                if x_real_ip and x_real_ip.strip():
                    return x_real_ip.strip()
                x_forwarded_for = request_headers.get("X-Forwarded-For")
                if x_forwarded_for:
                    first_hop = x_forwarded_for.split(",", 1)[0].strip()
                    if first_hop:
                        return first_hop
        return peer

    @staticmethod
    def _normalize_origin(origin: str) -> str:
        return origin.strip().rstrip("/").lower()

    def _origin_allowed(self, origin: Optional[str]) -> bool:
        """Exact scheme+host+port match against MESH_ALLOWED_ORIGINS.

        Non-browser clients send no Origin and are always allowed; a browser's
        Origin must equal an allowed entry (no prefix matching, so
        `http://localhost.evil.com` does not pass for `http://localhost`).
        """
        if not origin:
            return True
        allowed = {
            self._normalize_origin(o)
            for o in os.getenv("MESH_ALLOWED_ORIGINS", "http://127.0.0.1,http://localhost").split(",")
            if o.strip()
        }
        return self._normalize_origin(origin) in allowed

    def _auth_is_open(self) -> bool:
        return self._auth_handler is self._default_auth and not os.getenv("MESH_AUTH_TOKEN")

    async def start(self):
        if self._auth_is_open() and not self.allow_anonymous:
            warnings.warn(
                "MeshServer is starting WITHOUT authentication: MESH_AUTH_TOKEN is unset and no "
                "auth_handler was given. Every socket that can reach this port is admitted. Set "
                "MESH_AUTH_TOKEN, pass auth_handler=, or pass allow_anonymous=True to opt in "
                "explicitly. MeshSocket 0.3.0 will refuse to start in this configuration.",
                DeprecationWarning,
                stacklevel=2,
            )
            logging.warning(f"{LogColors.FAIL}Starting with NO authentication (MESH_AUTH_TOKEN unset). "
                            f"This becomes an error in MeshSocket 0.3.0.{LogColors.ENDC}")
        if self._on_startup:
            self._on_startup()
        logging.info(f"{LogColors.HEADER}Starting Server on ws://{self.host}:{self.port}{LogColors.ENDC}")
        async with websockets.serve(
            self._handle_connection,
            self.host,
            self.port,
            max_size=self.max_size,
            ping_interval=self.ping_interval,
            ping_timeout=self.ping_timeout,
        ) as server:
            self._server = server
            # Port 0 means "pick one" — record what the OS chose so tests and
            # embedders can find the server.
            try:
                self.bound_port = server.sockets[0].getsockname()[1]
            except Exception:
                self.bound_port = self.port
            self.started.set()
            await asyncio.Future()

    async def _handle_connection(self, websocket):
        remote_ip = self._client_ip(websocket)
        logging.info(f"New connection from {remote_ip}")

        if self._open_conns >= self.max_connections:
            logging.warning(
                f"{LogColors.FAIL}Rejected {remote_ip} — "
                f"{self._open_conns} connections open (MESH_MAX_CONNECTIONS={self.max_connections}){LogColors.ENDC}"
            )
            await websocket.close(4429, "Too many connections")
            return

        pending = self._pending_by_ip.get(remote_ip, 0)
        if pending >= self.max_pending_per_ip:
            logging.warning(
                f"{LogColors.FAIL}Rejected {remote_ip} — "
                f"{pending} pending connections already{LogColors.ENDC}"
            )
            await websocket.close(4429, "Too many pending connections")
            return

        self._open_conns += 1
        self._pending_by_ip[remote_ip] = pending + 1
        pending_released = False

        def _release_pending(*_):
            # Runs when auth resolves (either way) or at disconnect, whichever
            # comes first — but only once. The pending counter measures sockets
            # waiting on identify, NOT live connections.
            nonlocal pending_released
            if pending_released:
                return
            pending_released = True
            left = self._pending_by_ip.get(remote_ip, 1) - 1
            if left <= 0:
                self._pending_by_ip.pop(remote_ip, None)
            else:
                self._pending_by_ip[remote_ip] = left

        try:
            client = MeshSocket(
                connection=websocket,
                name=f"Client-{id(websocket)}",
                rate_limit=self.rate_limit,
            )
            client.channel = "default"
            client.role = "unknown"
            client.can_broadcast = False
            client.can_route = False
            client.can_cross_channel_route = False
            client.can_monitor = False
            client.broadcast_scope = "channel"

            request_headers = self._request_headers(websocket)
            origin = request_headers.get("Origin") if request_headers else None
            if not self._origin_allowed(origin):
                logging.warning(f"{LogColors.FAIL}Origin rejected: {origin!r}{LogColors.ENDC}")
                await websocket.close(1008, "origin not allowed")
                return

            authenticated: asyncio.Future = asyncio.get_running_loop().create_future()
            authenticated.add_done_callback(_release_pending)

            def _admitted() -> bool:
                return (authenticated.done() and not authenticated.cancelled()
                        and authenticated.result() is True)

            # Pre-dispatch gate: until identify has succeeded, `identify` is the
            # ONLY frame type this socket can get processed (no ping, no
            # node_status, no replies). Replaces the per-handler capability
            # checks as the thing standing between an unidentified socket and
            # the mesh.
            client.authorize = lambda msg_type: msg_type == "identify" or _admitted()

            @client.on('identify')
            async def handle_identify(payload):
                # identify is a one-shot. A second identify (valid token or not)
                # would re-run name eviction and capability assignment against a
                # client that is already registered, so it is a protocol violation.
                if authenticated.done():
                    logging.warning(
                        f"{LogColors.FAIL}re-identify from '{client.name}' ({remote_ip}) — closing{LogColors.ENDC}"
                    )
                    await client.connection.close(1008, "identify already processed")
                    return

                payload = payload if isinstance(payload, dict) else {}
                requested_name = payload.get('name', client.name)
                client_token = payload.get('token', '')

                if not self._auth_handler(client_token, remote_ip):
                    logging.warning(
                        f"{LogColors.FAIL}Auth failed (bad/missing token) for {remote_ip}{LogColors.ENDC}"
                    )
                    if not authenticated.done():
                        authenticated.set_result(False)
                    # Close now rather than letting the 5 s auth timer do it.
                    await client.connection.close(1008, "unauthorized")
                    return

                # Identity grammar: a missing channel means "default" (as does
                # an empty one, matching the gateway); a missing name keeps the
                # server-assigned placeholder. Anything present must match the
                # shared regex — no empty names, no whitespace, no 900 KB names.
                requested_channel = payload.get('channel')
                if requested_channel is None or requested_channel == "":
                    requested_channel = "default"
                if not valid_ident(requested_name) or not valid_ident(requested_channel):
                    logging.warning(
                        f"{LogColors.FAIL}Invalid name/channel in identify from {remote_ip} — closing{LogColors.ENDC}"
                    )
                    if not authenticated.done():
                        authenticated.set_result(False)
                    await client.connection.close(1008, "invalid name or channel")
                    return
                requested_role = payload.get('role')
                if not isinstance(requested_role, str) or not requested_role or len(requested_role) > 64:
                    requested_role = "node"

                incumbent = self.clients_by_name.get(requested_name)
                if incumbent is not None and incumbent is not client:
                    # Last-writer-wins: a reconnecting peer reclaims its name. The
                    # old socket is usually a half-open ghost the server hasn't
                    # reaped yet; rejecting the newcomer instead would leave it
                    # silently retrying forever. Drop the incumbent from the roster
                    # now (so the newcomer can register cleanly below) and close it
                    # out of band. The identity-checked cleanup in the connection
                    # finally keeps the ghost's teardown from clobbering this entry.
                    logging.warning(
                        f"{LogColors.WARNING}Name '{requested_name}' reclaimed by {remote_ip} — "
                        f"evicting stale client {incumbent.id}{LogColors.ENDC}"
                    )
                    if self.clients.get(incumbent.id) is incumbent:
                        self.clients.pop(incumbent.id, None)
                    if self.clients_by_name.get(requested_name) is incumbent:
                        self.clients_by_name.pop(requested_name, None)
                    asyncio.create_task(incumbent.stop())

                new_id = str(uuid.uuid4())
                while new_id in self.clients:
                    new_id = str(uuid.uuid4())

                client.id = new_id
                client.freeze_id()
                client.name = requested_name
                client.channel = requested_channel
                client.role = requested_role
                client.can_broadcast = self._parse_bool(payload.get('can_broadcast'), client.role in {"dashboard", "browser", "mobile", "node"})
                client.can_route = self._parse_bool(payload.get('can_route'), client.role in {"browser", "mobile", "dashboard", "node"})
                client.can_cross_channel_route = self._parse_bool(payload.get('can_cross_channel_route'), False)
                client.can_monitor = self._parse_bool(payload.get('can_monitor'), client.role in {"dashboard", "browser"})
                requested_scope = payload.get('broadcast_scope')
                if requested_scope not in ("global", "channel"):
                    requested_scope = "global" if client.can_monitor else "channel"
                client.broadcast_scope = requested_scope

                logging.info(
                    f"{LogColors.GREEN}Identified: '{client.name}' → {client.id}{LogColors.ENDC}"
                )

                if self._on_authenticated:
                    self._on_authenticated(client, remote_ip, client_token)

                # Register BEFORE welcome and the roster push, so the joiner is in
                # the roster its peers receive and is routable from the moment it
                # learns its id. (The connection handler below re-asserts this once
                # `authenticated` resolves; both writes are identity-safe.)
                self.clients[client.id] = client
                self.clients_by_name[client.name] = client

                if not authenticated.done():
                    authenticated.set_result(True)

                await client.send("welcome", {"id": client.id, "name": client.name})
                await self._broadcast_client_list()

            # Register handlers BEFORE the listen loop starts so the first
            # request after the identify ack can't race past registration and
            # get dropped. Permission flags stay False until identify, so
            # nothing here is reachable pre-auth.
            @client.on("broadcast_request")
            async def on_broadcast(payload):
                if not client.can_broadcast:
                    return {"error": "broadcast not allowed", "status": "failed"}
                await self.broadcast("broadcast", payload, sender=client)
                return {"status": "sent"}

            @client.on("iCloud_data_Broadcast")
            async def on_iCloud_broadcast(payload):
                if not client.can_broadcast:
                    return {"error": "broadcast not allowed", "status": "failed"}
                await self.broadcast("iCloudListen", payload, sender=client)
                return {"status": "sent"}

            @client.on("place_visit_Broadcast")
            async def on_place_visit_broadcast(payload):
                if not client.can_broadcast:
                    return {"error": "broadcast not allowed", "status": "failed"}
                await self.broadcast("placeVisitListen", payload, sender=client)
                return {"status": "sent"}

            @client.on("request_prediction")
            async def on_prediction_request(payload):
                if not client.can_broadcast:
                    return {"error": "broadcast not allowed", "status": "failed"}
                await self.broadcast("request_prediction", payload, sender=client)
                return {"status": "forwarded"}

            @client.on("prediction_result")
            async def on_prediction_result(payload):
                if not client.can_broadcast:
                    return {"error": "broadcast not allowed", "status": "failed"}
                await self.broadcast("prediction_result", payload, sender=client)
                return {"status": "forwarded"}

            @client.on("service_log")
            async def on_service_log(payload):
                if not client.can_broadcast:
                    return {"error": "log broadcast not allowed", "status": "failed"}
                await self.broadcast("service_log", payload, sender=client)
                return {"status": "broadcasted"}

            @client.on("node_status")
            async def on_node_status(payload):
                if not client.can_broadcast:
                    return {"error": "broadcast not allowed", "status": "failed"}
                await self.broadcast("node_status", payload, sender=client)
                return {"status": "broadcasted"}

            outstanding_routes = 0

            @client.on('route_msg')
            async def on_route(payload):
                nonlocal outstanding_routes
                if not isinstance(payload, dict):
                    return {"error": "malformed route", "status": "failed"}
                target_id = payload.get('target_id')
                msg_type = payload.get('type')
                data = payload.get('payload')
                if not isinstance(target_id, str) or not isinstance(msg_type, str):
                    return {"error": "malformed route", "status": "failed"}

                target = self.clients.get(target_id)
                if not target:
                    return {"error": "Target not found", "status": "failed"}
                if not client.can_route:
                    return {"error": "routing not allowed", "status": "failed"}
                if not client.can_cross_channel_route and not self._same_channel(client, target):
                    return {"error": "cross-channel route denied", "status": "failed"}
                if outstanding_routes >= self.MAX_OUTSTANDING_ROUTES:
                    return {"error": "too many outstanding routes", "status": "failed"}

                outstanding_routes += 1
                try:
                    response = await target.request(msg_type, data, timeout=self.ROUTE_TIMEOUT)
                finally:
                    outstanding_routes -= 1
                if response is None:
                    # The target never answered: say so instead of leaving the
                    # requester to wait out its own timeout with no signal.
                    return {"error": "target timeout", "status": "failed"}
                return response

            @client.on("get_nodes")
            async def on_get_nodes(payload):
                client_list = [
                    self._client_summary(c)
                    for c in self.clients.values()
                    if c.id != client.id and (client.can_monitor and (client.broadcast_scope == "global" or self._same_channel(client, c)))
                ]
                return {"clients": client_list}

            @client.on('route_msg_noreply')
            async def on_noreply_route(payload):
                if not isinstance(payload, dict):
                    return {"error": "malformed route", "status": "failed"}
                target_name = payload.get('target_name')
                msg_type = payload.get('type')
                data = payload.get('payload')
                if not isinstance(target_name, str) or not isinstance(msg_type, str):
                    return {"error": "malformed route", "status": "failed"}

                target = self.clients_by_name.get(target_name)
                if target:
                    if not client.can_route:
                        return {"error": "routing not allowed", "status": "failed"}
                    if not client.can_cross_channel_route and not self._same_channel(client, target):
                        return {"error": "cross-channel route denied", "status": "failed"}
                    await target.send(msg_type, data)
                else:
                    return {"error": "Target not found", "status": "failed"}

            listen_task = asyncio.create_task(client.listen())
            # A socket that closes before identifying must not hold its slot
            # (and the handler) open for the rest of the auth window.
            listen_task.add_done_callback(
                lambda _t: authenticated.done() or authenticated.set_result(False)
            )

            try:
                is_auth = await asyncio.wait_for(authenticated, timeout=self.AUTH_TIMEOUT)
            except asyncio.TimeoutError:
                logging.warning(
                    f"{LogColors.FAIL}Auth timeout for {remote_ip} — closing silently{LogColors.ENDC}"
                )
                listen_task.cancel()
                await client.stop()
                return
            except Exception as e:
                logging.error(f"Auth error for {remote_ip}: {e}")
                listen_task.cancel()
                await client.stop()
                return

            if not is_auth:
                listen_task.cancel()
                await client.stop()
                return

            self.clients[client.id] = client
            self.clients_by_name[client.name] = client
            logging.info(
                f"{LogColors.GREEN}'{client.name}' connected. "
                f"Total clients: {len(self.clients)}{LogColors.ENDC}"
            )


            try:
                await listen_task
            finally:
                # Identity-checked pops: only release an entry that still points at
                # *this* client. An evicted incumbent must not pop the entry of the
                # peer that reclaimed its name, and no frame can have moved this
                # client's id (it is frozen at registration).
                if self.clients.get(client.id) is client:
                    self.clients.pop(client.id, None)
                if self.clients_by_name.get(client.name) is client:
                    self.clients_by_name.pop(client.name, None)
                logging.info(
                    f"{LogColors.WARNING}'{client.name}' disconnected. "
                    f"Remaining: {len(self.clients)}{LogColors.ENDC}"
                )
                await self._broadcast_client_list()
        finally:
            _release_pending()
            self._open_conns = max(0, self._open_conns - 1)

    # Per-peer send budget for fan-outs. A peer that cannot take a frame within
    # this window is stalled or gone; it is reaped so it cannot hold up the
    # roster or a channel broadcast for everyone else.
    SEND_TIMEOUT = 2.0

    async def _fan_out(self, sends, targets, what: str):
        """Send to every target concurrently; reap any that fail or stall."""
        if not sends:
            return
        results = await asyncio.gather(
            *(asyncio.wait_for(s, self.SEND_TIMEOUT) for s in sends),
            return_exceptions=True,
        )
        for peer, result in zip(targets, results):
            if isinstance(result, BaseException):
                self._reap(peer, f"{what} send failed: {type(result).__name__}")

    def _reap(self, peer: MeshSocket, reason: str):
        """Drop a dead/stalled peer from the tables now and close it out of band."""
        logging.warning(f"{LogColors.WARNING}Reaping '{peer.name}' — {reason}{LogColors.ENDC}")
        if self.clients.get(peer.id) is peer:
            self.clients.pop(peer.id, None)
        if self.clients_by_name.get(peer.name) is peer:
            self.clients_by_name.pop(peer.name, None)
        asyncio.create_task(peer.stop(reason=reason))

    async def _broadcast_client_list(self):
        sends, targets = [], []
        for client in list(self.clients.values()):
            if not getattr(client, "can_monitor", False):
                continue
            if getattr(client, "broadcast_scope", "channel") == "global":
                visible_clients = [self._client_summary(peer) for peer in self.clients.values()]
            else:
                visible_clients = [self._client_summary(peer) for peer in self.clients.values() if self._same_channel(client, peer)]
            sends.append(client.send('server_client_list', {'clients': visible_clients}))
            targets.append(client)
        await self._fan_out(sends, targets, "roster")

    async def broadcast(self, type: str, payload: dict, sender: MeshSocket | None = None):
        if not self.clients:
            return
        sends, targets = [], []
        for peer in list(self.clients.values()):
            if sender and not self._can_receive_broadcast(sender, peer):
                continue
            sends.append(peer.send(type, payload))
            targets.append(peer)
        await self._fan_out(sends, targets, "broadcast")


if __name__ == "__main__":
    server = MeshServer()
    try:
        asyncio.run(server.start())
    except KeyboardInterrupt:
        print("Server stopped.")
