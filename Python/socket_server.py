import asyncio
import uuid
import websockets
from socketCore import MeshSocket, LogColors
import logging
import os
from typing import Any, Callable, Dict, Optional

_MAX_PENDING_PER_IP = 10
_pending_conns: Dict[str, int] = {}


class MeshServer:
    def __init__(self,
                 host: str = "0.0.0.0",
                 port: int = 8765,
                 rate_limit: int = 0,
                 max_size: int = 1_048_576,
                 auth_handler: Optional[Callable] = None,
                 on_startup: Optional[Callable] = None,
                 on_authenticated: Optional[Callable] = None):
        self.host = host
        self.port = port
        self.rate_limit = rate_limit
        self.max_size = max_size

        # auth_handler(token, remote_ip) → bool
        # Default: compare against MESH_AUTH_TOKEN env var.
        self._auth_handler = auth_handler or self._default_auth
        # on_startup() — called once before the server starts accepting connections.
        self._on_startup = on_startup
        # on_authenticated(client, remote_ip, token) — called after a client passes auth.
        self._on_authenticated = on_authenticated

        self.clients: Dict[str, MeshSocket] = {}
        self.clients_by_name: Dict[str, MeshSocket] = {}

    @staticmethod
    def _default_auth(token: str, remote_ip: str) -> bool:
        server_token = os.getenv("MESH_AUTH_TOKEN")
        if not server_token:
            return True
        return token == server_token

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

    def _client_ip(self, websocket) -> str:
        request_headers = getattr(websocket, "request_headers", None)
        if request_headers is None and getattr(websocket, "request", None) is not None:
            request_headers = websocket.request.headers

        if request_headers:
            x_real_ip = request_headers.get("X-Real-IP")
            if x_real_ip:
                return x_real_ip.strip()

            x_forwarded_for = request_headers.get("X-Forwarded-For")
            if x_forwarded_for:
                first_hop = x_forwarded_for.split(",", 1)[0].strip()
                if first_hop:
                    return first_hop

        if websocket.remote_address:
            return websocket.remote_address[0]
        return "unknown"

    async def start(self):
        if self._on_startup:
            self._on_startup()
        logging.info(f"{LogColors.HEADER}Starting Server on ws://{self.host}:{self.port}{LogColors.ENDC}")
        async with websockets.serve(
            self._handle_connection,
            self.host,
            self.port,
            max_size=self.max_size,
        ):
            await asyncio.Future()

    async def _handle_connection(self, websocket):
        remote_ip = self._client_ip(websocket)
        logging.info(f"New connection from {remote_ip}")

        pending = _pending_conns.get(remote_ip, 0)
        if pending >= _MAX_PENDING_PER_IP:
            logging.warning(
                f"{LogColors.FAIL}Rejected {remote_ip} — "
                f"{pending} pending connections already{LogColors.ENDC}"
            )
            await websocket.close(4429, "Too many pending connections")
            return

        _pending_conns[remote_ip] = pending + 1
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

            allowed_origins = {
                origin.strip()
                for origin in os.getenv(
                    "MESH_ALLOWED_ORIGINS",
                    "http://127.0.0.1,http://localhost"
                ).split(",")
                if origin.strip()
            }
            request_headers = getattr(websocket, "request_headers", None)
            if request_headers is None and getattr(websocket, "request", None) is not None:
                request_headers = websocket.request.headers
            origin = request_headers.get("Origin") if request_headers else None
            if origin and not any(origin == allowed or origin.startswith(allowed) for allowed in allowed_origins):
                logging.warning(f"{LogColors.FAIL}Origin rejected: {origin}{LogColors.ENDC}")
                await websocket.close(1008, "origin not allowed")
                return

            authenticated: asyncio.Future = asyncio.get_running_loop().create_future()

            @client.on('identify')
            async def handle_identify(payload):
                payload = payload or {}
                requested_name = payload.get('name', client.name)
                client_token = payload.get('token', '')

                if not self._auth_handler(client_token, remote_ip):
                    logging.warning(
                        f"{LogColors.FAIL}Auth failed (bad/missing token) for {remote_ip}{LogColors.ENDC}"
                    )
                    if not authenticated.done():
                        authenticated.set_result(False)
                    return

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
                    self.clients.pop(incumbent.id, None)
                    self.clients_by_name.pop(requested_name, None)
                    asyncio.create_task(incumbent.stop())

                new_id = str(uuid.uuid4())
                while new_id in self.clients:
                    new_id = str(uuid.uuid4())

                client.id = new_id
                client.name = requested_name
                client.channel = payload.get('channel') or "default"
                client.role = payload.get('role') or "node"
                client.can_broadcast = self._parse_bool(payload.get('can_broadcast'), client.role in {"dashboard", "browser", "mobile", "node"})
                client.can_route = self._parse_bool(payload.get('can_route'), client.role in {"browser", "mobile", "dashboard", "node"})
                client.can_cross_channel_route = self._parse_bool(payload.get('can_cross_channel_route'), False)
                client.can_monitor = self._parse_bool(payload.get('can_monitor'), client.role in {"dashboard", "browser"})
                client.broadcast_scope = payload.get('broadcast_scope') or ("global" if client.can_monitor else "channel")

                logging.info(
                    f"{LogColors.GREEN}Identified: '{client.name}' → {client.id}{LogColors.ENDC}"
                )

                if self._on_authenticated:
                    self._on_authenticated(client, remote_ip, client_token)

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
                await self.broadcast("node_status", payload, sender=client)
                return {"status": "broadcasted"}

            @client.on('route_msg')
            async def on_route(payload):
                target_id = payload.get('target_id')
                msg_type = payload.get('type')
                data = payload.get('payload')

                target = self.clients.get(target_id)
                if target:
                    if not client.can_route:
                        return {"error": "routing not allowed", "status": "failed"}
                    if not client.can_cross_channel_route and not self._same_channel(client, target):
                        return {"error": "cross-channel route denied", "status": "failed"}
                    response = await target.request(msg_type, data, timeout=5.0)
                    return response
                return {"error": "Target not found", "status": "failed"}

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
                target_name = payload.get('target_name')
                msg_type = payload.get('type')
                data = payload.get('payload')

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

            try:
                is_auth = await asyncio.wait_for(authenticated, timeout=5.0)
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
                self.clients.pop(client.id, None)
                # Only release the name if it still points at *this* client — an
                # evicted incumbent must not pop the entry of the peer that just
                # reclaimed its name (see the eviction path in handle_identify).
                if self.clients_by_name.get(client.name) is client:
                    self.clients_by_name.pop(client.name, None)
                logging.info(
                    f"{LogColors.WARNING}'{client.name}' disconnected. "
                    f"Remaining: {len(self.clients)}{LogColors.ENDC}"
                )
                await self._broadcast_client_list()
        finally:
            _pending_conns[remote_ip] = max(0, _pending_conns.get(remote_ip, 1) - 1)

    async def _broadcast_client_list(self):
        for client in self.clients.values():
            if not getattr(client, "can_monitor", False):
                continue
            if getattr(client, "broadcast_scope", "channel") == "global":
                visible_clients = [self._client_summary(peer) for peer in self.clients.values()]
            else:
                visible_clients = [self._client_summary(peer) for peer in self.clients.values() if self._same_channel(client, peer)]
            await client.send('server_client_list', {'clients': visible_clients})

    async def broadcast(self, type: str, payload: dict, sender: MeshSocket | None = None):
        if not self.clients:
            return
        tasks = []
        for peer in self.clients.values():
            if sender and not self._can_receive_broadcast(sender, peer):
                continue
            tasks.append(peer.send(type, payload))
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    server = MeshServer()
    try:
        asyncio.run(server.start())
    except KeyboardInterrupt:
        print("Server stopped.")
