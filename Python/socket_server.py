import asyncio
import websockets
from lib.socketCore import MeshSocket, LogColors
import logging
import os
from typing import Any

# Pre-auth connection ceiling for client bursts on mobile browsers.
_MAX_PENDING_PER_IP = 10
_pending_conns: dict[str, int] = {}

class MeshServer:
    def __init__(self, host="localhost", port=8765):
        self.host = host
        self.port = port
        self.clients = set() # Set of MeshSocket objects

    async def start(self):
        logging.info(f"{LogColors.HEADER}Starting Server on {self.host}:{self.port}{LogColors.ENDC}")
        async with websockets.serve(self._handle_connection, self.host, self.port, max_size=262144):
            await asyncio.Future() # Run forever

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

    def _client_summary(self, client: MeshSocket) -> dict[str, Any]:
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

    async def _handle_connection(self, websocket):
        remote_ip = self._client_ip(websocket)
        logging.info(f"{LogColors.BLUE}New connection from {remote_ip}{LogColors.ENDC}")

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

            # 1. Wrap the raw websocket in our protocol
            client = MeshSocket(connection=websocket, name=f"Client-{id(websocket)}")
            client.channel = "default"
            client.role = "unknown"
            client.can_broadcast = False
            client.can_route = False
            client.can_cross_channel_route = False
            client.can_monitor = False
            client.broadcast_scope = "channel"
        
            # 2. Authentication Flow
            authenticated = asyncio.Future()
            server_token = os.getenv('MESH_AUTH_TOKEN')
            allowed_origins = {
                origin.strip()
                for origin in os.getenv(
                    "MESH_ALLOWED_ORIGINS",
                    "http://127.0.0.1,http://localhost"
                )
                .split(",")
                if origin.strip()
            }

            request_headers = getattr(websocket, "request_headers", None)
            if request_headers is None and getattr(websocket, "request", None) is not None:
                request_headers = websocket.request.headers
            origin = request_headers.get("Origin") if request_headers else None
            if origin and not any(origin == allowed or origin.startswith(allowed) for allowed in allowed_origins):
                logging.warning(f"{LogColors.FAIL}Origin Rejected: {origin}{LogColors.ENDC}")
                await websocket.close(code=1008, reason="origin not allowed")
                return

            @client.on('identify')
            async def handle_identify(payload):
                payload = payload or {}
                client_token = payload.get('token')
                if server_token and client_token != server_token:
                    logging.warning(f"{LogColors.FAIL}Auth Failed for {client.name}{LogColors.ENDC}")
                    if not authenticated.done():
                        authenticated.set_result(False)
                    return

                client.name = payload.get('name', client.name)
                client.id = payload.get('id', client.id)
                client.channel = payload.get('channel') or "default"
                client.role = payload.get('role') or "node"
                client.can_broadcast = self._parse_bool(payload.get('can_broadcast'), client.role in {"dashboard", "browser", "mobile", "node"})
                client.can_route = self._parse_bool(payload.get('can_route'), client.role in {"browser", "mobile", "dashboard", "node"})
                client.can_cross_channel_route = self._parse_bool(payload.get('can_cross_channel_route'), False)
                client.can_monitor = self._parse_bool(payload.get('can_monitor'), client.role in {"dashboard", "browser"})
                client.broadcast_scope = payload.get('broadcast_scope') or ("global" if client.can_monitor else "channel")

                logging.info(f"{LogColors.GREEN}Client identified as: {client.name}{LogColors.ENDC}")
                if not authenticated.done():
                    authenticated.set_result(True)

                await self._broadcast_client_list()

            # We need to start processing packets to receive the 'identify' message
            listen_task = asyncio.create_task(client.listen())

            try:
                # Wait for authentication with a timeout
                is_auth = await asyncio.wait_for(authenticated, timeout=5.0)
                if not is_auth:
                    await client.stop()
                    return
            except asyncio.TimeoutError:
                logging.warning(f"{LogColors.FAIL}Auth Timeout for {client.name}{LogColors.ENDC}")
                await client.stop()
                return
            except Exception as e:
                logging.error(f"Auth error: {e}")
                await client.stop()
                return

            self.clients.add(client)
            logging.info(f"{LogColors.GREEN}New Authenticated Connection. Total Clients: {len(self.clients)}{LogColors.ENDC}")

            # 3. Register Server-Side Handlers for this specific client
            @client.on("broadcast_request")
            async def on_broadcast(payload):
                if not client.can_broadcast:
                    return {"error": "broadcast not allowed", "status": "failed"}
                logging.info(f"Broadcast Request: {payload}")
                await self.broadcast("broadcast", payload, sender=client)
                return {"status": "sent"}

            @client.on("iCloud_data_Broadcast")
            async def on_iCloud_broadcast(payload):
                if not client.can_broadcast:
                    return {"error": "broadcast not allowed", "status": "failed"}
                logging.info(f"iCloud Data: {payload}")
                await self.broadcast("iCloudListen", payload, sender=client)
                return {"status": "sent"}
        
            @client.on("request_prediction")
            async def on_prediction_request(payload):
                if not client.can_broadcast:
                    return {"error": "broadcast not allowed", "status": "failed"}
                logging.info(f"Prediction Request: {payload}")
                await self.broadcast("request_prediction", payload, sender=client)
                return {"status": "forwarded"}

            @client.on("prediction_result")
            async def on_prediction_result(payload):
                if not client.can_broadcast:
                    return {"error": "broadcast not allowed", "status": "failed"}
                logging.info(f"Prediction Result: {payload}")
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
                """
                Allows a client to send a 'UDP' message to another specific client ( by ID )
                and get the response back.
                """
                if not client.can_route:
                    return {"error": "routing not allowed", "status": "failed"}

                target_id = payload.get('target_id')
                msg_type = payload.get('type')
                data = payload.get('payload')

                target = next((c for c in self.clients if c.id == target_id), None)

                if target:
                    if not client.can_cross_channel_route and not self._same_channel(client, target):
                        return {"error": "cross-channel route denied", "status": "failed"}
                    response = await target.request(msg_type, data, timeout=5.0)
                    return response
                else:
                    return {"error": "Target not found", "status": "failed"}

            @client.on('route_msg_noreply')
            async def on_noreply_route(payload):
                """Allows a client to send a 'TCP' message to another specific client ( by name )"""
                if not client.can_route:
                    return {"error": "routing not allowed", "status": "failed"}

                target_name = payload.get('target_name')
                msg_type = payload.get('type')
                data = payload.get('payload')

                target = next((c for c in self.clients if c.name == target_name), None)

                if target:
                    if not client.can_cross_channel_route and not self._same_channel(client, target):
                        return {"error": "cross-channel route denied", "status": "failed"}
                    await target.send(msg_type, data)
                else:
                    return {"error": "Target not found", "status": "failed"}

            try:
                # Wait for the listen task to complete (when connection closes)
                await listen_task
            finally:
                # 4. Cleanup on disconnect
                if client in self.clients:
                    self.clients.remove(client)
                logging.info(f"{LogColors.WARNING}Client Disconnected. Remaining: {len(self.clients)}{LogColors.ENDC}")
        finally:
            _pending_conns[remote_ip] = max(0, _pending_conns.get(remote_ip, 1) - 1)

            
    async def _broadcast_client_list(self):
        """Sends the current list of connected clients to everyone."""
        for client in self.clients:
            if not getattr(client, "can_monitor", False):
                continue

            if getattr(client, "broadcast_scope", "channel") == "global":
                visible_clients = [self._client_summary(peer) for peer in self.clients]
            else:
                visible_clients = [
                    self._client_summary(peer)
                    for peer in self.clients
                    if self._same_channel(client, peer)
                ]

            await client.send('server_client_list', {'clients': visible_clients})

    async def broadcast(self, type: str, payload: dict, sender: MeshSocket | None = None):
        """Sends a message to all connected clients."""
        if not self.clients:
            return

        tasks = []
        for peer in self.clients:
            if sender and not self._can_receive_broadcast(sender, peer):
                continue
            tasks.append(peer.send(type, payload))
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    server = MeshServer(host='0.0.0.0')
    try:
        asyncio.run(server.start())
    except KeyboardInterrupt:
        print("Server Stopped.")
