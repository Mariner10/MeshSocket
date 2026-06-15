import asyncio
import certifi
import inspect
import json
import ssl
import uuid
import time
import logging
import os
import websockets
from collections import deque
from typing import Callable, Dict, Any, List, Optional

# ANSI Colors for nicer logs
class LogColors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class SocketLoggingHandler(logging.Handler):
    def __init__(self, socket: 'MeshSocket'):
        super().__init__()
        self.socket = socket
        self._is_logging = False # To prevent recursion

    def emit(self, record):
        if not self.socket.is_running or self._is_logging:
            return
        
        log_entry = self.format(record)
        # We use asyncio.create_task because emit is called from non-async contexts
        try:
            loop = asyncio.get_running_loop()
            
            async def send_log():
                if self._is_logging: return
                self._is_logging = True
                try:
                    await self.socket.send("service_log", {
                        "level": record.levelname,
                        "msg": log_entry,
                        "name": self.socket.name,
                        "timestamp": record.created
                    })
                except Exception:
                    # Silently ignore errors in the logging handler to avoid loops
                    pass
                finally:
                    self._is_logging = False

            loop.create_task(send_log())
        except RuntimeError:
            # No running event loop
            pass

class MeshSocket:
    def __init__(self,
                 url: str = None,
                 connection=None,
                 name="Node",
                 max_offline_buffer: int = 0,
                 offline_file_path: str = None,
                 on_reconnect: Callable = None,
                 on_disconnect: Callable = None,
                 auth_token: str = None,
                 rate_limit: int = 0,
                 channel: str = None,
                 role: str = None,
                 can_broadcast: bool | None = None,
                 can_route: bool | None = None,
                 can_cross_channel_route: bool | None = None,
                 can_monitor: bool | None = None,
                 broadcast_scope: str | None = None):

        self.url = url
        self.connection = connection
        self.auth_token = auth_token or os.getenv('MESH_AUTH_TOKEN')
        self.channel = channel or os.getenv("MESH_CHANNEL")
        self.role = role or os.getenv("MESH_ROLE")
        self.can_broadcast = can_broadcast
        self.can_route = can_route
        self.can_cross_channel_route = can_cross_channel_route
        self.can_monitor = can_monitor
        self.broadcast_scope = broadcast_scope or os.getenv("MESH_BROADCAST_SCOPE")

        if name == "Node":
            self.name = os.getenv('CONTAINER_NAME') or os.getenv('HOSTNAME') or "Node"
        else:
            self.name = name

        self.id = str(uuid.uuid4())

        # Callbacks
        self.on_reconnect_callback = on_reconnect
        self.on_disconnect_callback = on_disconnect

        # Buffering state
        self.max_offline_buffer = max_offline_buffer
        self.offline_file_path = offline_file_path
        self._ram_buffer: deque = deque()

        # Rate limiting — track timestamps of last `rate_limit` messages
        self.rate_limit = rate_limit
        self._msg_times: Optional[deque] = deque(maxlen=rate_limit) if rate_limit > 0 else None

        # State
        self.is_running = False

        # Lazy-initialised inside the event loop to avoid DeprecationWarning
        # in Python 3.10+ when constructing outside an async context.
        self._connected_event: Optional[asyncio.Event] = None
        self._server_mode_connected: bool = connection is not None

        # Handlers
        self.handlers: Dict[str, Callable] = {}
        self._pending_requests: Dict[str, asyncio.Future] = {}

        self.on("handshake", self._handle_handshake)
        self.on("ping", self._handle_ping)
        self.on("status_request", self._handle_status_request)
        self.on("welcome", self._handle_welcome)

    @property
    def connected_event(self) -> asyncio.Event:
        if self._connected_event is None:
            self._connected_event = asyncio.Event()
            if self._server_mode_connected:
                self._connected_event.set()
                self._server_mode_connected = False
        return self._connected_event

    def __str__(self):
        return f"{self.name}, {self.url}"

    def on(self, type: str, func: Callable = None):
        if func is None:
            def wrapper(f):
                self.handlers[type] = f
                return f
            return wrapper
        self.handlers[type] = func
        return func

    def off(self, type: str):
        self.handlers.pop(type, None)

    def setup_logging(self, level=logging.INFO):
        """Attaches a SocketLoggingHandler to the root logger."""
        handler = SocketLoggingHandler(self)
        handler.setLevel(level)
        formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logging.getLogger().addHandler(handler)
        logging.info(f"Socket logging enabled for {self.name}")

    def _build_identity_payload(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "name": self.name,
            "token": self.auth_token
        }
        if self.channel:
            payload["channel"] = self.channel
        if self.role:
            payload["role"] = self.role
        if self.can_broadcast is not None:
            payload["can_broadcast"] = self.can_broadcast
        if self.can_route is not None:
            payload["can_route"] = self.can_route
        if self.can_cross_channel_route is not None:
            payload["can_cross_channel_route"] = self.can_cross_channel_route
        if self.can_monitor is not None:
            payload["can_monitor"] = self.can_monitor
        if self.broadcast_scope:
            payload["broadcast_scope"] = self.broadcast_scope
        return payload

    async def report_status(self, metrics: Dict[str, Any] = None):
        """Broadcasts the current status/metrics of this node."""
        status_payload = {
            "name": self.name,
            "id": self.id,
            "status": "online",
            "uptime": time.time() - self._start_time if hasattr(self, '_start_time') else 0,
            "metrics": metrics or {}
        }
        await self.send("node_status", status_payload)

    # --- CLIENT MODE METHODS ---
    async def start(self):
        if not self.url:
            raise ValueError("Cannot use start() without a URL.")
        self._start_time = time.time()
        self.is_running = True
        asyncio.create_task(self._maintain_connection())

    async def wait_until_ready(self):
        await self.connected_event.wait()

    async def stop(self):
        self.is_running = False
        if self.connection:
            await self.connection.close()

    # --- SERVER MODE METHODS ---
    async def listen(self):
        if not self.connection:
            raise ValueError("Server mode requires an existing connection.")
        self._start_time = time.time()
        try:
            await self._listen_loop()
        except websockets.exceptions.ConnectionClosed:
            logging.warning(f"{self.name} connection closed normally.")
        except Exception as e:
            logging.error(f"{self.name} listen error: {e}")
            raise e

    # --- INTERNAL CORE ---
    async def _maintain_connection(self):
        retry_delay = 2
        while self.is_running:
            try:
                logging.info(f"{LogColors.BLUE}{self.name} connecting to {self.url}...{LogColors.ENDC}")
                ssl_context = ssl.create_default_context(cafile=certifi.where()) if self.url.startswith("wss://") else None
                async with websockets.connect(self.url, ssl=ssl_context) as ws:
                    self.connection = ws
                    self.connected_event.set()
                    logging.info(f"{LogColors.GREEN}{self.name} Connected!{LogColors.ENDC}")
                    retry_delay = 2

                    await self.send("identify", self._build_identity_payload())

                    await self._flush_offline_queue()

                    if self.on_reconnect_callback:
                        try:
                            if inspect.iscoroutinefunction(self.on_reconnect_callback):
                                await self.on_reconnect_callback()
                            else:
                                self.on_reconnect_callback()
                        except Exception as e:
                            logging.error(f"Error in on_reconnect callback: {e}")

                    await self._listen_loop()

            except Exception as e:
                # Catch everything (not just OSError/ConnectionClosed): a server
                # restart behind Cloudflare surfaces as InvalidStatus (HTTP 521),
                # which previously escaped and killed this task for good.
                logging.warning(f"{LogColors.WARNING}{self.name} disconnected: {e}{LogColors.ENDC}")

                if self.on_disconnect_callback:
                    try:
                        if inspect.iscoroutinefunction(self.on_disconnect_callback):
                            await self.on_disconnect_callback()
                        else:
                            self.on_disconnect_callback()
                    except Exception as err:
                        logging.error(f"Error in on_disconnect callback: {err}")

            finally:
                self.connected_event.clear()
                self.connection = None
                self._fail_all_pending_requests()

            if self.is_running:
                logging.info(f"{self.name} retrying in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30)

    # --- SEND LOGIC ---
    async def send(self, type: str, payload: Any = None, reply_to: str = None) -> Optional[str]:
        msg_id = str(uuid.uuid4())
        packet = {"id": msg_id, "type": type, "payload": payload, "reply_to": reply_to}
        packet_str = json.dumps(packet)

        # CASE A: Connected - Send immediately
        if self.connection and self.connected_event.is_set():
            try:
                await self.connection.send(packet_str)
                return msg_id
            except websockets.exceptions.ConnectionClosed:
                logging.warning(f"Send failed mid-flight, attempting to buffer '{type}'")

        # CASE B: Disconnected & Buffering Enabled
        if self.max_offline_buffer > 0:
            await self._handle_offline_buffering(packet_str)
            return msg_id # Return ID even if buffered
            
        # CASE C: Disconnected & No Buffering
        raise ConnectionError("Socket is not connected and buffering is disabled.")

    async def emit(self, type: str, payload: Any = None, reply_to: str = None) -> Optional[str]:
        return await self.send(type, payload, reply_to)

    async def _handle_offline_buffering(self, packet_str: str):
        if len(self._ram_buffer) >= self.max_offline_buffer:
            if self.offline_file_path:
                await self._dump_ram_to_disk()
                await self._append_to_disk(packet_str)
            else:
                self._ram_buffer.popleft()
                self._ram_buffer.append(packet_str)
                logging.warning("Buffer full (no file path). Dropped oldest message.")
        else:
            self._ram_buffer.append(packet_str)

    async def _dump_ram_to_disk(self):
        if not self._ram_buffer:
            return
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._write_lines_to_file, list(self._ram_buffer))
        self._ram_buffer.clear()

    async def _append_to_disk(self, packet_str: str):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._write_lines_to_file, [packet_str])

    def _write_lines_to_file(self, lines: List[str]):
        with open(self.offline_file_path, "a") as f:
            for line in lines:
                f.write(line + "\n")

    async def _flush_offline_queue(self):
        if self.offline_file_path and os.path.exists(self.offline_file_path):
            logging.info(f"{LogColors.WARNING}Flushing disk buffer...{LogColors.ENDC}")
            try:
                with open(self.offline_file_path, "r") as f:
                    for line in f:
                        if line.strip():
                            await self.connection.send(line.strip())
                os.remove(self.offline_file_path)
            except Exception as e:
                logging.error(f"Failed to flush disk buffer: {e}")

        if self._ram_buffer:
            logging.info(f"{LogColors.WARNING}Flushing RAM buffer ({len(self._ram_buffer)} items)...{LogColors.ENDC}")
            while self._ram_buffer:
                await self.connection.send(self._ram_buffer.popleft())

    # --- REQUEST / RESPONSE ---
    async def request(self, type: str, payload: Any = None, timeout: float = 5.0):
        if self.url and not self.connected_event.is_set():
            logging.warning(f"Request '{type}' waiting for connection...")
            await self.connected_event.wait()

        msg_id = await self.send(type, payload)
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._pending_requests[msg_id] = future
        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending_requests.pop(msg_id, None)
            return None
        except asyncio.CancelledError:
            return None

    def _fail_all_pending_requests(self):
        for future in self._pending_requests.values():
            if not future.done(): future.cancel()
        self._pending_requests.clear()

    async def _listen_loop(self):
        async for message in self.connection:
            if self.rate_limit > 0:
                now = time.time()
                if len(self._msg_times) == self.rate_limit and (now - self._msg_times[0]) < 1.0:
                    logging.warning(f"Rate limit exceeded for {self.name}, closing connection.")
                    await self.connection.close(1008, "Rate limit exceeded")
                    return
                self._msg_times.append(now)

            try:
                data = json.loads(message)
                asyncio.create_task(self._process_packet(data))
            except json.JSONDecodeError:
                pass

    async def _process_packet(self, packet: dict):
        msg_id = packet.get('id')
        msg_type = packet.get('type')
        payload = packet.get('payload')
        reply_to = packet.get('reply_to')
        if reply_to and reply_to in self._pending_requests:
            future = self._pending_requests.pop(reply_to)
            if not future.done(): future.set_result(payload)
            return 
        if msg_type in self.handlers:
            try:
                response = await self.handlers[msg_type](payload)
                if response is not None:
                    await self.send(type=msg_type, payload=response, reply_to=msg_id)
            except Exception as e:
                logging.error(f"Error processing {msg_type}: {e}")

    async def _handle_welcome(self, payload):
        server_id = payload.get("id")
        if server_id:
            logging.info(f"{self.name} assigned server ID: {server_id}")
            self.id = server_id

    async def _handle_handshake(self, payload):
        t_remote = float(payload.get('t'))
        return {"server_id": self.id, "l": time.time() - t_remote}

    async def _handle_ping(self, _):
        return "pong"

    async def _handle_status_request(self, _):
        return {
            "name": self.name,
            "id": self.id,
            "status": "online",
            "uptime": time.time() - self._start_time if hasattr(self, '_start_time') else 0,
            "memory_usage": "unknown"
        }
