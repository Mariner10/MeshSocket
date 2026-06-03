import asyncio
import os
import sys
import logging
import aiohttp
from typing import Any, Dict, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(message)s')
log = logging.getLogger("cider-bridge")


class CiderClient:
    def __init__(self, host: str = None, port: int = None, api_token: str = None):
        self.host = host or os.getenv("CIDER_HOST", "localhost")
        self.port = port or int(os.getenv("CIDER_PORT", "10767"))
        self.api_token = api_token or os.getenv("CIDER_API_TOKEN", "")
        self.base_url = f"http://{self.host}:{self.port}"
        self.headers = {"apitoken": self.api_token} if self.api_token else {}
        self._session: Optional[aiohttp.ClientSession] = None

    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                base_url=self.base_url,
                headers=self.headers,
                timeout=aiohttp.ClientTimeout(total=5),
            )

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _get(self, path: str) -> Any:
        await self._ensure_session()
        async with self._session.get(path) as resp:
            if resp.status == 204:
                return None
            return await resp.json()

    async def _post(self, path: str, body: dict = None) -> Any:
        await self._ensure_session()
        async with self._session.post(path, json=body) as resp:
            if resp.status == 204:
                return None
            return await resp.json()

    async def is_active(self) -> bool:
        try:
            await self._get("/api/v1/playback/active")
            return True
        except Exception:
            return False

    async def now_playing(self) -> Optional[dict]:
        return await self._get("/api/v1/playback/now-playing")

    async def get_volume(self) -> Optional[dict]:
        return await self._get("/api/v1/playback/volume")

    async def get_shuffle(self) -> Optional[dict]:
        return await self._get("/api/v1/playback/shuffle-mode")

    async def get_repeat(self) -> Optional[dict]:
        return await self._get("/api/v1/playback/repeat-mode")

    async def get_queue(self) -> Optional[list]:
        return await self._get("/api/v1/playback/queue")

    async def play(self):
        return await self._post("/api/v1/playback/play")

    async def pause(self):
        return await self._post("/api/v1/playback/pause")

    async def playpause(self):
        return await self._post("/api/v1/playback/playpause")

    async def stop(self):
        return await self._post("/api/v1/playback/stop")

    async def next_track(self):
        return await self._post("/api/v1/playback/next")

    async def previous_track(self):
        return await self._post("/api/v1/playback/previous")

    async def seek(self, position: float):
        return await self._post("/api/v1/playback/seek", {"position": position})

    async def set_volume(self, volume: float):
        return await self._post("/api/v1/playback/volume", {"volume": volume})

    async def toggle_shuffle(self):
        return await self._post("/api/v1/playback/toggle-shuffle")

    async def toggle_repeat(self):
        return await self._post("/api/v1/playback/toggle-repeat")

    async def play_url(self, url: str):
        return await self._post("/api/v1/playback/play-url", {"url": url})

    async def play_next(self, item_type: str, item_id: str):
        return await self._post("/api/v1/playback/play-next", {"type": item_type, "id": item_id})

    async def play_later(self, item_type: str, item_id: str):
        return await self._post("/api/v1/playback/play-later", {"type": item_type, "id": item_id})

    async def clear_queue(self):
        return await self._post("/api/v1/playback/queue/clear-queue")

    async def remove_from_queue(self, index: int):
        return await self._post("/api/v1/playback/queue/remove-by-index", {"index": index})


ARTWORK_SIZE = 600


def extract_state(now_playing, volume, shuffle, repeat, queue) -> dict:
    if now_playing is None:
        return {"available": False}

    info = now_playing.get("info") or {}
    artwork_raw = (info.get("artwork") or {}).get("url", "")
    artwork_url = artwork_raw.replace("{w}", str(ARTWORK_SIZE)).replace("{h}", str(ARTWORK_SIZE))

    queue_items = []
    if queue:
        for item in queue:
            attrs = item if isinstance(item, dict) else {}
            item_info = attrs.get("attributes") or attrs
            item_artwork_raw = (item_info.get("artwork") or {}).get("url", "")
            item_artwork = item_artwork_raw.replace("{w}", str(ARTWORK_SIZE)).replace("{h}", str(ARTWORK_SIZE))
            queue_items.append({
                "title": item_info.get("name", ""),
                "artist": item_info.get("artistName", ""),
                "artwork_url": item_artwork,
            })

    return {
        "available": True,
        "playing": now_playing.get("isPlaying", False),
        "title": info.get("name", ""),
        "artist": info.get("artistName", ""),
        "album": info.get("albumName", ""),
        "artwork_url": artwork_url,
        "duration": (info.get("durationInMillis") or 0) / 1000.0,
        "elapsed": info.get("currentPlaybackTime", 0.0),
        "shuffle": (shuffle or {}).get("value", 0),
        "repeat": (repeat or {}).get("value", 0),
        "volume": (volume or {}).get("volume", 0.0),
        "queue": queue_items,
    }


def state_changed(old: dict, new: dict) -> bool:
    if old is None or new is None:
        return True
    if old.get("available") != new.get("available"):
        return True
    for key in ("title", "artist", "album", "artwork_url", "playing", "shuffle", "repeat", "duration"):
        if old.get(key) != new.get(key):
            return True
    if abs(old.get("volume", 0) - new.get("volume", 0)) > 0.01:
        return True
    if int(old.get("elapsed", 0)) != int(new.get("elapsed", 0)):
        return True
    return False


def track_changed(old: Optional[dict], new: dict) -> bool:
    if old is None:
        return True
    return old.get("title") != new.get("title") or old.get("artist") != new.get("artist")


async def handle_cider_command(cider: CiderClient, payload: dict) -> dict:
    command = (payload or {}).get("command", "")

    try:
        if command == "play":
            await cider.play()
        elif command == "pause":
            await cider.pause()
        elif command == "playpause":
            await cider.playpause()
        elif command == "stop":
            await cider.stop()
        elif command == "next":
            await cider.next_track()
        elif command == "previous":
            await cider.previous_track()
        elif command == "seek":
            await cider.seek(payload["position"])
        elif command == "volume_get":
            data = await cider.get_volume()
            return {"status": "ok", "data": data}
        elif command == "volume_set":
            await cider.set_volume(payload["volume"])
        elif command == "toggle_shuffle":
            await cider.toggle_shuffle()
        elif command == "toggle_repeat":
            await cider.toggle_repeat()
        elif command == "play_url":
            await cider.play_url(payload["url"])
        elif command == "play_next":
            await cider.play_next(payload["type"], payload["id"])
        elif command == "play_later":
            await cider.play_later(payload["type"], payload["id"])
        elif command == "queue_clear":
            await cider.clear_queue()
        elif command == "queue_remove":
            await cider.remove_from_queue(payload["index"])
        elif command == "get_queue":
            data = await cider.get_queue()
            return {"status": "ok", "data": data}
        elif command == "get_now_playing":
            data = await cider.now_playing()
            return {"status": "ok", "data": data}
        else:
            return {"status": "error", "error": "unknown_command"}

        return {"status": "ok"}

    except (aiohttp.ClientConnectionError, aiohttp.ClientError):
        return {"status": "error", "error": "cider_unreachable"}
    except KeyError as e:
        return {"status": "error", "error": f"missing_field:{e}"}


sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from socketCore import MeshSocket


class CiderBridge:
    def __init__(self):
        self.poll_interval = float(os.getenv("CIDER_POLL_INTERVAL", "1.5"))
        self.cider = CiderClient()

        mesh_url = os.getenv("MESH_SERVER_URL", "ws://localhost:8765")
        mesh_token = os.getenv("MESH_AUTH_TOKEN", "")

        self.mesh = MeshSocket(
            url=mesh_url,
            name="cider-bridge",
            auth_token=mesh_token,
            channel="music",
            role="node",
            can_broadcast=True,
            can_route=True,
            broadcast_scope="global",
        )

        self.last_state: Optional[dict] = None

    async def start(self):
        self.mesh.on("cider_command", self._on_command)
        await self.mesh.start()
        await self.mesh.wait_until_ready()
        log.info("Connected to MeshSocket")

        try:
            await self._poll_loop()
        except asyncio.CancelledError:
            pass
        finally:
            await self.cider.close()
            await self.mesh.stop()

    async def _on_command(self, payload):
        return await handle_cider_command(self.cider, payload)

    async def _poll_loop(self):
        while True:
            try:
                now_playing = await self.cider.now_playing()
                volume = await self.cider.get_volume()
                shuffle = await self.cider.get_shuffle()
                repeat = await self.cider.get_repeat()

                queue = None
                new_state_preview = extract_state(now_playing, volume, shuffle, repeat, None)
                if track_changed(self.last_state, new_state_preview):
                    try:
                        queue = await self.cider.get_queue()
                    except Exception:
                        queue = None

                new_state = extract_state(now_playing, volume, shuffle, repeat, queue)
                if queue is None and self.last_state:
                    new_state["queue"] = self.last_state.get("queue", [])

            except (aiohttp.ClientConnectionError, aiohttp.ClientError, Exception):
                new_state = {"available": False}

            if state_changed(self.last_state, new_state):
                try:
                    broadcast_payload = {"msg_type": "cider_state", **new_state}
                    await self.mesh.send("broadcast_request", broadcast_payload)
                    self.last_state = new_state
                except Exception as e:
                    log.warning(f"Failed to broadcast: {e}")

            await asyncio.sleep(self.poll_interval)


async def main():
    bridge = CiderBridge()
    await bridge.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Cider bridge stopped.")
