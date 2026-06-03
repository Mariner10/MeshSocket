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
