import asyncio
import pytest
import json
from unittest.mock import AsyncMock, MagicMock, patch
import aiohttp
from aiohttp import ClientConnectionError


class FakeResponse:
    def __init__(self, status, body=None):
        self.status = status
        self._body = body

    async def json(self):
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class TestCiderClient:
    def test_import(self):
        from cider_bridge import CiderClient
        client = CiderClient()
        assert client.base_url == "http://localhost:10767"

    def test_custom_host_port(self):
        from cider_bridge import CiderClient
        client = CiderClient(host="192.168.1.5", port=9999, api_token="mytoken")
        assert client.base_url == "http://192.168.1.5:9999"
        assert client.headers == {"apitoken": "mytoken"}

    def test_no_token_means_no_header(self):
        from cider_bridge import CiderClient
        client = CiderClient()
        assert client.headers == {}


SAMPLE_NOW_PLAYING = {
    "info": {
        "name": "Test Song",
        "artistName": "Test Artist",
        "albumName": "Test Album",
        "durationInMillis": 245000,
        "currentPlaybackTime": 32.5,
        "artwork": {
            "url": "https://is1-ssl.mzstatic.com/image/{w}x{h}.jpg",
            "width": 3000,
            "height": 3000,
        },
    },
    "isPlaying": True,
}

SAMPLE_NOW_PLAYING_DIFFERENT_TRACK = {
    "info": {
        "name": "Other Song",
        "artistName": "Other Artist",
        "albumName": "Other Album",
        "durationInMillis": 180000,
        "currentPlaybackTime": 0.0,
        "artwork": {
            "url": "https://is1-ssl.mzstatic.com/image2/{w}x{h}.jpg",
            "width": 3000,
            "height": 3000,
        },
    },
    "isPlaying": True,
}


class TestStateFunctions:
    def test_extract_state_from_cider_response(self):
        from cider_bridge import extract_state
        state = extract_state(
            now_playing=SAMPLE_NOW_PLAYING,
            volume={"volume": 0.75},
            shuffle={"value": 1},
            repeat={"value": 0},
            queue=None,
        )
        assert state["available"] is True
        assert state["title"] == "Test Song"
        assert state["artist"] == "Test Artist"
        assert state["album"] == "Test Album"
        assert state["playing"] is True
        assert state["duration"] == 245.0
        assert state["elapsed"] == 32.5
        assert state["volume"] == 0.75
        assert state["shuffle"] == 1
        assert state["repeat"] == 0
        assert "artwork_url" in state
        assert "{w}" not in state["artwork_url"]
        assert state["queue"] == []

    def test_extract_state_unavailable(self):
        from cider_bridge import extract_state
        state = extract_state(
            now_playing=None,
            volume=None,
            shuffle=None,
            repeat=None,
            queue=None,
        )
        assert state == {"available": False}

    def test_state_changed_detects_track_change(self):
        from cider_bridge import state_changed
        old = {"title": "Song A", "artist": "Art A", "elapsed": 10, "volume": 0.5, "shuffle": 0, "repeat": 0, "playing": True}
        new = {"title": "Song B", "artist": "Art B", "elapsed": 0, "volume": 0.5, "shuffle": 0, "repeat": 0, "playing": True}
        assert state_changed(old, new) is True

    def test_state_changed_ignores_subsecond_elapsed_jitter(self):
        from cider_bridge import state_changed
        old = {"title": "Song", "artist": "Art", "elapsed": 10.1, "volume": 0.5, "shuffle": 0, "repeat": 0, "playing": True, "available": True}
        new = {"title": "Song", "artist": "Art", "elapsed": 10.8, "volume": 0.5, "shuffle": 0, "repeat": 0, "playing": True, "available": True}
        assert state_changed(old, new) is False

    def test_state_changed_detects_second_boundary(self):
        from cider_bridge import state_changed
        old = {"title": "Song", "artist": "Art", "elapsed": 10.1, "volume": 0.5, "shuffle": 0, "repeat": 0, "playing": True, "available": True}
        new = {"title": "Song", "artist": "Art", "elapsed": 11.1, "volume": 0.5, "shuffle": 0, "repeat": 0, "playing": True, "available": True}
        assert state_changed(old, new) is True

    def test_state_changed_detects_volume_change(self):
        from cider_bridge import state_changed
        old = {"title": "Song", "artist": "Art", "elapsed": 10, "volume": 0.5, "shuffle": 0, "repeat": 0, "playing": True}
        new = {"title": "Song", "artist": "Art", "elapsed": 10, "volume": 0.8, "shuffle": 0, "repeat": 0, "playing": True}
        assert state_changed(old, new) is True

    def test_state_changed_detects_play_pause(self):
        from cider_bridge import state_changed
        old = {"title": "Song", "artist": "Art", "elapsed": 10, "volume": 0.5, "shuffle": 0, "repeat": 0, "playing": True}
        new = {"title": "Song", "artist": "Art", "elapsed": 10, "volume": 0.5, "shuffle": 0, "repeat": 0, "playing": False}
        assert state_changed(old, new) is True

    def test_track_changed_same_track(self):
        from cider_bridge import track_changed
        old = {"title": "Song", "artist": "Art"}
        new = {"title": "Song", "artist": "Art"}
        assert track_changed(old, new) is False

    def test_track_changed_different_track(self):
        from cider_bridge import track_changed
        old = {"title": "Song A", "artist": "Art"}
        new = {"title": "Song B", "artist": "Art"}
        assert track_changed(old, new) is True

    def test_track_changed_from_none(self):
        from cider_bridge import track_changed
        assert track_changed(None, {"title": "Song", "artist": "Art"}) is True


class TestCommandDispatch:
    @pytest.fixture
    def mock_cider(self):
        from cider_bridge import CiderClient
        client = CiderClient()
        client.play = AsyncMock(return_value=None)
        client.pause = AsyncMock(return_value=None)
        client.playpause = AsyncMock(return_value=None)
        client.stop = AsyncMock(return_value=None)
        client.next_track = AsyncMock(return_value=None)
        client.previous_track = AsyncMock(return_value=None)
        client.seek = AsyncMock(return_value=None)
        client.get_volume = AsyncMock(return_value={"volume": 0.5})
        client.set_volume = AsyncMock(return_value=None)
        client.toggle_shuffle = AsyncMock(return_value=None)
        client.toggle_repeat = AsyncMock(return_value=None)
        client.play_url = AsyncMock(return_value=None)
        client.play_next = AsyncMock(return_value=None)
        client.play_later = AsyncMock(return_value=None)
        client.clear_queue = AsyncMock(return_value=None)
        client.remove_from_queue = AsyncMock(return_value=None)
        client.get_queue = AsyncMock(return_value=[])
        client.now_playing = AsyncMock(return_value=SAMPLE_NOW_PLAYING)
        return client

    @pytest.mark.asyncio
    async def test_play_command(self, mock_cider):
        from cider_bridge import handle_cider_command
        result = await handle_cider_command(mock_cider, {"command": "play"})
        mock_cider.play.assert_called_once()
        assert result["status"] == "ok"

    @pytest.mark.asyncio
    async def test_seek_command(self, mock_cider):
        from cider_bridge import handle_cider_command
        result = await handle_cider_command(mock_cider, {"command": "seek", "position": 60.0})
        mock_cider.seek.assert_called_once_with(60.0)
        assert result["status"] == "ok"

    @pytest.mark.asyncio
    async def test_volume_set_command(self, mock_cider):
        from cider_bridge import handle_cider_command
        result = await handle_cider_command(mock_cider, {"command": "volume_set", "volume": 0.8})
        mock_cider.set_volume.assert_called_once_with(0.8)
        assert result["status"] == "ok"

    @pytest.mark.asyncio
    async def test_volume_get_command(self, mock_cider):
        from cider_bridge import handle_cider_command
        result = await handle_cider_command(mock_cider, {"command": "volume_get"})
        assert result["status"] == "ok"
        assert result["data"] == {"volume": 0.5}

    @pytest.mark.asyncio
    async def test_play_url_command(self, mock_cider):
        from cider_bridge import handle_cider_command
        result = await handle_cider_command(mock_cider, {"command": "play_url", "url": "https://music.apple.com/..."})
        mock_cider.play_url.assert_called_once_with("https://music.apple.com/...")
        assert result["status"] == "ok"

    @pytest.mark.asyncio
    async def test_play_next_command(self, mock_cider):
        from cider_bridge import handle_cider_command
        result = await handle_cider_command(mock_cider, {"command": "play_next", "type": "songs", "id": "12345"})
        mock_cider.play_next.assert_called_once_with("songs", "12345")
        assert result["status"] == "ok"

    @pytest.mark.asyncio
    async def test_unknown_command(self, mock_cider):
        from cider_bridge import handle_cider_command
        result = await handle_cider_command(mock_cider, {"command": "nonexistent"})
        assert result["status"] == "error"
        assert "unknown_command" in result["error"]

    @pytest.mark.asyncio
    async def test_cider_unreachable(self, mock_cider):
        from cider_bridge import handle_cider_command
        mock_cider.play = AsyncMock(side_effect=aiohttp.ClientConnectionError("refused"))
        result = await handle_cider_command(mock_cider, {"command": "play"})
        assert result["status"] == "error"
        assert "cider_unreachable" in result["error"]

    @pytest.mark.asyncio
    async def test_get_queue_command(self, mock_cider):
        from cider_bridge import handle_cider_command
        result = await handle_cider_command(mock_cider, {"command": "get_queue"})
        assert result["status"] == "ok"
        assert result["data"] == []

    @pytest.mark.asyncio
    async def test_queue_remove_command(self, mock_cider):
        from cider_bridge import handle_cider_command
        result = await handle_cider_command(mock_cider, {"command": "queue_remove", "index": 3})
        mock_cider.remove_from_queue.assert_called_once_with(3)
        assert result["status"] == "ok"
