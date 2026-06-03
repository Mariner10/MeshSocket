import asyncio
import pytest
import json
from unittest.mock import AsyncMock, MagicMock, patch
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
