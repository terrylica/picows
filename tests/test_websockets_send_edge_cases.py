import asyncio
from collections.abc import AsyncIterator

import pytest

from picows import websockets
from tests.utils import WSServer


async def test_send_empty_iterable_is_noop():
    async with WSServer() as server:
        async with websockets.connect(server.url, compression=None, ping_interval=None) as ws:
            await ws.send([])
            pong_waiter = await ws.ping(b"noop")
            await asyncio.wait_for(pong_waiter, 1.0)


async def test_send_empty_async_iterable_is_noop():
    async def fragments() -> AsyncIterator[bytes]:
        if False:
            yield b"never"

    async with WSServer() as server:
        async with websockets.connect(server.url, compression=None, ping_interval=None) as ws:
            await ws.send(fragments())
            pong_waiter = await ws.ping(b"noop")
            await asyncio.wait_for(pong_waiter, 1.0)


async def test_send_rejects_dict_like_objects():
    async with WSServer() as server:
        async with websockets.connect(server.url, compression=None) as ws:
            with pytest.raises(TypeError, match="dict-like object"):
                await ws.send({"a": 1})


async def test_ping_accepts_byteslike_payloads():
    async with WSServer() as server:
        async with websockets.connect(server.url, compression=None, ping_interval=None) as ws:
            pong_waiter = await ws.ping(bytearray(b"abcd"))
            await asyncio.wait_for(pong_waiter, 1.0)
            pong_waiter = await ws.ping(memoryview(b"efgh"))
            await asyncio.wait_for(pong_waiter, 1.0)


async def test_pong_accepts_byteslike_payloads():
    async with WSServer() as server:
        async with websockets.connect(server.url, compression=None, ping_interval=None) as ws:
            await ws.pong(bytearray(b"abcd"))
            await ws.pong(memoryview(b"efgh"))
