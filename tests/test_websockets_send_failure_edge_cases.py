import asyncio
from collections.abc import AsyncIterator

import pytest

from picows import websockets
from tests.utils import WSServer


class FragmentError(Exception):
    pass


async def test_send_sync_iterable_exception_closes_connection():
    async with WSServer() as server:
        async with websockets.connect(server.url, compression=None, ping_interval=None) as ws:
            def fragments():
                yield b"first"
                raise FragmentError("boom")

            with pytest.raises(FragmentError, match="boom"):
                await ws.send(fragments())

            await asyncio.wait_for(ws.wait_closed(), 1.0)
            with pytest.raises(websockets.ConnectionClosed):
                await ws.send(b"x")


async def test_send_async_iterable_exception_closes_connection():
    async with WSServer() as server:
        async with websockets.connect(server.url, compression=None, ping_interval=None) as ws:
            async def fragments() -> AsyncIterator[bytes]:
                yield b"first"
                raise FragmentError("boom")

            with pytest.raises(FragmentError, match="boom"):
                await ws.send(fragments())

            await asyncio.wait_for(ws.wait_closed(), 1.0)
            with pytest.raises(websockets.ConnectionClosed):
                await ws.send(b"x")


async def test_send_async_iterable_cancellation_closes_connection():
    async with WSServer() as server:
        async with websockets.connect(server.url, compression=None, ping_interval=None) as ws:
            first_sent = asyncio.Event()
            unblock = asyncio.Event()

            async def fragments() -> AsyncIterator[bytes]:
                yield b"first"
                first_sent.set()
                await unblock.wait()
                yield b"second"

            send_task = asyncio.create_task(ws.send(fragments()))
            await asyncio.wait_for(first_sent.wait(), 1.0)
            send_task.cancel()

            with pytest.raises(asyncio.CancelledError):
                await send_task

            await asyncio.wait_for(ws.wait_closed(), 1.0)
            with pytest.raises(websockets.ConnectionClosed):
                await ws.send(b"x")


async def test_close_during_fragmented_send_closes_connection():
    async with WSServer() as server:
        async with websockets.connect(server.url, compression=None, ping_interval=None) as ws:
            first_sent = asyncio.Event()
            unblock = asyncio.Event()

            async def fragments() -> AsyncIterator[bytes]:
                yield b"first"
                first_sent.set()
                await unblock.wait()
                yield b"second"

            send_task = asyncio.create_task(ws.send(fragments()))
            await asyncio.wait_for(first_sent.wait(), 1.0)
            await ws.close()
            unblock.set()

            with pytest.raises(websockets.ConnectionClosed):
                await send_task
