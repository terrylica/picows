import asyncio

import picows
import pytest

from picows import websockets
from tests.utils import ServerEchoListener, WSServer


class FragmentedTextListener(ServerEchoListener):
    def __init__(self, allow_first_fragment: asyncio.Event, allow_second_fragment: asyncio.Event):
        self._allow_first_fragment = allow_first_fragment
        self._allow_second_fragment = allow_second_fragment

    def on_ws_connected(self, transport: picows.WSTransport):
        super().on_ws_connected(transport)

        async def send_fragments():
            await self._allow_first_fragment.wait()
            transport.send(picows.WSMsgType.TEXT, b"he", fin=False)
            await self._allow_second_fragment.wait()
            transport.send(picows.WSMsgType.CONTINUATION, b"llo", fin=True)

        asyncio.create_task(send_fragments())


async def test_recv_cancellation_is_safe_for_fragmented_message():
    allow_first_fragment = asyncio.Event()
    allow_second_fragment = asyncio.Event()

    async with WSServer(lambda _: FragmentedTextListener(allow_first_fragment, allow_second_fragment)) as server:
        async with websockets.connect(server.url, compression=None) as ws:
            recv_task = asyncio.create_task(ws.recv())
            allow_first_fragment.set()
            await asyncio.sleep(0)
            recv_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await recv_task

            allow_second_fragment.set()
            assert await ws.recv() == "hello"


async def test_recv_streaming_cancellation_before_first_fragment_is_safe():
    allow_first_fragment = asyncio.Event()
    allow_second_fragment = asyncio.Event()

    async with WSServer(lambda _: FragmentedTextListener(allow_first_fragment, allow_second_fragment)) as server:
        async with websockets.connect(server.url, compression=None) as ws:
            iterator = ws.recv_streaming()
            recv_task = asyncio.create_task(anext(iterator))
            recv_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await recv_task

            allow_first_fragment.set()
            allow_second_fragment.set()
            assert await ws.recv() == "hello"


async def test_recv_streaming_partial_consumption_breaks_future_receives():
    allow_first_fragment = asyncio.Event()
    allow_second_fragment = asyncio.Event()

    async with WSServer(lambda _: FragmentedTextListener(allow_first_fragment, allow_second_fragment)) as server:
        async with websockets.connect(server.url, compression=None) as ws:
            iterator = ws.recv_streaming()
            allow_first_fragment.set()
            assert await anext(iterator) == "he"

            with pytest.raises(websockets.ConcurrencyError):
                await ws.recv()

            allow_second_fragment.set()

            with pytest.raises(websockets.ConcurrencyError):
                await ws.recv()
