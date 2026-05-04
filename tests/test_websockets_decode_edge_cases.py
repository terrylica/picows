import picows

from picows import websockets
from tests.utils import ServerEchoListener, WSServer


class SendTextOnConnect(ServerEchoListener):
    def __init__(self, payload: bytes):
        self._payload = payload

    def on_ws_connected(self, transport: picows.WSTransport):
        super().on_ws_connected(transport)
        transport.send(picows.WSMsgType.TEXT, self._payload)


class SendBinaryOnConnect(ServerEchoListener):
    def __init__(self, payload: bytes):
        self._payload = payload

    def on_ws_connected(self, transport: picows.WSTransport):
        super().on_ws_connected(transport)
        transport.send(picows.WSMsgType.BINARY, self._payload)


async def test_recv_decode_false_returns_bytes_for_text_messages():
    async with WSServer(lambda _: SendTextOnConnect(b"hello")) as server:
        async with websockets.connect(server.url, compression=None) as ws:
            assert await ws.recv(decode=False) == b"hello"


async def test_recv_decode_true_returns_text_for_binary_messages():
    async with WSServer(lambda _: SendBinaryOnConnect(b"hello")) as server:
        async with websockets.connect(server.url, compression=None) as ws:
            assert await ws.recv(decode=True) == "hello"


async def test_recv_streaming_decode_false_returns_bytes_for_text_messages():
    async with WSServer(lambda _: SendTextOnConnect(b"hello")) as server:
        async with websockets.connect(server.url, compression=None) as ws:
            fragments = []
            async for fragment in ws.recv_streaming(decode=False):
                fragments.append(fragment)
            assert fragments == [b"hello"]


async def test_recv_streaming_decode_true_returns_text_for_binary_messages():
    async with WSServer(lambda _: SendBinaryOnConnect(b"hello")) as server:
        async with websockets.connect(server.url, compression=None) as ws:
            fragments = []
            async for fragment in ws.recv_streaming(decode=True):
                fragments.append(fragment)
            assert fragments == ["hello"]
