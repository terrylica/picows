import pytest

import picows
from picows import websockets
from tests.utils import ServerEchoListener, WSServer


class SendBinaryOnConnect(ServerEchoListener):
    def __init__(self, payload: bytes):
        self._payload = payload

    def on_ws_connected(self, transport: picows.WSTransport):
        super().on_ws_connected(transport)
        transport.send(picows.WSMsgType.BINARY, self._payload)


async def test_recv_decode_true_invalid_utf8_closes_connection():
    async with WSServer(lambda _: SendBinaryOnConnect(b"\xff\xfe")) as server:
        async with websockets.connect(server.url, compression=None) as ws:
            with pytest.raises(websockets.ConnectionClosedError):
                await ws.recv(decode=True)
            assert ws.close_code == 1007


async def test_recv_streaming_decode_true_invalid_utf8_closes_connection():
    async with WSServer(lambda _: SendBinaryOnConnect(b"\xff\xfe")) as server:
        async with websockets.connect(server.url, compression=None) as ws:
            with pytest.raises(websockets.ConnectionClosedError):
                async for _fragment in ws.recv_streaming(decode=True):
                    pass
            assert ws.close_code == 1007
