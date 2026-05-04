import asyncio
import base64
import hashlib
from contextlib import asynccontextmanager

import pytest
import websockets as upstream_websockets

from picows import websockets


@asynccontextmanager
async def upstream_server(handler):
    server = await upstream_websockets.serve(
        handler,
        "127.0.0.1",
        0,
        compression="deflate",
    )
    port = server.sockets[0].getsockname()[1]
    try:
        yield f"ws://127.0.0.1:{port}/"
    finally:
        server.close()
        await server.wait_closed()


@asynccontextmanager
async def malformed_compressed_server():
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        request = await reader.readuntil(b"\r\n\r\n")
        headers = request.decode("ascii").split("\r\n")
        key = None
        for header in headers:
            if header.lower().startswith("sec-websocket-key:"):
                key = header.split(":", 1)[1].strip()
                break
        assert key is not None

        accept = base64.b64encode(
            hashlib.sha1(
                (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")
            ).digest()
        ).decode("ascii")

        response = (
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Accept: {accept}\r\n"
            "Sec-WebSocket-Extensions: permessage-deflate\r\n"
            "\r\n"
        )
        writer.write(response.encode("ascii"))

        payload = b"not-a-valid-deflate-stream"
        frame = bytes([0xC1, len(payload)]) + payload
        writer.write(frame)
        await writer.drain()
        await asyncio.sleep(0.1)
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        yield f"ws://127.0.0.1:{port}/"
    finally:
        server.close()
        await server.wait_closed()


async def test_compressed_message_exceeding_max_size_closes_connection():
    async def handler(ws):
        await ws.send("a" * 10000)

    async with upstream_server(handler) as url:
        async with websockets.connect(
            url, ping_interval=None, max_size=1000
        ) as ws:
            with pytest.raises(websockets.ConnectionClosedError):
                await ws.recv()


async def test_malformed_compressed_message_closes_connection():
    async with malformed_compressed_server() as url:
        async with websockets.connect(url, ping_interval=None) as ws:
            with pytest.raises(websockets.ConnectionClosedError):
                await ws.recv()
