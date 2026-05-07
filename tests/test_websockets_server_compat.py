import asyncio
import base64
import re

import pytest
from multidict import CIMultiDict

from picows import websockets


async def test_serve_echo_roundtrip():
    async def handler(ws: websockets.ServerConnection) -> None:
        assert ws.request.path == "/"
        assert ws.response.status_code == 101
        message = await ws.recv()
        await ws.send(message)

    async with websockets.serve(handler, "127.0.0.1", 0, compression=None) as server:
        port = server.sockets[0].getsockname()[1]
        async with websockets.connect(f"ws://127.0.0.1:{port}/", compression=None) as ws:
            await ws.send("hello")
            assert await ws.recv() == "hello"


async def test_serve_process_request_can_reject_handshake():
    async def handler(ws: websockets.ServerConnection) -> None:
        raise AssertionError("handler must not be called")

    def process_request(
        ws: websockets.ServerHandshakeConnection,
        request: websockets.Request,
    ) -> websockets.Response | None:
        assert ws.request is request
        return websockets.Response(
            status_code=418,
            reason_phrase="I'm a Teapot",
            headers=CIMultiDict({"X-Test": "yes"}),
            body=b"nope",
        )

    async with websockets.serve(
        handler,
        "127.0.0.1",
        0,
        compression=None,
        process_request=process_request,
    ) as server:
        port = server.sockets[0].getsockname()[1]
        with pytest.raises(websockets.InvalidStatus) as exc_info:
            async with websockets.connect(f"ws://127.0.0.1:{port}/", compression=None):
                pass
        assert int(exc_info.value.response.status) == 418


async def test_serve_process_response_can_mutate_handshake_response():
    async def handler(ws: websockets.ServerConnection) -> None:
        await ws.wait_closed()

    def process_response(
        ws: websockets.ServerHandshakeConnection,
        request: websockets.Request,
        response: websockets.Response,
    ) -> websockets.Response:
        assert ws.request is request
        response.headers["X-Handshake"] = "yes"
        return response

    async with websockets.serve(
        handler,
        "127.0.0.1",
        0,
        compression=None,
        process_response=process_response,
    ) as server:
        port = server.sockets[0].getsockname()[1]
        async with websockets.connect(f"ws://127.0.0.1:{port}/", compression=None) as ws:
            assert ws.response.headers["X-Handshake"] == "yes"


async def test_serve_rejects_async_process_request():
    async def handler(ws: websockets.ServerConnection) -> None:
        raise AssertionError("handler must not be called")

    async def process_request(
        ws: websockets.ServerHandshakeConnection,
        request: websockets.Request,
    ) -> websockets.Response | None:
        return None

    async with websockets.serve(
        handler,
        "127.0.0.1",
        0,
        compression=None,
        process_request=process_request,
    ) as server:
        port = server.sockets[0].getsockname()[1]
        with pytest.raises(websockets.InvalidStatus):
            async with websockets.connect(f"ws://127.0.0.1:{port}/", compression=None):
                pass


async def test_serve_rejects_create_connection():
    async def handler(ws: websockets.ServerConnection) -> None:
        raise AssertionError("handler must not be called")

    with pytest.raises(NotImplementedError):
        await websockets.serve(
            handler,
            "127.0.0.1",
            0,
            compression=None,
            create_connection=websockets.ServerConnection,
        )


async def test_serve_accepts_allowed_origin():
    async def handler(ws: websockets.ServerConnection) -> None:
        await ws.send("ok")

    async with websockets.serve(
        handler,
        "127.0.0.1",
        0,
        compression=None,
        origins=["https://example.com"],
    ) as server:
        port = server.sockets[0].getsockname()[1]
        async with websockets.connect(
            f"ws://127.0.0.1:{port}/",
            compression=None,
            origin="https://example.com",
        ) as ws:
            assert await ws.recv() == "ok"


async def test_serve_rejects_disallowed_origin():
    async def handler(ws: websockets.ServerConnection) -> None:
        raise AssertionError("handler must not be called")

    async with websockets.serve(
        handler,
        "127.0.0.1",
        0,
        compression=None,
        origins=[re.compile(r"https://allowed\\.example\\.com")],
    ) as server:
        port = server.sockets[0].getsockname()[1]
        with pytest.raises(websockets.InvalidStatus):
            async with websockets.connect(
                f"ws://127.0.0.1:{port}/",
                compression=None,
                origin="https://denied.example.com",
            ):
                pass


async def test_basic_auth_rejects_missing_credentials_and_sets_username():
    async def handler(ws: websockets.ServerConnection) -> None:
        assert ws.username == "hello"
        await ws.send(ws.username)

    async with websockets.serve(
        handler,
        "127.0.0.1",
        0,
        compression=None,
        process_request=websockets.basic_auth(
            realm="test",
            credentials=("hello", "secret"),
        ),
    ) as server:
        port = server.sockets[0].getsockname()[1]

        with pytest.raises(websockets.InvalidStatus) as exc_info:
            async with websockets.connect(f"ws://127.0.0.1:{port}/", compression=None):
                pass
        assert int(exc_info.value.response.status) == 401
        assert exc_info.value.response.headers["WWW-Authenticate"] == 'Basic realm="test"'

        token = base64.b64encode(b"hello:secret").decode()
        async with websockets.connect(
            f"ws://127.0.0.1:{port}/",
            compression=None,
            additional_headers={"Authorization": f"Basic {token}"},
        ) as ws:
            assert await ws.recv() == "hello"


async def test_serve_negotiates_subprotocol():
    async def handler(ws: websockets.ServerConnection) -> None:
        await ws.wait_closed()

    async with websockets.serve(
        handler,
        "127.0.0.1",
        0,
        compression=None,
        subprotocols=["chat", "superchat"],
    ) as server:
        port = server.sockets[0].getsockname()[1]
        async with websockets.connect(
            f"ws://127.0.0.1:{port}/",
            compression=None,
            subprotocols=["superchat", "chat"],
        ) as ws:
            assert ws.subprotocol == "chat"


async def test_select_subprotocol_receives_handshake_connection():
    seen = {}

    async def handler(ws: websockets.ServerConnection) -> None:
        await ws.wait_closed()

    def select_subprotocol(
        ws: websockets.ServerHandshakeConnection,
        offered: list[str],
    ) -> str | None:
        seen["type"] = type(ws)
        seen["path"] = ws.request.path
        seen["state"] = ws.state
        seen["has_recv"] = hasattr(ws, "recv")
        if "chat" in offered:
            return "chat"
        return None

    async with websockets.serve(
        handler,
        "127.0.0.1",
        0,
        compression=None,
        select_subprotocol=select_subprotocol,
    ) as server:
        port = server.sockets[0].getsockname()[1]
        async with websockets.connect(
            f"ws://127.0.0.1:{port}/room",
            compression=None,
            subprotocols=["chat"],
        ) as ws:
            assert ws.subprotocol == "chat"

    assert seen["type"] is websockets.ServerHandshakeConnection
    assert seen["path"] == "/room"
    assert seen["state"] is websockets.State.CONNECTING
    assert seen["has_recv"] is False


async def test_broadcast_sends_to_open_connections():
    connections: list[websockets.ServerConnection] = []

    async def handler(ws: websockets.ServerConnection) -> None:
        connections.append(ws)
        await ws.wait_closed()

    async with websockets.serve(handler, "127.0.0.1", 0, compression=None) as server:
        port = server.sockets[0].getsockname()[1]
        async with websockets.connect(f"ws://127.0.0.1:{port}/", compression=None) as ws1:
            async with websockets.connect(f"ws://127.0.0.1:{port}/", compression=None) as ws2:
                while len(connections) < 2:
                    await asyncio.sleep(0)
                websockets.broadcast(connections, "hi")
                assert await ws1.recv() == "hi"
                assert await ws2.recv() == "hi"


async def test_server_connections_tracks_open_connections():
    connected = asyncio.Event()

    async def handler(ws: websockets.ServerConnection) -> None:
        connected.set()
        await ws.wait_closed()

    async with websockets.serve(handler, "127.0.0.1", 0, compression=None) as server:
        port = server.sockets[0].getsockname()[1]
        assert server.connections == set()
        async with websockets.connect(f"ws://127.0.0.1:{port}/", compression=None):
            await connected.wait()
            assert len(server.connections) == 1
        await asyncio.sleep(0)
        assert server.connections == set()


async def test_handler_exception_closes_connection_with_internal_error():
    async def handler(ws: websockets.ServerConnection) -> None:
        raise RuntimeError("boom")

    async with websockets.serve(handler, "127.0.0.1", 0, compression=None) as server:
        port = server.sockets[0].getsockname()[1]
        async with websockets.connect(f"ws://127.0.0.1:{port}/", compression=None) as ws:
            with pytest.raises(websockets.ConnectionClosedError):
                await ws.recv()
            assert ws.close_code == 1011


async def test_server_close_closes_existing_connections():
    started = asyncio.Event()

    async def handler(ws: websockets.ServerConnection) -> None:
        started.set()
        await ws.wait_closed()

    async with websockets.serve(handler, "127.0.0.1", 0, compression=None) as server:
        port = server.sockets[0].getsockname()[1]
        async with websockets.connect(f"ws://127.0.0.1:{port}/", compression=None) as ws:
            await started.wait()
            server.close(reason="bye")
            with pytest.raises(websockets.ConnectionClosedOK):
                await ws.recv()
            assert ws.close_code == 1001
            assert ws.close_reason == "bye"
        await server.wait_closed()


async def test_wait_closed_waits_for_handler_completion():
    started = asyncio.Event()
    finish = asyncio.Event()
    finished = asyncio.Event()

    async def handler(ws: websockets.ServerConnection) -> None:
        started.set()
        await finish.wait()
        finished.set()

    server = await websockets.serve(handler, "127.0.0.1", 0, compression=None)
    port = server.sockets[0].getsockname()[1]
    async with websockets.connect(f"ws://127.0.0.1:{port}/", compression=None):
        await started.wait()
        server.close(close_connections=False)
        waiter = asyncio.create_task(server.wait_closed())
        await asyncio.sleep(0)
        assert not waiter.done()
        finish.set()
        await waiter
        assert finished.is_set()


def test_route_requires_werkzeug():
    with pytest.raises((ImportError, NotImplementedError)):
        websockets.route(None)  # type: ignore[arg-type]
