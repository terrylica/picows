from __future__ import annotations

import asyncio
import socket
import sys
from dataclasses import dataclass
from http import HTTPStatus

import pytest
from multidict import CIMultiDict

import picows
from picows import websockets
from picows.websockets.asyncio.client import _process_proxy
from picows.websockets.asyncio.connection import (
    _PerMessageDeflate,
    _normalize_watermarks,
    _resolve_logger,
    process_exception,
)
from picows.websockets.asyncio.negotiation import configure_permessage_deflate, resolve_subprotocol
from picows.websockets.asyncio.server import _parse_basic_authorization
from tests.utils import ServerEchoListener, WSServer


@dataclass
class Close:
    code: int
    reason: str


def test_connection_closed_string_variants():
    assert str(websockets.ConnectionClosed(None, None)) == "no close frame received or sent"
    assert str(websockets.ConnectionClosed(None, Close(1000, "bye"))) == "sent 1000 (bye)"
    assert str(websockets.ConnectionClosed(Close(1001, "away"), None)) == "received 1001 (away)"
    assert str(websockets.ConnectionClosed(Close(1001, "away"), Close(1000, "bye"), True)) == (
        "received then sent close frames: received 1001 (away), sent 1000 (bye)"
    )
    assert str(websockets.ConnectionClosed(Close(1001, "away"), Close(1000, "bye"), False)) == (
        "sent then received close frames: received 1001 (away), sent 1000 (bye)"
    )


def test_exception_attributes_and_strings():
    assert str(websockets.InvalidURI("http://example.com", "wrong scheme")) == (
        "http://example.com isn't a valid WebSocket URI: wrong scheme"
    )
    assert str(websockets.InvalidProxy("ftp://proxy", "wrong scheme")) == (
        "ftp://proxy isn't a valid proxy: wrong scheme"
    )
    assert str(websockets.InvalidProxyStatus(object())) == "proxy rejected connection"
    assert str(websockets.InvalidProxyStatus(websockets.Response(502, "Bad Gateway", CIMultiDict(), b""))) == (
        "proxy rejected connection: HTTP 502"
    )

    invalid_header = websockets.InvalidHeader("X-Test", "bad")
    assert invalid_header.name == "X-Test"
    assert invalid_header.value == "bad"
    assert websockets.InvalidOrigin("https://bad.example").name == "Origin"
    assert websockets.InvalidHeaderFormat("X-Test", "bad syntax", "x:y", 1).value == "bad syntax at 1 in x:y"

    assert str(websockets.DuplicateParameter("server_max_window_bits")) == (
        "duplicate parameter: server_max_window_bits"
    )
    assert str(websockets.InvalidParameterName("x")) == "invalid parameter name: x"
    assert str(websockets.InvalidParameterValue("x", None)) == "missing value for parameter x"
    assert str(websockets.InvalidParameterValue("x", "")) == "empty value for parameter x"
    assert str(websockets.InvalidParameterValue("x", "bad")) == "invalid value for parameter x: bad"


def test_client_private_option_helpers():
    assert _process_proxy(None, False) is None
    assert _process_proxy("http://127.0.0.1:8080", False) == "http://127.0.0.1:8080"
    with pytest.raises(websockets.InvalidProxy):
        _process_proxy(123, False)  # type: ignore[arg-type]

    assert _normalize_watermarks(None) == (0, 0)
    assert _normalize_watermarks((None, 1)) == (0, 0)
    assert _normalize_watermarks((8, None)) == (8, 2)
    assert _resolve_logger("picows.test").name == "picows.test"


async def test_client_connection_starts_in_connecting_state():
    connection = websockets.ClientConnection(
        request=websockets.Request("/", CIMultiDict()),
        response=websockets.Response(101, "Switching Protocols", CIMultiDict(), b""),
        subprotocol=None,
        permessage_deflate=None,
    )

    assert connection.state is websockets.State.CONNECTING


async def test_connect_await_style_and_socket_options():
    async with WSServer() as server:
        ws = await websockets.connect(server.url, compression=None, ping_interval=None)
        try:
            await ws.send("awaited")
            assert await ws.recv() == "awaited"
        finally:
            await ws.close()

        sock = socket.create_connection((server.host, server.port))
        ws = await websockets.connect(server.url, compression=None, ping_interval=None, sock=sock)
        try:
            await ws.send(b"sock")
            assert await ws.recv() == b"sock"
        finally:
            await ws.close()

        async with websockets.connect(
            "ws://example.invalid/",
            compression=None,
            ping_interval=None,
            proxy=None,
            host=server.host,
            port=server.port,
        ) as ws:
            await ws.send("override")
            assert await ws.recv() == "override"


async def test_connect_rejects_conflicting_and_invalid_socket_options():
    async with WSServer() as server:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            with pytest.raises(TypeError, match="cannot pass both sock and socket_factory"):
                await websockets.connect(server.url, compression=None, sock=sock, socket_factory=lambda _: None)
        finally:
            sock.close()

        with pytest.raises(TypeError, match="sock must be a socket.socket instance"):
            await websockets.connect(server.url, compression=None, sock=object())

        with pytest.raises(TypeError, match="cannot pass both host/port override and socket_factory"):
            await websockets.connect(
                server.url,
                compression=None,
                host=server.host,
                socket_factory=lambda _: None,
            )


async def test_connect_rejects_invalid_ssl_options_before_network():
    with pytest.raises(NotImplementedError, match="ssl=False"):
        await websockets.connect("wss://example.com/", compression=None, ssl=False)
    with pytest.raises(TypeError, match="ssl must be"):
        await websockets.connect("wss://example.com/", compression=None, ssl=object())


def test_process_exception_retries_transient_failures():
    assert process_exception(EOFError()) is None
    assert process_exception(OSError()) is None
    assert process_exception(asyncio.TimeoutError()) is None

    response = websockets.Response(503, "Service Unavailable", CIMultiDict(), b"")
    assert process_exception(websockets.InvalidStatus(response)) is None

    error = RuntimeError("boom")
    assert process_exception(error) is error


def test_negotiation_rejects_invalid_subprotocol_and_extension_headers():
    response = websockets.Response(101, "Switching Protocols", CIMultiDict(), b"")

    response.headers["Sec-WebSocket-Protocol"] = 123  # type: ignore[assignment]
    with pytest.raises(websockets.InvalidHandshake, match="non-string subprotocol"):
        resolve_subprotocol(["chat"], response)

    response.headers["Sec-WebSocket-Protocol"] = "other"
    with pytest.raises(websockets.InvalidHandshake, match="unsupported subprotocol"):
        resolve_subprotocol(["chat"], response)

    response.headers.clear()
    response.headers["Sec-WebSocket-Extensions"] = "permessage-deflate"
    with pytest.raises(websockets.InvalidHandshake, match="unexpected websocket extensions"):
        configure_permessage_deflate(response, None)

    response.headers["Sec-WebSocket-Extensions"] = 123  # type: ignore[assignment]
    with pytest.raises(websockets.InvalidHandshake, match="invalid Sec-WebSocket-Extensions"):
        configure_permessage_deflate(response, "deflate")


def test_permessage_deflate_rejects_invalid_parameters():
    response = websockets.Response(101, "Switching Protocols", CIMultiDict(), b"")
    invalid_headers = [
        "x-webkit-deflate-frame",
        "permessage-deflate, permessage-deflate",
        "permessage-deflate; server_no_context_takeover=true",
        "permessage-deflate; client_no_context_takeover=true",
        "permessage-deflate; server_max_window_bits",
        "permessage-deflate; server_max_window_bits=7",
        "permessage-deflate; client_max_window_bits",
        "permessage-deflate; client_max_window_bits=16",
        "permessage-deflate; unknown=value",
        "permessage-deflate; server_max_window_bits=15; server_max_window_bits=15",
    ]

    for header in invalid_headers:
        response.headers["Sec-WebSocket-Extensions"] = header
        with pytest.raises(websockets.InvalidHandshake):
            configure_permessage_deflate(response, "deflate")


def test_permessage_deflate_accepts_no_context_takeover_parameters():
    permessage_deflate = _PerMessageDeflate.from_response_header(
        "permessage-deflate; server_no_context_takeover; client_no_context_takeover"
    )

    first = permessage_deflate.encode_frame(picows.WSMsgType.TEXT, "hello", True)
    second = permessage_deflate.encode_frame(picows.WSMsgType.TEXT, b"hello", True)

    assert isinstance(first, memoryview)
    assert isinstance(second, memoryview)
    assert bytes(first)
    assert bytes(second)


class Frame:
    def __init__(
        self,
        msg_type: picows.WSMsgType,
        payload: bytes,
        *,
        fin: bool = True,
        rsv1: bool = False,
    ):
        self.msg_type = msg_type
        self.payload = payload
        self.fin = fin
        self.rsv1 = rsv1

    def get_payload_as_bytes(self) -> bytes:
        return self.payload

    def get_payload_as_memoryview(self) -> memoryview:
        return memoryview(self.payload)


def test_permessage_deflate_decode_passthrough_and_protocol_error_branches():
    permessage_deflate = _PerMessageDeflate.from_response_header(
        "permessage-deflate; server_no_context_takeover; client_no_context_takeover"
    )

    assert permessage_deflate.decode_frame(
        Frame(picows.WSMsgType.TEXT, b"plain", rsv1=False),
        0,
    ) == b"plain"
    assert permessage_deflate.decode_frame(
        Frame(picows.WSMsgType.CONTINUATION, b"continuation", rsv1=False),
        0,
    ) == b"continuation"

    encoded = bytes(permessage_deflate.encode_frame(picows.WSMsgType.TEXT, b"compressed", True))
    assert permessage_deflate.decode_frame(
        Frame(picows.WSMsgType.TEXT, encoded, rsv1=True),
        100,
    ) == b"compressed"

    with pytest.raises(picows.WSProtocolError, match="unexpected rsv1"):
        permessage_deflate.decode_frame(
            Frame(picows.WSMsgType.CONTINUATION, b"bad", rsv1=True),
            0,
        )


def test_basic_auth_argument_validation_and_malformed_headers():
    with pytest.raises(ValueError, match="provide either credentials or check_credentials"):
        websockets.basic_auth()
    with pytest.raises(ValueError, match="provide either credentials or check_credentials"):
        websockets.basic_auth(credentials=("a", "b"), check_credentials=lambda _u, _p: True)
    with pytest.raises(TypeError, match="invalid credentials argument"):
        websockets.basic_auth(credentials=("a", "b", "c"))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="invalid credentials argument"):
        websockets.basic_auth(credentials=[("a", object())])  # type: ignore[list-item]

    with pytest.raises(ValueError, match="unsupported authorization scheme"):
        _parse_basic_authorization("Bearer token")
    with pytest.raises(ValueError, match="invalid basic authorization header"):
        _parse_basic_authorization("Basic !!!")
    with pytest.raises(ValueError, match="invalid basic authorization header"):
        _parse_basic_authorization("Basic bm9jb2xvbg==")


async def test_basic_auth_rejects_bad_and_async_credentials():
    async def handler(_ws: websockets.ServerConnection) -> None:
        raise AssertionError("handler must not be called")

    async def check_credentials(_username: str, _password: str) -> bool:
        return True

    async with websockets.serve(
        handler,
        "127.0.0.1",
        0,
        compression=None,
        process_request=websockets.basic_auth(check_credentials=check_credentials),
    ) as server:
        port = server.sockets[0].getsockname()[1]
        token = "Basic " + "aW52YWxpZDpjcmVkZW50aWFscw=="
        with pytest.raises(websockets.InvalidStatus):
            async with websockets.connect(
                f"ws://127.0.0.1:{port}/",
                compression=None,
                additional_headers={"Authorization": token},
            ):
                pass


async def test_connect_async_iterator_retries_then_succeeds():
    attempts = 0

    def process_exception(exc: Exception) -> Exception | None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return None
        return exc

    connector = websockets.connect(
        "ws://127.0.0.1:1/",
        compression=None,
        open_timeout=0.01,
        process_exception=process_exception,
    )
    connector._backoff = 0
    with pytest.raises(OSError):
        async for _ws in connector:
            pass
    assert attempts == 2


class ContinuationOnConnect(ServerEchoListener):
    def on_ws_connected(self, transport: picows.WSTransport):
        super().on_ws_connected(transport)
        transport.send(picows.WSMsgType.CONTINUATION, b"bad", fin=True)


class Rsv1OnConnect(ServerEchoListener):
    def on_ws_connected(self, transport: picows.WSTransport):
        super().on_ws_connected(transport)
        transport.send(picows.WSMsgType.TEXT, b"bad", fin=True, rsv1=True)


class BadContinuationSequenceOnConnect(ServerEchoListener):
    def on_ws_connected(self, transport: picows.WSTransport):
        super().on_ws_connected(transport)
        transport.send(picows.WSMsgType.TEXT, b"first", fin=False)
        transport.send(picows.WSMsgType.TEXT, b"second", fin=True)


class SendLargeTextOnConnect(ServerEchoListener):
    def on_ws_connected(self, transport: picows.WSTransport):
        super().on_ws_connected(transport)
        transport.send(picows.WSMsgType.TEXT, b"large")


class DelayedFragmentedTextOnConnect(ServerEchoListener):
    def on_ws_connected(self, transport: picows.WSTransport):
        super().on_ws_connected(transport)

        async def send_fragments():
            transport.send(picows.WSMsgType.TEXT, b"he", fin=False)
            await asyncio.sleep(0.01)
            transport.send(picows.WSMsgType.CONTINUATION, b"llo", fin=True)

        asyncio.create_task(send_fragments())


class FragmentedTextOnConnect(ServerEchoListener):
    def on_ws_connected(self, transport: picows.WSTransport):
        super().on_ws_connected(transport)
        transport.send(picows.WSMsgType.TEXT, b"he", fin=False)
        transport.send(picows.WSMsgType.CONTINUATION, b"llo", fin=True)


class IgnoreCloseListener(ServerEchoListener):
    def on_ws_frame(self, transport: picows.WSTransport, frame: picows.WSFrame):
        if frame.msg_type == picows.WSMsgType.CLOSE:
            return
        super().on_ws_frame(transport, frame)


async def test_recv_rejects_unexpected_continuation_and_rsv1():
    async with WSServer(lambda _: ContinuationOnConnect()) as server:
        async with websockets.connect(server.url, compression=None, ping_interval=None) as ws:
            with pytest.raises(websockets.ConnectionClosedError):
                await ws.recv()

    async with WSServer(lambda _: Rsv1OnConnect()) as server:
        async with websockets.connect(server.url, compression=None, ping_interval=None) as ws:
            with pytest.raises(websockets.ConnectionClosedError):
                await ws.recv()


async def test_recv_rejects_bad_continuation_and_too_large_message():
    async with WSServer(lambda _: BadContinuationSequenceOnConnect()) as server:
        async with websockets.connect(server.url, compression=None, ping_interval=None) as ws:
            with pytest.raises(websockets.ConnectionClosedError):
                await ws.recv()

    async with WSServer(lambda _: SendLargeTextOnConnect()) as server:
        async with websockets.connect(server.url, compression=None, ping_interval=None, max_size=2) as ws:
            with pytest.raises(websockets.ConnectionClosedError):
                await ws.recv()


async def test_recv_streaming_waits_for_later_fragment():
    async with WSServer(lambda _: DelayedFragmentedTextOnConnect()) as server:
        async with websockets.connect(server.url, compression=None, ping_interval=None) as ws:
            fragments = []
            async for fragment in ws.recv_streaming():
                fragments.append(fragment)
            assert fragments == ["he", "llo"]


async def test_fragmented_message_exceeding_max_size_closes_connection():
    async with WSServer(lambda _: FragmentedTextOnConnect()) as server:
        async with websockets.connect(server.url, compression=None, ping_interval=None, max_size=4) as ws:
            with pytest.raises(websockets.ConnectionClosedError):
                await ws.recv()


async def test_send_and_ping_validation_branches():
    async with WSServer() as server:
        async with websockets.connect(server.url, compression=None, ping_interval=None) as ws:
            assert ws.state is websockets.State.OPEN
            assert ws.local_address[0] == "127.0.0.1"
            assert ws.remote_address[0] == "127.0.0.1"
            assert ws.latency == 0
            assert ws.subprotocol is None
            assert ws.close_code is None
            assert ws.close_reason is None

            default_waiter = await ws.ping()
            await asyncio.wait_for(default_waiter, 1.0)

            waiter = await ws.ping("same")
            with pytest.raises(websockets.ConcurrencyError, match="same data"):
                await ws.ping(b"same")
            await asyncio.wait_for(waiter, 1.0)

            with pytest.raises(TypeError, match="ping payload"):
                await ws.ping(object())  # type: ignore[arg-type]
            with pytest.raises(TypeError, match="unsupported type"):
                await ws.send(object())  # type: ignore[arg-type]
            with pytest.raises(TypeError, match="same category"):
                await ws.send([b"bytes", "text"])


async def test_send_text_overrides_and_concurrent_send_waits_for_turn():
    async with WSServer() as server:
        async with websockets.connect(server.url, compression=None, ping_interval=None) as ws:
            await ws.send("as-binary", text=False)
            assert await ws.recv(decode=False) == b"as-binary"

            await ws.send(b"as-text", text=True)
            assert await ws.recv() == "as-text"

            first_sent = asyncio.Event()
            release = asyncio.Event()

            async def fragments():
                yield b"first"
                first_sent.set()
                await release.wait()
                yield b"second"

            first_send = asyncio.create_task(ws.send(fragments()))
            await asyncio.wait_for(first_sent.wait(), 1.0)

            second_send = asyncio.create_task(ws.send(b"after"))
            await asyncio.sleep(0)
            assert not second_send.done()

            release.set()
            await asyncio.wait_for(first_send, 1.0)
            await asyncio.wait_for(second_send, 1.0)
            assert await ws.recv() == b"firstsecond"
            assert await ws.recv() == b"after"


async def test_send_string_fragments_and_write_pause_wait_paths():
    async with WSServer() as server:
        async with websockets.connect(server.url, compression=None, ping_interval=None) as ws:
            await ws.send(["he", "llo"])
            assert await ws.recv() == "hello"

            ws.pause_writing()
            single_send = asyncio.create_task(ws.send(b"paused"))
            await asyncio.sleep(0)
            assert not single_send.done()
            ws.resume_writing()
            await asyncio.wait_for(single_send, 1.0)
            assert await ws.recv() == b"paused"

            ws.pause_writing()
            fragmented_send = asyncio.create_task(ws.send([b"frag", b"mented"]))
            await asyncio.sleep(0)
            assert not fragmented_send.done()
            ws.resume_writing()
            await asyncio.wait_for(fragmented_send, 1.0)
            assert await ws.recv() == b"fragmented"


async def test_send_rejects_invalid_first_fragment_and_closes():
    async with WSServer() as server:
        async with websockets.connect(server.url, compression=None, ping_interval=None) as ws:
            with pytest.raises(TypeError, match="message must contain"):
                await ws.send([object()])  # type: ignore[list-item]
            await asyncio.wait_for(ws.wait_closed(), 1.0)
            assert ws.state is websockets.State.CLOSED


async def test_connection_context_manager_and_close_timeout_none():
    async with WSServer() as server:
        ws = await websockets.connect(server.url, compression=None, ping_interval=None, close_timeout=None)
        async with ws:
            await ws.send("context")
            assert await ws.recv() == "context"
        assert ws.state is websockets.State.CLOSED


async def test_close_timeout_disconnects_when_peer_ignores_close():
    async with WSServer(lambda _: IgnoreCloseListener()) as server:
        ws = await websockets.connect(
            server.url,
            compression=None,
            ping_interval=None,
            close_timeout=0.01,
        )
        await ws.close()
        assert ws.state is websockets.State.CLOSED
        assert ws.close_code == 1000
        assert ws.close_reason == ""


async def test_keepalive_loop_without_ping_timeout_sends_default_pings():
    async with WSServer(enable_auto_pong=True) as server:
        async with websockets.connect(
            server.url,
            compression=None,
            ping_interval=0.01,
            ping_timeout=None,
        ) as ws:
            await asyncio.sleep(0.03)
            assert ws.latency >= 0


async def test_keepalive_loop_with_ping_timeout_observes_pong():
    async with WSServer(enable_auto_pong=True) as server:
        async with websockets.connect(
            server.url,
            compression=None,
            ping_interval=0.01,
            ping_timeout=1.0,
        ) as ws:
            await asyncio.sleep(0.03)
            assert ws.latency >= 0


async def test_disconnect_without_close_frame_sets_error_close_state():
    async with WSServer() as server:
        async with websockets.connect(server.url, compression=None, ping_interval=None) as ws:
            await ws.send("disconnect_me_without_close_frame")
            with pytest.raises(websockets.ConnectionClosedError):
                await ws.recv()
            assert ws.close_code is None
            assert ws.close_reason is None


async def test_send_ping_and_pong_after_close_raise_connection_closed():
    async with WSServer() as server:
        ws = await websockets.connect(server.url, compression=None, ping_interval=None)
        await ws.close()

        with pytest.raises(websockets.ConnectionClosedOK):
            await ws.send(b"closed")
        with pytest.raises(websockets.ConnectionClosedOK):
            await ws.ping()
        with pytest.raises(websockets.ConnectionClosedOK):
            await ws.pong()


def test_broadcast_validation_and_exception_group():
    with pytest.raises(TypeError, match="data must be str or bytes"):
        websockets.broadcast([], object())  # type: ignore[arg-type]

    if sys.version_info[:2] < (3, 11):
        with pytest.raises(ValueError, match="requires at least Python 3.11"):
            websockets.broadcast([], "hello", raise_exceptions=True)
        return

    class BrokenConnection:
        state = websockets.State.OPEN
        _send_in_progress = False

        def _encode_and_send(self, _msg_type, _message, _fin):
            raise RuntimeError("broken")

    with pytest.raises(ExceptionGroup) as exc_info:
        websockets.broadcast([BrokenConnection()], "hello", raise_exceptions=True)  # type: ignore[list-item]
    assert len(exc_info.value.exceptions) == 1


def test_response_to_picows_supports_empty_body_and_status_alias():
    response = websockets.Response(
        int(HTTPStatus.SWITCHING_PROTOCOLS),
        HTTPStatus.SWITCHING_PROTOCOLS.phrase,
        CIMultiDict({"X-Test": "yes"}),
        bytearray(b"body"),
    )

    assert response.status == 101
    picows_response = response.to_picows()
    assert picows_response.status is HTTPStatus.SWITCHING_PROTOCOLS
    assert picows_response.headers["X-Test"] == "yes"
    assert picows_response.body == b"body"
