from __future__ import annotations

import asyncio
import base64
import binascii
import hmac
import http
import inspect
import re
import socket
import sys
from collections.abc import Awaitable, Callable, Iterable
from logging import getLogger
from typing import Any, Optional, Pattern, Sequence, cast

import picows

from .connection import (
    ServerConnection,
    _default_server_header,
    _resolve_logger,
    broadcast_message,
    stash_server_request,
    stash_server_response,
    stash_server_username,
)
from ..compat import Request, Response, State
from ..exceptions import ConcurrencyError, InvalidHandshake, InvalidOrigin
from ..typing import DataLike, HeadersLike, LoggerLike, Origin, StatusLike, Subprotocol

__all__ = [
    "ServerConnection",
    "Server",
    "serve",
    "broadcast",
    "basic_auth",
]


_PERMESSAGE_DEFLATE_REQUEST = "permessage-deflate"


def _header_items(headers: Any) -> list[tuple[str, str]]:
    return [] if headers is None else list(headers.items())


def _supports_permessage_deflate(request: Request) -> bool:
    value = request.headers.get("Sec-WebSocket-Extensions")
    return isinstance(value, str) and "permessage-deflate" in value


def _origin_allowed(
    origin: str | None,
    origins: Sequence[Origin | Pattern[str] | None] | None,
) -> bool:
    if origins is None:
        return True
    for candidate in origins:
        if candidate is None:
            if origin is None:
                return True
        elif isinstance(candidate, str):
            if origin == candidate:
                return True
        elif candidate.fullmatch(origin or "") is not None:
            return True
    return False


def _select_subprotocol(
    connection: ServerConnection,
    request: Request,
    subprotocols: Optional[Sequence[Subprotocol]],
    select_subprotocol: Optional[Callable[[ServerConnection, Sequence[Subprotocol]], Subprotocol | None]],
) -> Optional[Subprotocol]:
    header_value = request.headers.get("Sec-WebSocket-Protocol")
    if header_value is None:
        return None
    offered = [item.strip() for item in header_value.split(",") if item.strip()]
    if not offered:
        return None
    if select_subprotocol is not None:
        selected = select_subprotocol(connection, offered)
        if selected is not None and selected not in offered:
            raise InvalidHandshake(f"selected subprotocol isn't offered by client: {selected}")
        return selected
    if subprotocols is None:
        return None
    for subprotocol in subprotocols:
        if subprotocol in offered:
            return subprotocol
    return None


def _build_www_authenticate_basic(realm: str) -> str:
    realm.encode("ascii")
    return f'Basic realm="{realm}"'


def _parse_authorization_basic(header: str) -> tuple[str, str]:
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "basic" or not token:
        raise InvalidHandshake("unsupported authorization header")
    try:
        decoded = base64.b64decode(token.encode("ascii"), validate=True).decode("utf-8")
    except (UnicodeDecodeError, ValueError, binascii.Error) as exc:
        raise InvalidHandshake("invalid basic authorization header") from exc
    username, sep, password = decoded.partition(":")
    if not sep:
        raise InvalidHandshake("invalid basic authorization header")
    return username, password


def _is_credentials(value: object) -> bool:
    return (
        isinstance(value, tuple)
        and len(value) == 2
        and isinstance(value[0], str)
        and isinstance(value[1], str)
    )


def basic_auth(
    realm: str = "",
    credentials: tuple[str, str] | Iterable[tuple[str, str]] | None = None,
    check_credentials: Callable[[str, str], bool] | None = None,
) -> Callable[[ServerConnection, Request], Response | None]:
    if (credentials is None) == (check_credentials is None):
        raise ValueError("provide either credentials or check_credentials")

    if check_credentials is not None and inspect.iscoroutinefunction(check_credentials):
        raise NotImplementedError("async check_credentials isn't supported by picows core yet")

    if credentials is not None:
        if _is_credentials(credentials):
            credentials_list = [cast(tuple[str, str], credentials)]
        else:
            if not isinstance(credentials, Iterable):
                raise TypeError(f"invalid credentials argument: {credentials}")
            credentials_iterable = cast(Iterable[tuple[str, str]], credentials)
            credentials_list = list(credentials_iterable)
            if not all(_is_credentials(item) for item in credentials_list):
                raise TypeError(f"invalid credentials argument: {credentials}")

        credentials_dict = dict(credentials_list)

        def check_credentials(username: str, password: str) -> bool:
            try:
                expected_password = credentials_dict[username]
            except KeyError:
                return False
            return hmac.compare_digest(expected_password, password)

    assert check_credentials is not None

    def process_request(
        connection: ServerConnection,
        request: Request,
    ) -> Response | None:
        authorization = request.headers.get("Authorization")
        if authorization is None:
            response = connection.respond(http.HTTPStatus.UNAUTHORIZED, "Missing credentials\n")
            response.headers["WWW-Authenticate"] = _build_www_authenticate_basic(realm)
            return response

        try:
            username, password = _parse_authorization_basic(authorization)
        except InvalidHandshake:
            response = connection.respond(http.HTTPStatus.UNAUTHORIZED, "Unsupported credentials\n")
            response.headers["WWW-Authenticate"] = _build_www_authenticate_basic(realm)
            return response

        if not check_credentials(username, password):
            response = connection.respond(http.HTTPStatus.UNAUTHORIZED, "Invalid credentials\n")
            response.headers["WWW-Authenticate"] = _build_www_authenticate_basic(realm)
            return response

        stash_server_username(connection, username)
        return None

    return process_request


class Server:
    def __init__(
        self,
        handler: Callable[[ServerConnection], Awaitable[None]],
        *,
        process_request: Callable[[ServerConnection, Request], Response | None] | None = None,
        process_response: Callable[[ServerConnection, Request, Response], Response | None] | None = None,
        server_header: str | None = _default_server_header(),
        open_timeout: float | None = 10,
        logger: LoggerLike | None = None,
    ) -> None:
        self.loop = asyncio.get_running_loop()
        self.handler = handler
        self.process_request = process_request
        self.process_response = process_response
        self.server_header = server_header
        self.open_timeout = open_timeout
        self.logger = _resolve_logger(logger if logger is not None else getLogger("websockets.server"))
        self.handlers: dict[ServerConnection, asyncio.Task[None]] = {}
        self.close_task: asyncio.Task[None] | None = None
        self.closed_waiter: asyncio.Future[None] = self.loop.create_future()
        self.server: asyncio.Server

    @property
    def connections(self) -> set[ServerConnection]:
        return {connection for connection in self.handlers if connection.state is State.OPEN}

    def wrap(self, server: asyncio.Server) -> None:
        self.server = server
        for sock in server.sockets:
            if sock.family == socket.AF_INET:
                name = "%s:%d" % sock.getsockname()
            elif sock.family == socket.AF_INET6:
                name = "[%s]:%d" % sock.getsockname()[:2]
            else:
                name = str(sock.getsockname())
            self.logger.info("server listening on %s", name)

    async def conn_handler(self, connection: ServerConnection) -> None:
        try:
            try:
                await asyncio.sleep(0)
                await self.handler(connection)
            except Exception:
                self.logger.error("connection handler failed", exc_info=True)
                await connection.close(1011)
            else:
                await connection.close()
        finally:
            del self.handlers[connection]

    def start_connection_handler(self, connection: ServerConnection) -> None:
        self.handlers[connection] = self.loop.create_task(self.conn_handler(connection))

    def close(
        self,
        close_connections: bool = True,
        code: int = 1001,
        reason: str = "",
    ) -> None:
        if self.close_task is None:
            self.close_task = self.loop.create_task(self._close(close_connections, code, reason))

    async def _close(
        self,
        close_connections: bool = True,
        code: int = 1001,
        reason: str = "",
    ) -> None:
        self.logger.info("server closing")
        self.server.close()
        await asyncio.sleep(0)
        if close_connections:
            close_tasks = [
                asyncio.create_task(connection.close(code, reason))
                for connection in self.handlers
                if connection.state is not State.CONNECTING
            ]
            if close_tasks:
                await asyncio.wait(close_tasks)
        await self.server.wait_closed()
        if self.handlers:
            await asyncio.wait(self.handlers.values())
        self.closed_waiter.set_result(None)
        self.logger.info("server closed")

    async def wait_closed(self) -> None:
        await asyncio.shield(self.closed_waiter)

    def get_loop(self) -> asyncio.AbstractEventLoop:
        return self.server.get_loop()

    def is_serving(self) -> bool:
        return self.server.is_serving()

    async def start_serving(self) -> None:
        await self.server.start_serving()

    async def serve_forever(self) -> None:
        await self.server.serve_forever()

    @property
    def sockets(self) -> tuple[socket.socket, ...]:
        return self.server.sockets

    async def __aenter__(self) -> Server:
        return self

    async def __aexit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.close()
        await self.wait_closed()


class serve:
    def __init__(
        self,
        handler: Callable[[ServerConnection], Awaitable[None]],
        host: str | None = None,
        port: int | None = None,
        *,
        origins: Sequence[Origin | Pattern[str] | None] | None = None,
        extensions: Sequence[Any] | None = None,
        subprotocols: Sequence[Subprotocol] | None = None,
        select_subprotocol: Callable[[ServerConnection, Sequence[Subprotocol]], Subprotocol | None] | None = None,
        compression: str | None = "deflate",
        process_request: Callable[[ServerConnection, Request], Response | None] | None = None,
        process_response: Callable[[ServerConnection, Request, Response], Response | None] | None = None,
        server_header: str | None = _default_server_header(),
        open_timeout: float | None = 10,
        ping_interval: float | None = 20,
        ping_timeout: float | None = 20,
        close_timeout: float | None = 10,
        max_size: int | None | tuple[int | None, int | None] = 1024 * 1024,
        max_queue: int | None | tuple[int | None, int | None] = 16,
        write_limit: int | tuple[int, int | None] = 32768,
        logger: LoggerLike | None = None,
        create_connection: type[ServerConnection] | None = None,
        **kwargs: Any,
    ):
        self.handler = handler
        self.host = host
        self.port = port
        self.origins = origins
        self.extensions = extensions
        self.subprotocols = subprotocols
        self.select_subprotocol = select_subprotocol
        self.compression = compression
        self.process_request = process_request
        self.process_response = process_response
        self.server_header = server_header
        self.open_timeout = open_timeout
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.close_timeout = close_timeout
        self.max_size = max_size
        self.max_queue = max_queue
        self.write_limit = write_limit
        self.logger = logger
        self.connection_factory = create_connection or ServerConnection
        self.kwargs = kwargs
        self._server: Server | None = None

    def __await__(self) -> Any:
        return self._create().__await__()

    async def __aenter__(self) -> Server:
        self._server = await self._create()
        return self._server

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        assert self._server is not None
        self._server.close()
        await self._server.wait_closed()
        self._server = None

    async def _create(self) -> Server:
        if self.extensions is not None:
            raise NotImplementedError("custom server extensions aren't supported by picows.websockets")
        if self.compression not in (None, "deflate"):
            raise NotImplementedError("only compression=None or 'deflate' are accepted")
        if self.process_request is not None and inspect.iscoroutinefunction(self.process_request):
            raise NotImplementedError("async process_request isn't supported by picows core yet")
        if self.process_response is not None and inspect.iscoroutinefunction(self.process_response):
            raise NotImplementedError("async process_response isn't supported by picows core yet")

        server = Server(
            self.handler,
            process_request=self.process_request,
            process_response=self.process_response,
            server_header=self.server_header,
            open_timeout=self.open_timeout,
            logger=self.logger,
        )

        max_message_size = self.max_size[0] if isinstance(self.max_size, tuple) else self.max_size
        max_frame_size = 2 ** 31 - 1 if max_message_size is None else max_message_size

        def listener_factory(
            upgrade_request: picows.WSUpgradeRequest,
        ) -> picows.WSUpgradeResponseWithListener:
            request = Request.from_picows(upgrade_request)
            connection = self.connection_factory(
                server,
                ping_interval=self.ping_interval,
                ping_timeout=self.ping_timeout,
                close_timeout=self.close_timeout,
                max_queue=self.max_queue,
                write_limit=self.write_limit,
                max_message_size=max_message_size,
                logger=self.logger,
                compression=self.compression,
            )
            stash_server_request(connection, request)

            response: Response | None = None

            origin = request.headers.get("Origin")
            if origin is not None and not isinstance(origin, str):
                raise InvalidOrigin(None)
            if not _origin_allowed(origin, self.origins):
                response = connection.respond(http.HTTPStatus.FORBIDDEN, "Origin not allowed\n")

            if self.process_request is not None and response is None:
                response = self.process_request(connection, request)

            if response is None:
                if server.close_task is not None:
                    response = connection.respond(http.HTTPStatus.SERVICE_UNAVAILABLE, "Server is shutting down.\n")
                else:
                    headers = {}
                    if self.server_header is not None:
                        headers["Server"] = self.server_header
                    subprotocol = _select_subprotocol(connection, request, self.subprotocols, self.select_subprotocol)
                    if subprotocol is not None:
                        headers["Sec-WebSocket-Protocol"] = subprotocol
                    if self.compression == "deflate" and _supports_permessage_deflate(request):
                        headers["Sec-WebSocket-Extensions"] = _PERMESSAGE_DEFLATE_REQUEST
                    response = Response(
                        status_code=int(http.HTTPStatus.SWITCHING_PROTOCOLS),
                        reason_phrase=http.HTTPStatus.SWITCHING_PROTOCOLS.phrase,
                        headers=type(request.headers)(headers),
                        body=b"",
                    )

            if self.process_response is not None:
                updated = self.process_response(connection, request, response)
                if updated is not None:
                    response = updated

            assert response is not None
            stash_server_response(connection, response)
            listener = connection if response.status_code == int(http.HTTPStatus.SWITCHING_PROTOCOLS) else None
            if listener is not None:
                raw_response = picows.WSUpgradeResponse.create_101_response(response.headers)
            else:
                raw_response = response.to_picows()
            return picows.WSUpgradeResponseWithListener(raw_response, listener)

        raw_server = await picows.ws_create_server(
            listener_factory,
            self.host,
            self.port,
            websocket_handshake_timeout=self.open_timeout,
            enable_auto_ping=False,
            enable_auto_pong=True,
            max_frame_size=max_frame_size,
            logger_name=self.logger if self.logger is not None else getLogger("websockets.server"),
            **self.kwargs,
        )
        server.wrap(raw_server)
        return server


def broadcast(
    connections: Iterable[ServerConnection],
    message: DataLike,
    raise_exceptions: bool = False,
) -> None:
    if isinstance(message, str):
        msg_type = picows.WSMsgType.TEXT
    elif isinstance(message, (bytes, bytearray, memoryview)):
        msg_type = picows.WSMsgType.BINARY
    else:
        raise TypeError("data must be str or bytes")

    if raise_exceptions:
        if sys.version_info[:2] < (3, 11):
            raise ValueError("raise_exceptions requires at least Python 3.11")
        exceptions: list[Exception] = []

    for connection in connections:
        if connection.state is not State.OPEN:
            continue
        try:
            sent = broadcast_message(connection, msg_type, message)
        except Exception as write_exception:
            if raise_exceptions:
                exception = RuntimeError("failed to write message")
                exception.__cause__ = write_exception
                exceptions.append(exception)
            else:
                getLogger("websockets.server").warning(
                    "skipped broadcast: failed to write message: %s",
                    write_exception,
                )
            continue

        if not sent:
            if raise_exceptions:
                exceptions.append(ConcurrencyError("sending a fragmented message"))
            else:
                getLogger("websockets.server").warning("skipped broadcast: sending a fragmented message")

    if raise_exceptions and exceptions:
        raise ExceptionGroup("skipped broadcast", exceptions)
