from __future__ import annotations

import http
import urllib.parse
from typing import Any

from .server import Server, ServerConnection, serve
from ..compat import Request, Response

try:
    from werkzeug.routing import Map, RequestRedirect
    from werkzeug.exceptions import NotFound
except ImportError:  # pragma: no cover
    Map = None
    RequestRedirect = None
    NotFound = None


class Router:
    def __init__(
        self,
        url_map: Map,
        server_name: str | None = None,
        url_scheme: str = "ws",
    ) -> None:
        self.url_map = url_map
        self.server_name = server_name
        self.url_scheme = url_scheme
        for rule in self.url_map.iter_rules():
            rule.websocket = True

    def get_server_name(self, connection: ServerConnection, request: Request) -> str:
        if self.server_name is None:
            return request.headers["Host"]
        return self.server_name

    def redirect(self, connection: ServerConnection, url: str) -> Response:
        response = connection.respond(http.HTTPStatus.FOUND, f"Found at {url}")
        response.headers["Location"] = url
        return response

    def not_found(self, connection: ServerConnection) -> Response:
        return connection.respond(http.HTTPStatus.NOT_FOUND, "Not Found")

    def route_request(self, connection: ServerConnection, request: Request) -> Response | None:
        url_map_adapter = self.url_map.bind(
            server_name=self.get_server_name(connection, request),
            url_scheme=self.url_scheme,
        )
        try:
            parsed = urllib.parse.urlparse(request.path)
            handler, kwargs = url_map_adapter.match(
                path_info=parsed.path,
                query_args=parsed.query,
            )
        except RequestRedirect as redirect:
            return self.redirect(connection, redirect.new_url)
        except NotFound:
            return self.not_found(connection)
        connection.handler = handler
        connection.handler_kwargs = kwargs
        return None

    async def handler(self, connection: ServerConnection) -> None:
        handler = connection.handler
        assert handler is not None
        await handler(connection, **connection.handler_kwargs)


def route(
    url_map: Map,
    *args: Any,
    server_name: str | None = None,
    create_router: type[Router] | None = None,
    **kwargs: Any,
) -> Any:
    if Map is None:
        raise ImportError("route() requires werkzeug")
    router_cls = create_router or Router
    router = router_cls(url_map, server_name=server_name)
    return serve(router.handler, *args, process_request=router.route_request, **kwargs)
