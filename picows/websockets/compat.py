from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum

import picows
from multidict import CIMultiDict

CloseCode = picows.WSCloseCode


class State(IntEnum):
    CONNECTING = 0
    OPEN = 1
    CLOSING = 2
    CLOSED = 3


@dataclass(slots=True)
class Request:
    path: str
    headers: CIMultiDict[str]

    @classmethod
    def from_picows(cls, request: picows.WSUpgradeRequest) -> Request:
        return cls(
            path=request.path.decode("ascii", "surrogateescape"),
            headers=request.headers,
        )


@dataclass(slots=True)
class Response:
    status_code: int
    reason_phrase: str
    headers: CIMultiDict[str]
    body: bytes | bytearray

    @classmethod
    def from_picows(cls, response: picows.WSUpgradeResponse) -> Response:
        return cls(
            status_code=int(response.status),
            reason_phrase=response.status.phrase,
            headers=response.headers,
            body=b"" if response.body is None else response.body,
        )

    @property
    def status(self) -> int:
        return self.status_code


__all__ = [
    "State",
    "CloseCode",
    "Request",
    "Response",
]
