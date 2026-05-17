from http import HTTPStatus

from multidict import CIMultiDict

from picows import websockets


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
