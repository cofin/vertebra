
import pytest
from vertebra._vertebra import VertebraResponse, submit_response, init_transport

def test_submit_response_exists():
    init_transport()
    resp = VertebraResponse(status=200, body=b"hello", request_id=123)
    submit_response(resp)
