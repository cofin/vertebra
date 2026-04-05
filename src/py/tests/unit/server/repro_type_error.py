import asyncio
from vertebra.server._asgi._adapter import AsgiAdapter
from vertebra._vertebra import VertebraResponse, submit_response

class MockRequest:
    def __init__(self):
        self.method = "GET"
        self.path = "/"
        self.query = None
        self.body = b""
        self.request_id = 123
        self.headers = {}

async def test_asgi_list_headers():
    async def app(scope, receive, send):
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [
                [b"content-type", b"text/plain"], # LIST instead of TUPLE
            ]
        })
        await send({
            "type": "http.response.body",
            "body": b"hello",
        })

    adapter = AsgiAdapter(app)
    resp = await adapter.run_single(MockRequest())
    
    print(f"ASGI headers type: {type(resp.headers[0])}")
    
    try:
        vtb_resp = VertebraResponse(
            status=resp.status,
            body=resp.body,
            headers=resp.headers,
            request_id=resp.request_id
        )
        print("SUCCESS: VertebraResponse constructed successfully with list headers")
    except TypeError as e:
        print(f"CAUGHT EXPECTED ERROR: {e}")
        raise

if __name__ == "__main__":
    try:
        asyncio.run(test_asgi_list_headers())
    except Exception:
        import sys
        sys.exit(1)
