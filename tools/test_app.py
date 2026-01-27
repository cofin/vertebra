async def app(scope, receive, send):
    assert scope["type"] == "http"
    if scope["path"] == "/hello":
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [
                [b"content-type", b"application/json"],
            ],
        })
        await send({
            "type": "http.response.body",
            "body": b'{"hello":"world"}',
        })
    else:
        await send({
            "type": "http.response.start",
            "status": 404,
            "headers": [],
        })
        await send({
            "type": "http.response.body",
            "body": b"",
        })
