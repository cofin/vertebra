from vertebra import App

app = App()


@app.get("/")
def hello() -> dict[str, str]:
    return {"message": "Hello from bundled Vertebra!"}
