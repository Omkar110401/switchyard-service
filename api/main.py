from fastapi import FastAPI

app = FastAPI(title="Switchyard API")


@app.get("/health")
def health():
    return {"status": "ok"}