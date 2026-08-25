import logging

from fastapi import FastAPI

from shared.db import init_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Switchyard API")


@app.on_event("startup")
def startup_event():
    try:
        init_db()
        logger.info("Database initialized")
    except Exception as e:
        logger.error(f"DB init failed: {e}")
        raise


@app.get("/health")
def health():
    return {"status": "ok"}