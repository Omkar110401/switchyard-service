import logging
import os
from typing import Optional

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from shared.models import Base

logger = logging.getLogger(__name__)


def get_database_url() -> str:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL not set")
    return db_url


def create_database_if_not_exists(db_url: str) -> None:
    from urllib.parse import urlparse

    parsed = urlparse(db_url)
    db_name = parsed.path.lstrip("/")
    if not db_name:
        raise ValueError(f"Bad database URL: {db_url}")

    admin_url = db_url.rsplit("/", 1)[0] + "/postgres"
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")

    try:
        with admin_engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
            )
            if not result.fetchone():
                logger.info(f"Creating database {db_name}")
                conn.execute(text(f"CREATE DATABASE {db_name}"))
            else:
                logger.info(f"Database {db_name} exists")
    finally:
        admin_engine.dispose()


def init_db(db_url: Optional[str] = None) -> None:
    if db_url is None:
        db_url = get_database_url()

    host = db_url.split("@")[1].split("/")[0] if "@" in db_url else "?"
    logger.info(f"Initializing database on {host}")

    create_database_if_not_exists(db_url)
    engine = create_engine(db_url, echo=False)

    try:
        Base.metadata.create_all(engine)
        logger.info("Schema ready")
    except Exception as e:
        logger.error(f"Schema init failed: {e}")
        raise
    finally:
        engine.dispose()


def get_engine(db_url: Optional[str] = None) -> Engine:
    if db_url is None:
        db_url = get_database_url()
    return create_engine(db_url, echo=False)


def get_session_factory(db_url: Optional[str] = None) -> sessionmaker:
    engine = get_engine(db_url)
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_session(db_url: Optional[str] = None) -> Session:
    SessionLocal = get_session_factory(db_url)
    return SessionLocal()
