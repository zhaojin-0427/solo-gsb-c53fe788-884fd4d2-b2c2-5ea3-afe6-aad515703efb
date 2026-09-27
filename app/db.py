"""Database engine / session management.

``DATABASE_URL`` defaults to a local PostgreSQL. SQLite is accepted as a
development fallback (used by the test-suite); PostgreSQL is what
docker-compose wires up.
"""
from __future__ import annotations

import os
import time

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session, sessionmaker

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://contracts:contracts@localhost:5432/contracts",
)

_connect_args = {}
if DATABASE_URL.startswith("sqlite"):
    _connect_args = {"check_same_thread": False, "timeout": 15}

engine = create_engine(DATABASE_URL, pool_pre_ping=True, connect_args=_connect_args)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def get_db():
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db(retries: int = 30, delay: float = 1.0) -> None:
    """Create tables, waiting for the database to come up if needed."""
    from . import models
    from .events import ensure_counter

    last_exc: Exception | None = None
    for _ in range(retries):
        try:
            models.Base.metadata.create_all(engine)
            # Seed the singleton event-counter row. Two API replicas
            # starting at once race on the primary key; the loser rolls
            # back (the winner's seed is already in place).
            db: Session = SessionLocal()
            try:
                ensure_counter(db)
                db.commit()
            except IntegrityError:
                db.rollback()
            finally:
                db.close()
            return
        except OperationalError as exc:  # database not ready yet
            last_exc = exc
            time.sleep(delay)
    raise RuntimeError("database is not reachable") from last_exc
