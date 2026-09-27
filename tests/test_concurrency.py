"""Concurrency: two registrations on the same baseline — exactly one wins."""
from __future__ import annotations

import threading

import pytest
import uvicorn

import httpx

from app import service
from app.db import SessionLocal
from app.main import app
from app.models import Subject, Version

V1 = {
    "type": "record",
    "name": "R",
    "fields": [{"id": 1, "name": "a", "type": "int"}],
}
# two different, individually compatible evolutions of V1
EVOLUTION_A = {
    "type": "record",
    "name": "R",
    "fields": [
        {"id": 1, "name": "a", "type": "int"},
        {"id": 2, "name": "b", "type": "string", "default": ""},
    ],
}
EVOLUTION_B = {
    "type": "record",
    "name": "R",
    "fields": [{"id": 1, "name": "a", "type": "long"}],
}


@pytest.fixture()
def live_server():
    config = uvicorn.Config(app, host="127.0.0.1", port=8173, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(100):
        try:
            httpx.get("http://127.0.0.1:8173/health", timeout=0.2)
            break
        except Exception:
            import time

            time.sleep(0.05)
    yield "http://127.0.0.1:8173"
    server.should_exit = True
    thread.join(timeout=5)


@pytest.fixture(autouse=True)
def clean_db():
    yield
    db = SessionLocal()
    db.query(Version).delete()
    db.query(Subject).delete()
    db.commit()
    db.close()


def test_concurrent_same_baseline_only_one_succeeds(live_server, monkeypatch):
    base = live_server
    assert httpx.post(f"{base}/subjects", json={"name": "race"}).status_code == 201
    assert (
        httpx.post(f"{base}/subjects/race/versions", json={"content": V1}).status_code
        == 201
    )

    # Force both requests to compute the next version number before either
    # commits: the unique (subject, version) constraint then decides.
    barrier = threading.Barrier(2)
    original = service._next_version

    def synchronised_next_version(db, subject_id):
        barrier.wait(timeout=10)
        return original(db, subject_id)

    monkeypatch.setattr(service, "_next_version", synchronised_next_version)

    results: list[int] = []

    def publish(content):
        resp = httpx.post(
            f"{base}/subjects/race/versions", json={"content": content}, timeout=15
        )
        results.append(resp.status_code)

    t1 = threading.Thread(target=publish, args=(EVOLUTION_A,))
    t2 = threading.Thread(target=publish, args=(EVOLUTION_B,))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert sorted(results) == [201, 409]
    versions = httpx.get(f"{base}/subjects/race/versions").json()
    assert [v["version"] for v in versions] == [1, 2]  # no gap, no duplicate
