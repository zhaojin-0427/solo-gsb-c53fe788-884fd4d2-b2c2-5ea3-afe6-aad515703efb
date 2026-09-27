"""PostgreSQL-only controlled concurrency regressions for migration confirm.

The races pinned here need real row-level locking (``SELECT ... FOR
UPDATE``); SQLite ignores that clause, so these tests run only against
PostgreSQL. They are skipped automatically when no server is reachable.

``TEST_DATABASE_URL`` only selects the PostgreSQL **server** (the default
is the docker-compose database); the suite never creates tables in,
truncates or drops the database that URL points at. It leases its own
scratch database ``contracts_concurrency_test`` on that server instead —
ownership is proven by a database comment, a marker table carrying the
suite id / run id / generation and a cluster advisory lock with a live
heartbeat (see ``tests/pg_scratch.py``). The lease is:

* created at session start (a same-named database is reset only when it
  is a leftover of a *previous, dead* run of this exact suite; a foreign
  database, a live run or anything whose ownership cannot be proven makes
  the session fail before any destructive statement),
* truncated before every test (ownership re-checked first),
* dropped at session end with a plain, never ``WITH (FORCE)`` DROP: this
  run's own connections are closed first, the database must still be
  owned and no external client may still be attached (a client appearing
  in the check→drop window makes PostgreSQL reject the drop instead),
  otherwise teardown fails loudly and the data is left for the next run.

So the default, or pointing the variable at the Compose business
database by mistake, is safe:

    docker compose up -d db
    TEST_DATABASE_URL=postgresql+psycopg://contracts:contracts@localhost:5432/contracts \\
        pytest tests/test_migration_concurrency_pg.py

The login needs CREATE DATABASE privileges (the Compose ``contracts``
user is a superuser).

Everything else in the suite runs on a throwaway SQLite database (see
``tests/conftest.py``) and needs no external service.
"""
from __future__ import annotations

import itertools
import os
import threading
import time

import httpx
import pytest
import uvicorn
from sqlalchemy.orm import sessionmaker

from app.db import get_db
from app.main import app
from app.routers import migrations as migrations_router

from .pg_scratch import (
    claim_scratch_database,
    probe_server,
)

# TEST_DATABASE_URL names the PostgreSQL *server* the regression runs on
# (any database the login may connect to). The suite never runs DDL or
# deletes against that database — it only creates/drops its own scratch
# database below. The default is the docker-compose server.
SERVER_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql+psycopg://contracts:contracts@localhost:5432/contracts",
)

# Dedicated scratch database owned by this suite. Every destructive
# statement issued here (DROP/CREATE DATABASE, TRUNCATE) targets this
# name and is gated by an ownership check.
TEST_DB_NAME = "contracts_concurrency_test"

SIMPLE_V1 = {
    "type": "record",
    "name": "S",
    "fields": [{"id": 1, "name": "f1", "type": "int"}],
}


SKIP_REASON = probe_server(SERVER_URL)
pytestmark = pytest.mark.skipif(SKIP_REASON is not None, reason=SKIP_REASON or "")


@pytest.fixture(scope="session")
def pg_lease():
    """Ownership-checked build/clean/destroy lifecycle for the scratch
    database. A name collision is never treated as ownership: foreign
    data, a still-running peer or an unprovable claim fail loudly BEFORE
    any DROP or TRUNCATE (ScratchDatabaseError)."""
    lease = claim_scratch_database(SERVER_URL, TEST_DB_NAME)
    summary = lease.claim()
    assert summary.action in ("created", "reclaimed_stale", "reclaimed_orphan")
    try:
        yield lease
    finally:
        # Destroys the database only if this run still provably owns it;
        # if ownership was lost it raises and leaves the data in place.
        lease.release()


@pytest.fixture(scope="session")
def pg_engine(pg_lease):
    return pg_lease.engine


@pytest.fixture()
def pg_session(pg_lease):
    """Route the app's ``get_db`` dependency at the scratch test database
    and start every test from a clean slate. The lease re-verifies
    (current database, suite marker, this run id) immediately before the
    TRUNCATE and refuses if ownership cannot be proven."""
    pg_lease.reset_app_tables()
    Session = sessionmaker(
        bind=pg_lease.engine, autoflush=False, expire_on_commit=False
    )

    def override():
        session = Session()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_db] = override
    yield Session
    app.dependency_overrides.pop(get_db, None)


@pytest.fixture()
def live_server(pg_session):
    config = uvicorn.Config(app, host="127.0.0.1", port=8177, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(100):
        try:
            httpx.get("http://127.0.0.1:8177/health", timeout=0.2)
            break
        except Exception:
            time.sleep(0.05)
    yield "http://127.0.0.1:8177"
    server.should_exit = True
    thread.join(timeout=5)


def synchronise_confirm_entries(monkeypatch):
    """Barrier the first two confirm calls so both requests read the
    candidate while it is still uncommitted, before either transaction
    takes any lock — the interleaving the regression is about."""
    barrier = threading.Barrier(2)
    calls = itertools.count()
    original = migrations_router.confirm_migration

    def synchronised(db, migration):
        if next(calls) < 2:
            barrier.wait(timeout=10)
        return original(db, migration)

    monkeypatch.setattr(migrations_router, "confirm_migration", synchronised)


def run_concurrently(*tasks):
    threads = [threading.Thread(target=t) for t in tasks]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def create_subject_with_version(base, name):
    assert httpx.post(f"{base}/subjects", json={"name": name}).status_code == 201
    assert (
        httpx.post(
            f"{base}/subjects/{name}/versions", json={"content": SIMPLE_V1}
        ).status_code
        == 201
    )


def create_candidate(base, targets):
    resp = httpx.post(f"{base}/migrations", json={"subjects": targets})
    assert resp.status_code == 201, resp.text
    return resp.json()["fingerprint"]


class TestSameCandidateRace:
    """Two confirms of the SAME candidate must both return the committed
    result — the loser's view of the winner's policy switch is not drift."""

    def test_concurrent_confirm_returns_same_committed_result(
        self, live_server, pg_session, monkeypatch
    ):
        base = live_server
        create_subject_with_version(base, "pg-a")
        create_subject_with_version(base, "pg-b")
        fp = create_candidate(
            base,
            {
                "pg-a": {"to_compatibility": "FULL"},
                "pg-b": {"to_compatibility": "FULL_TRANSITIVE"},
            },
        )

        synchronise_confirm_entries(monkeypatch)
        responses: list[httpx.Response] = []

        def confirm():
            responses.append(
                httpx.post(f"{base}/migrations/{fp}/confirm", timeout=15)
            )

        run_concurrently(confirm, confirm)

        # idempotent convergence: both callers get the one committed result
        assert sorted(r.status_code for r in responses) == [200, 200]
        bodies = [r.json() for r in responses]
        assert bodies[0] == bodies[1]
        assert bodies[0]["status"] == "committed"
        # every participating subject was switched exactly once
        assert httpx.get(f"{base}/subjects/pg-a").json()["compatibility"] == "FULL"
        assert (
            httpx.get(f"{base}/subjects/pg-b").json()["compatibility"]
            == "FULL_TRANSITIVE"
        )
        detail = httpx.get(f"{base}/migrations/{fp}").json()
        assert detail["status"] == "committed"
        assert detail["committed_at"] is not None


class TestDifferentCandidatesRace:
    """Confirms of DIFFERENT candidates are still decided by the baseline
    CAS: exactly one commits, the loser is rejected wholesale."""

    def test_same_subject_exactly_one_commits(
        self, live_server, pg_session, monkeypatch
    ):
        base = live_server
        create_subject_with_version(base, "pg-c")
        fp_full = create_candidate(base, {"pg-c": {"to_compatibility": "FULL"}})
        fp_forward = create_candidate(base, {"pg-c": {"to_compatibility": "FORWARD"}})

        synchronise_confirm_entries(monkeypatch)
        results: list[int] = []

        def confirm(fp):
            results.append(
                httpx.post(f"{base}/migrations/{fp}/confirm", timeout=15).status_code
            )

        run_concurrently(lambda: confirm(fp_full), lambda: confirm(fp_forward))

        assert sorted(results) == [200, 409]
        mode = httpx.get(f"{base}/subjects/pg-c").json()["compatibility"]
        assert mode in ("FULL", "FORWARD")
        winner = fp_full if mode == "FULL" else fp_forward
        loser = fp_forward if mode == "FULL" else fp_full
        assert httpx.get(f"{base}/migrations/{winner}").json()["status"] == "committed"
        # the loser was not committed — and never becomes re-confirmable:
        # its baseline CAS now reports the winner's mode as drift
        assert httpx.get(f"{base}/migrations/{loser}").json()["status"] == "planned"
        replay = httpx.post(f"{base}/migrations/{loser}/confirm")
        assert replay.status_code == 409
        (conflict,) = replay.json()["detail"]["conflicts"]
        assert conflict["kind"] == "compatibility_drift"
        assert conflict["expected_compatibility"] == "BACKWARD"
        assert conflict["actual_compatibility"] == mode

    def test_overlapping_multi_subject_candidates_are_atomic(
        self, live_server, pg_session, monkeypatch
    ):
        base = live_server
        create_subject_with_version(base, "pg-d")
        create_subject_with_version(base, "pg-e")
        fp_both = create_candidate(
            base,
            {
                "pg-d": {"to_compatibility": "FULL"},
                "pg-e": {"to_compatibility": "FULL"},
            },
        )
        fp_one = create_candidate(base, {"pg-e": {"to_compatibility": "FORWARD"}})

        synchronise_confirm_entries(monkeypatch)
        results: list[int] = []

        def confirm(fp):
            results.append(
                httpx.post(f"{base}/migrations/{fp}/confirm", timeout=15).status_code
            )

        run_concurrently(lambda: confirm(fp_both), lambda: confirm(fp_one))

        assert sorted(results) == [200, 409]
        d_mode = httpx.get(f"{base}/subjects/pg-d").json()["compatibility"]
        e_mode = httpx.get(f"{base}/subjects/pg-e").json()["compatibility"]
        # either the two-subject candidate committed wholesale, or the
        # single-subject one did and the other rolled back ENTIRELY —
        # a torn (FULL, FORWARD) / (BACKWARD, FULL) mix is impossible
        assert (d_mode, e_mode) in (("FULL", "FULL"), ("BACKWARD", "FORWARD"))
        loser = fp_one if d_mode == "FULL" else fp_both
        assert httpx.get(f"{base}/migrations/{loser}").json()["status"] == "planned"


class TestEventLogUnderConcurrency:
    """The event log rides the same transactions as the migration commit:
    the concurrent same-candidate confirms converge to one committed
    result and the per-subject migration events are written exactly once,
    with a gapless global sequence."""

    def test_concurrent_same_candidate_confirm_emits_events_once(
        self, live_server, pg_session, monkeypatch
    ):
        base = live_server
        create_subject_with_version(base, "pg-ev-a")
        create_subject_with_version(base, "pg-ev-b")
        fp = create_candidate(
            base,
            {
                "pg-ev-a": {"to_compatibility": "FULL"},
                "pg-ev-b": {"to_compatibility": "FULL"},
            },
        )

        synchronise_confirm_entries(monkeypatch)
        responses: list[httpx.Response] = []

        def confirm():
            responses.append(
                httpx.post(f"{base}/migrations/{fp}/confirm", timeout=15)
            )

        run_concurrently(confirm, confirm)
        assert sorted(r.status_code for r in responses) == [200, 200]

        sid = httpx.post(f"{base}/event-subscriptions", json={}).json()[
            "subscription_id"
        ]
        page = httpx.get(f"{base}/event-subscriptions/{sid}/events").json()
        # two version registrations (seqs 1-2), one migration event per
        # subject (seqs 3-4): the loser's transaction wrote nothing
        assert [e["seq"] for e in page["events"]] == [1, 2, 3, 4]
        migration_events = [
            e for e in page["events"] if e["type"] == "migration_committed"
        ]
        assert sorted(e["subject"] for e in migration_events) == [
            "pg-ev-a",
            "pg-ev-b",
        ]
        assert all(e["fingerprint"] == fp for e in migration_events)
