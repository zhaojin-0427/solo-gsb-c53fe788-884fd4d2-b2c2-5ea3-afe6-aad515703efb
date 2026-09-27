"""Real-PostgreSQL boundary tests for scratch-database ownership.

These tests pin the safety properties of the lease protocol in
``tests/pg_scratch.py`` against a real PostgreSQL server:

* a same-named **foreign** database — even one whose tables are exactly
  the suite's model tables (``subjects`` ...) filled with foreign rows —
  is never DROPped or TRUNCATEd; the lease fails and the data survives;
* a crash between CREATE DATABASE and the marker write leaves an
  identifiable orphan that a later run can safely recycle;
* a leftover of a previous, *dead* run of the same suite is reclaimed
  exactly once per generation;
* a **live** run (advisory lock held, heartbeat fresh, owner backend
  connected) blocks a parallel startup within a bounded wait — both the
  lock and its data survive;
* a legitimate lease supports repeated table resets (the marker is never
  truncated) and destroys its database on release;
* at exit, an **external client still connected** to the scratch
  database (comment and marker both valid for this run) is never
  force-disconnected: ``lease.release()`` closes this run's own
  connections first, reports the occupants loudly, keeps the database and
  data and releases only the advisory lock — likewise a client appearing
  in the check→drop race window blocks the plain (never ``WITH (FORCE)``)
  drop; a refused teardown leaves a state the next run judges safely,
  while an idle owned database is dropped normally;
* pointing TEST_DATABASE_URL at the scratch name itself is rejected.

Skipped automatically when no PostgreSQL server is reachable.
"""
from __future__ import annotations

import os
import threading
import time
import uuid

import psycopg
import pytest
from psycopg import sql
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from app.models import Base

from .pg_scratch import (
    MARKER_TABLE,
    SUITE_NAME,
    ScratchDatabaseError,
    _hash64_key,
    _psycopg_dsn,
    claim_scratch_database,
    owner_comment,
    probe_server,
)

# Tight staleness window for this module: a killed lease whose heartbeat
# stopped ~a second ago is an unambiguous leftover here.
STALE = 1.0

SERVER_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql+psycopg://contracts:contracts@localhost:5432/contracts",
)

SKIP_REASON = probe_server(SERVER_URL)
pytestmark = pytest.mark.skipif(SKIP_REASON is not None, reason=SKIP_REASON or "")

_SERVER_DB = make_url(SERVER_URL).database


def _dsn(dbname: str) -> str:
    return _psycopg_dsn(make_url(SERVER_URL).set(database=dbname))


def _server_conn():
    return psycopg.connect(_dsn(_SERVER_DB), autocommit=True)


def _scratch_conn(name):
    return psycopg.connect(_dsn(name), autocommit=True)


def _force_drop(name: str) -> None:
    with _server_conn() as c:
        c.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                sql.Identifier(name)
            )
        )


def _database_exists(name) -> bool:
    with _server_conn() as c:
        return (
            c.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s", (name,)
            ).fetchone()
            is not None
        )


def _database_comment(name):
    with _server_conn() as c:
        return c.execute(
            "SELECT shobj_description(oid, 'pg_database') "
            "FROM pg_database WHERE datname = %s",
            (name,),
        ).fetchone()[0]


def _advisory_lock_is_free(dbname: str) -> bool:
    """Whether the suite's cluster advisory lock for ``dbname`` is
    currently held by anyone. pg_try_advisory_lock takes it only when free;
    unlock immediately so the probe itself is non-owning."""
    key_hi, key_lo = _hash64_key(f"{SUITE_NAME}:{dbname}")
    with _server_conn() as c:
        acquired = c.execute(
            "SELECT pg_try_advisory_lock(%s, %s)", (key_hi, key_lo)
        ).fetchone()[0]
        if acquired:
            c.execute(
                "SELECT pg_advisory_unlock(%s, %s)", (key_hi, key_lo)
            )
        return acquired


def _wait_until_no_foreign_sessions(dbname: str, timeout: float = 5.0) -> None:
    """Wait until no suite-labelled backend of ours is still visible in
    pg_stat_activity (a just-closed connection can lag there briefly)."""
    deadline = time.monotonic() + timeout
    with _server_conn() as c:
        while time.monotonic() < deadline:
            own = c.execute(
                """
                SELECT 1 FROM pg_stat_activity
                WHERE datname = %s AND backend_type = 'client backend'
                  AND coalesce(application_name, '') LIKE %s
                """,
                (dbname, f"{SUITE_NAME}:%"),
            ).fetchall()
            if not own:
                return
            time.sleep(0.05)
    raise AssertionError(f"lease backends for {dbname!r} lingered")


def _marker(name) -> dict:
    with _scratch_conn(name) as c:
        return c.execute(
            f"SELECT payload FROM {MARKER_TABLE} WHERE id = 1"
        ).fetchone()[0]


@pytest.fixture()
def dbname():
    """Unique scratch name per test: ownership scenarios share the server
    without ever sharing a database (also makes the module safe to run in
    parallel with the concurrency regression)."""
    name = f"contracts_lease_test_{uuid.uuid4().hex[:16]}"
    yield name
    _force_drop(name)


# ---------------------------------------------------------------------------
# 1. Foreign database, even with identical table names, must be preserved.
# ---------------------------------------------------------------------------


class TestForeignDatabasePreserved:
    def test_foreign_rows_in_coincidentally_named_database_survive(self, dbname):
        # An unrelated project happens to own a database with the scratch
        # name and a `subjects` table full of its own data.
        precious = "PRECIOUS FOREIGN RECORD - MUST SURVIVE"
        with _server_conn() as c:
            c.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
        with _scratch_conn(dbname) as c:
            c.execute(
                "CREATE TABLE subjects (id integer PRIMARY KEY, note text)"
            )
            c.execute("INSERT INTO subjects VALUES (1, %s)", (precious,))

        lease = claim_scratch_database(SERVER_URL, dbname)
        with pytest.raises(ScratchDatabaseError, match="no ownership marker"):
            lease.claim()

        # database, rows, and schema all survive untouched
        assert _database_exists(dbname)
        with _scratch_conn(dbname) as c:
            assert c.execute("SELECT note FROM subjects").fetchall() == [
                (precious,)
            ]
            tables = [
                r[0]
                for r in c.execute(
                    """
                    SELECT c.relname FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = 'public' AND c.relkind = 'r'
                    """
                )
            ]
        assert tables == ["subjects"]
        assert MARKER_TABLE not in tables

    def test_foreign_database_with_full_app_schema_survives(self, dbname):
        # Full app schema, populated, but no ownership claim: still foreign.
        with _server_conn() as c:
            c.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
        engine = create_engine(_dsn(dbname))
        Base.metadata.create_all(engine)
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO subjects (name, compatibility) "
                    "VALUES ('foreign-subject', 'FORWARD')"
                )
            )
        engine.dispose()

        lease = claim_scratch_database(SERVER_URL, dbname)
        with pytest.raises(ScratchDatabaseError):
            lease.claim()

        engine = create_engine(_dsn(dbname))
        with engine.connect() as conn:
            assert conn.execute(text("SELECT count(*) FROM subjects")).scalar() == 1
            assert conn.execute(
                text("SELECT compatibility FROM subjects WHERE name='foreign-subject'")
            ).scalar() == "FORWARD"
        engine.dispose()

    def test_foreign_marker_table_is_never_ours(self, dbname):
        # Someone imitates the shape but writes a different suite id.
        with _server_conn() as c:
            c.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
            c.execute(
                sql.SQL("COMMENT ON DATABASE {} IS {}").format(
                    sql.Identifier(dbname), sql.Literal(owner_comment(dbname))
                )
            )
        with _scratch_conn(dbname) as c:
            c.execute(
                f"CREATE TABLE {MARKER_TABLE} "
                "(id int PRIMARY KEY CHECK (id=1), payload jsonb NOT NULL)"
            )
            c.execute(
                f"INSERT INTO {MARKER_TABLE} VALUES (1, %s)",
                ('{"suite": "some-other-suite"}',),
            )
        lease = claim_scratch_database(SERVER_URL, dbname)
        with pytest.raises(ScratchDatabaseError, match="foreign ownership marker"):
            lease.claim()
        assert _database_exists(dbname)

    def test_our_comment_plus_unexpected_table_fails(self, dbname):
        # Our claim exists but the database also contains a foreign table
        # (e.g. someone shares the database): refuse, never truncate.
        with claim_scratch_database(SERVER_URL, dbname) as lease:
            lease.assert_owned()
        # Recreate the claim shape manually, then add the foreign table.
        with _server_conn() as c:
            c.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
            c.execute(
                sql.SQL("COMMENT ON DATABASE {} IS {}").format(
                    sql.Identifier(dbname), sql.Literal(owner_comment(dbname))
                )
            )
        with _scratch_conn(dbname) as c:
            c.execute("CREATE TABLE someone_elses_data (id int)")
            c.execute("INSERT INTO someone_elses_data VALUES (99)")
        lease = claim_scratch_database(SERVER_URL, dbname)
        with pytest.raises(ScratchDatabaseError, match="does not own"):
            lease.claim()
        with _scratch_conn(dbname) as c:
            assert c.execute(
                "SELECT id FROM someone_elses_data"
            ).fetchall() == [(99,)]


# ---------------------------------------------------------------------------
# 2. Crash between CREATE DATABASE and marker write -> orphan is recyclable.
# ---------------------------------------------------------------------------


class TestOrphanFromInterruptedCreation:
    def test_orphan_with_comment_but_no_marker_is_recycled(self, dbname):
        with _server_conn() as c:
            c.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
            # The comment is the first thing a real run writes; a crash in
            # the window before the marker leaves exactly this shape.
            c.execute(
                sql.SQL("COMMENT ON DATABASE {} IS {}").format(
                    sql.Identifier(dbname), sql.Literal(owner_comment(dbname))
                )
            )

        with claim_scratch_database(SERVER_URL, dbname) as lease:
            assert lease.summary.action == "reclaimed_orphan"
            assert lease.summary.generation == 1
            lease.assert_owned()
        assert not _database_exists(dbname)

    def test_unmarked_empty_database_is_never_treated_as_our_orphan(
        self, dbname
    ):
        with _server_conn() as c:
            c.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
        lease = claim_scratch_database(SERVER_URL, dbname)
        with pytest.raises(ScratchDatabaseError, match="no ownership marker"):
            lease.claim()
        assert _database_exists(dbname)
        assert _database_comment(dbname) is None


# ---------------------------------------------------------------------------
# 3. Leftover of a dead run -> reclaimed, generation advances.
# ---------------------------------------------------------------------------


def _kill_run_without_teardown(lease) -> None:
    """Simulate the process dying mid-suite: heartbeat stops and every
    socket (raw owner/lock backends and pooled engine connections)
    vanishes as the OS would close them, but the database files remain."""
    if lease.engine is not None:
        lease.engine.dispose()
    lease._release_locks()
    lease._closed = True


class TestStaleLeftoverReclaim:
    def test_dead_run_leftover_is_reclaimed_with_next_generation(self, dbname):
        first = claim_scratch_database(
            SERVER_URL, dbname, heartbeat_interval=0.1, stale_after=STALE
        )
        first.claim()
        first_run_id = first.run_id
        assert first.summary.action == "created"
        _kill_run_without_teardown(first)

        assert _database_exists(dbname)
        assert _marker(dbname)["run_id"] == first_run_id
        assert _marker(dbname)["generation"] == 1

        # While the heartbeat is still fresh, takeover is refused even
        # though the owner backend already disappeared (both signals must
        # agree).
        eager = claim_scratch_database(SERVER_URL, dbname, stale_after=STALE)
        with pytest.raises(ScratchDatabaseError, match="live run"):
            eager.claim()
        # The refused attempt released its own lock and left data alone.
        assert _marker(dbname)["run_id"] == first_run_id

        # Past staleness: reclaim, and the generation ticks exactly once.
        time.sleep(STALE + 0.5)
        second = claim_scratch_database(SERVER_URL, dbname, stale_after=STALE)
        second.claim()
        try:
            assert second.summary.action == "reclaimed_stale"
            assert second.summary.previous_run_id == first_run_id
            assert second.summary.generation == 2
            marker = _marker(dbname)
            assert marker["generation"] == 2
            assert marker["run_id"] == second.run_id
        finally:
            second.release()
        assert not _database_exists(dbname)

    def test_generation_advances_across_repeated_interrupted_runs(self, dbname):
        # Run 1: fresh create. Then it dies; runs 2 and 3 each reclaim the
        # stale leftover and die in turn.
        prior = None
        for generation in (1, 2, 3):
            if generation == 1:
                lease = claim_scratch_database(
                    SERVER_URL, dbname, heartbeat_interval=0.1, stale_after=STALE
                )
            else:
                time.sleep(STALE + 0.5)
                lease = claim_scratch_database(
                    SERVER_URL, dbname,
                    heartbeat_interval=0.1, stale_after=STALE,
                )
            lease.claim()
            assert lease.generation == generation
            if prior is not None:
                assert lease.summary.previous_run_id == prior
            prior = lease.run_id
            _kill_run_without_teardown(lease)

        # A final healthy run reclaims and properly destroys the database.
        time.sleep(STALE + 0.5)
        with claim_scratch_database(
            SERVER_URL, dbname, stale_after=STALE
        ) as lease:
            assert lease.generation == 4
        assert not _database_exists(dbname)


# ---------------------------------------------------------------------------
# 4. Live run -> parallel startup fails bounded; after exit takeover works.
# ---------------------------------------------------------------------------


class TestParallelStartupBlocked:
    def test_parallel_claim_waits_then_fails_and_preserves_peer(self, dbname):
        with claim_scratch_database(
            SERVER_URL, dbname, heartbeat_interval=1.0
        ) as live:
            # Data the blocked peer must never touch; use a table outside
            # the app schema so nothing else manages it.
            with _scratch_conn(dbname) as c:
                c.execute("CREATE TABLE peer_only_data (id int)")
                c.execute("INSERT INTO peer_only_data VALUES (42)")

            errors: list[BaseException] = []
            peer = claim_scratch_database(
                SERVER_URL, dbname, lock_timeout=1.0, poll_interval=0.05
            )

            def claim():
                try:
                    peer.claim()
                except BaseException as exc:  # noqa: BLE001
                    errors.append(exc)

            t = threading.Thread(target=claim)
            started = time.monotonic()
            t.start()
            t.join()
            elapsed = time.monotonic() - started

            assert len(errors) == 1
            assert isinstance(errors[0], ScratchDatabaseError)
            assert "another run" in str(errors[0])
            # bounded wait: it pollled rather than hung or failed instantly
            assert 0.9 <= elapsed < 10.0

            # live lease intact: same run id, foreign row still present
            live.assert_owned()
            assert _marker(dbname)["run_id"] == live.run_id
            with _scratch_conn(dbname) as c:
                assert c.execute(
                    "SELECT id FROM peer_only_data"
                ).fetchall() == [(42,)]
        assert not _database_exists(dbname)

    def test_next_startup_takes_over_after_live_run_ends(self, dbname):
        with claim_scratch_database(SERVER_URL, dbname) as live:
            live_run_id = live.run_id
        assert not _database_exists(dbname)

        with claim_scratch_database(
            SERVER_URL, dbname, lock_timeout=2.0
        ) as nxt:
            assert nxt.summary.action == "created"
            assert nxt.run_id != live_run_id
        assert not _database_exists(dbname)


# ---------------------------------------------------------------------------
# 5. Legitimate lease: resets are safe, marker survives TRUNCATE, destroy.
# ---------------------------------------------------------------------------


class TestLeaseLifecycle:
    def test_reset_between_tests_clears_rows_but_keeps_marker(self, dbname):
        with claim_scratch_database(SERVER_URL, dbname) as lease:
            assert lease.summary.action == "created"

            with lease.engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO subjects (name, compatibility) "
                        "VALUES ('a', 'BACKWARD')"
                    )
                )
            lease.reset_app_tables()
            with lease.engine.connect() as conn:
                assert conn.execute(text("SELECT count(*) FROM subjects")).scalar() == 0
                assert (
                    conn.execute(
                        text(
                            f"SELECT payload->>'suite' FROM {MARKER_TABLE}"
                        )
                    ).scalar()
                    == SUITE_NAME
                )
                assert (
                    conn.execute(
                        text(f"SELECT payload->>'run_id' FROM {MARKER_TABLE}")
                    ).scalar()
                    == lease.run_id
                )

            # repeatable: seed again, reset again
            with lease.engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO subjects (name, compatibility) "
                        "VALUES ('b', 'FULL')"
                    )
                )
            lease.reset_app_tables()
            with lease.engine.connect() as conn:
                assert conn.execute(text("SELECT count(*) FROM subjects")).scalar() == 0
        assert not _database_exists(dbname)

    def test_heartbeat_advances_while_run_is_alive(self, dbname):
        with claim_scratch_database(
            SERVER_URL, dbname, heartbeat_interval=0.2
        ) as lease:
            first = _marker(dbname)["heartbeat_at"]
            time.sleep(0.7)
            second = _marker(dbname)["heartbeat_at"]
            assert second > first
            assert _marker(dbname)["run_id"] == lease.run_id

    def test_url_pointing_at_scratch_name_is_rejected(self, dbname):
        url = _psycopg_dsn(make_url(SERVER_URL).set(database=dbname))
        with pytest.raises(ScratchDatabaseError, match="not at the scratch"):
            claim_scratch_database(url, dbname)


# ---------------------------------------------------------------------------
# 6. Exit destruction: an external client must never be force-disconnected.
# ---------------------------------------------------------------------------


class TestExitRefusesExternalClients:
    def test_external_client_keeps_database_data_and_connection(self, dbname):
        # The run holds the advisory lock and both ownership artefacts are
        # valid (comment + marker naming this run_id); an unrelated client
        # simply keeps a connection to the scratch database open at exit.
        lease = claim_scratch_database(SERVER_URL, dbname)
        lease.claim()
        run_id = lease.run_id
        with lease.engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO subjects (name, compatibility) "
                    "VALUES ('in-use-row', 'BACKWARD')"
                )
            )

        external = _scratch_conn(dbname)
        try:
            external.execute(
                "CREATE TABLE external_holds (id int PRIMARY KEY, note text)"
            )
            external.execute(
                "INSERT INTO external_holds VALUES (1, 'EXTERNAL DATA')"
            )

            with pytest.raises(ScratchDatabaseError, match="still in use"):
                lease.release()

            # The external client was NOT force-disconnected: it keeps
            # querying the same backend, and all data is still readable.
            assert external.execute("SELECT 1").fetchone() == (1,)
            assert external.execute(
                "SELECT note FROM external_holds"
            ).fetchall() == [("EXTERNAL DATA",)]

            # Database, app data and the ownership artefacts survive.
            assert _database_exists(dbname)
            with _scratch_conn(dbname) as c:
                assert c.execute(
                    "SELECT name FROM subjects"
                ).fetchall() == [("in-use-row",)]
                assert c.execute(
                    f"SELECT payload->>'run_id' FROM {MARKER_TABLE}"
                ).fetchone()[0] == run_id
            assert _database_comment(dbname) == owner_comment(dbname)
        finally:
            external.close()

        # The refusal still released the advisory lock, so the next run is
        # not blocked behind a dead lock.
        assert _advisory_lock_is_free(dbname)

    def test_external_client_appears_only_while_closing_still_refused(
        self, dbname
    ):
        # The external connection does not exist during the occupancy
        # pre-check; it opens in the check→drop race window. The plain
        # (never FORCE) DROP must then be rejected by PostgreSQL itself.
        lease = claim_scratch_database(SERVER_URL, dbname)
        lease.claim()
        run_id = lease.run_id
        with lease.engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO subjects (name, compatibility) "
                    "VALUES ('racer-row', 'FULL')"
                )
            )

        racer_holder: dict = {}

        def open_racer():
            conn = _scratch_conn(dbname)
            conn.execute("CREATE TABLE racer (id int PRIMARY KEY)")
            conn.execute("INSERT INTO racer VALUES (7)")
            racer_holder["conn"] = conn

        lease._teardown_race_seam = open_racer
        with pytest.raises(ScratchDatabaseError, match="race window"):
            lease.release()

        try:
            racer = racer_holder["conn"]
            # The racer, having won the window, is still connected and its
            # data — together with the run's data — is intact.
            assert racer.execute("SELECT id FROM racer").fetchall() == [(7,)]
            assert _database_exists(dbname)
            with _scratch_conn(dbname) as c:
                assert c.execute(
                    "SELECT compatibility FROM subjects WHERE name='racer-row'"
                ).fetchone()[0] == "FULL"
                assert c.execute(
                    f"SELECT payload->>'run_id' FROM {MARKER_TABLE}"
                ).fetchone()[0] == run_id
        finally:
            if "conn" in racer_holder:
                racer_holder["conn"].close()

        assert _advisory_lock_is_free(dbname)

    def test_next_run_safely_judges_refused_teardown(self, dbname):
        # A refused teardown leaves comment + marker naming the dead run and
        # the lock released. While a client is still attached (and the
        # heartbeat fresh) a parallel run must refuse; once idle the dead
        # leftover is reclaimed exactly as any stale leftover.
        lease = claim_scratch_database(
            SERVER_URL, dbname, heartbeat_interval=0.1, stale_after=STALE
        )
        lease.claim()
        dead_run_id = lease.run_id

        external = _scratch_conn(dbname)
        external.execute("CREATE TEMP TABLE hold_session ()")
        external.execute("SELECT 1")
        try:
            with pytest.raises(ScratchDatabaseError, match="still in use"):
                lease.release()
            _wait_until_no_foreign_sessions(dbname)

            # Past the heartbeat window the leftover would be reclaimable by
            # staleness alone — but an attached client still makes the claim
            # fail at the idle gate rather than dropping a database in use.
            time.sleep(STALE + 0.5)
            with pytest.raises(ScratchDatabaseError, match="still in use"):
                claim_scratch_database(SERVER_URL, dbname, stale_after=STALE).claim()
            assert _database_exists(dbname)
        finally:
            external.close()
        _wait_until_no_foreign_sessions(dbname)

        # Once the client goes away and the run is stale, the next run
        # reclaims it (generation +1) and destroys it cleanly at its exit.
        time.sleep(STALE + 0.5)
        nxt = claim_scratch_database(SERVER_URL, dbname, stale_after=STALE)
        nxt.claim()
        try:
            assert nxt.summary.action == "reclaimed_stale"
            assert nxt.summary.previous_run_id == dead_run_id
            assert nxt.summary.generation == 2
        finally:
            nxt.release()
        assert not _database_exists(dbname)
        assert _advisory_lock_is_free(dbname)


# ---------------------------------------------------------------------------
# 7. Exit destruction of an idle, owned database stays plain and clean.
# ---------------------------------------------------------------------------


class TestExitDestroysIdleDatabase:
    def test_clean_exit_drops_and_releases_advisory_lock(self, dbname):
        assert _advisory_lock_is_free(dbname)
        with claim_scratch_database(SERVER_URL, dbname) as lease:
            run_id = lease.run_id
            lease.assert_owned()
            assert not _advisory_lock_is_free(dbname)
        assert not _database_exists(dbname)
        assert _advisory_lock_is_free(dbname)

        # A fresh run starts from nothing and also destroys on exit.
        with claim_scratch_database(SERVER_URL, dbname) as nxt:
            assert nxt.summary.action == "created"
            assert nxt.run_id != run_id
        assert not _database_exists(dbname)
        assert _advisory_lock_is_free(dbname)

    def test_clients_on_other_databases_do_not_block_exit(self, dbname):
        # Sessions attached to the *server* database (including this
        # suite's own advisory-lock backend) are not occupants.
        with claim_scratch_database(SERVER_URL, dbname):
            bystander = _server_conn()
            try:
                assert bystander.execute("SELECT 1").fetchone() == (1,)
            finally:
                bystander.close()
        assert not _database_exists(dbname)

