"""Verifiable ownership and run-generation for PostgreSQL scratch databases.

The PostgreSQL regression suites must never take over a database they
cannot prove they own. A name collision is not proof: another team,
another project or an older deployment may well have created a database
called ``contracts_concurrency_test`` that even contains a ``subjects``
table holding rows that are not ours.

This module gives the suite an *owned scratch database lease* backed by
three independently verifiable artefacts, all created on the PostgreSQL
**server** named by ``TEST_DATABASE_URL`` — never inside the database
that URL points at:

1. A cluster-wide advisory lock (``pg_advisory_lock`` keyed by the
   scratch database name) held on a dedicated connection for the whole
   run. Parallel suite startups serialize on it; a lock we cannot acquire
   within a bounded wait means another run is alive and using the
   database right now.
2. An ownership **comment on the database object**
   (``COMMENT ON DATABASE``), written immediately after ``CREATE
   DATABASE``. It closes the window between creating the database and
   writing the marker table: a half-created database left by a crash in
   that window carries no claim, whereas a stale database left by a dead
   run of this exact suite does.
3. A marker table (``_contracts_suite_owner``) written *inside* the
   scratch database, holding the suite identifier, a unique run id, a
   monotonically increasing generation, the owning backend PID and a
   heartbeat timestamp refreshed by a background thread while the run is
   alive.

Takeover rules, evaluated while holding the advisory lock:

* no ownership comment                     -> foreign database: FAIL,
  even when its tables happen to match our model names;
* our comment, but a non-suite table or a foreign marker is present
                                             -> FAIL, ownership is
  ambiguous or proven not ours;
* our marker, heartbeat fresh or owner PID still alive
                                             -> another run is using it:
  FAIL;
* our marker, heartbeat stale AND owner PID gone
                                             -> leftover of a dead run:
  safe to drop/recreate, the generation advances;
* our comment, no marker (crash between CREATE DATABASE and marker)
                                             -> empty orphan of our own
  suite: safe to drop/recreate;
* database does not exist                  -> create fresh.

Every destructive operation (DROP DATABASE, TRUNCATE) is additionally
guarded by :meth:`ScratchDatabaseLease.assert_owned`, which re-verifies
the current database name, the suite marker and the run id immediately
beforehand. When ownership cannot be proven the failure is explicit and
the original data is left untouched.

Exit destruction (``lease.release()``) is deliberately conservative.
Even when the comment and marker still prove this run's ownership, the
run first closes its *own* connections (application engine and owner
backend) and then, while still holding the advisory lock, checks for
**external clients** attached to the scratch database. If that check
fails, or any foreign client backend (or prepared transaction) is still
present, the drop is refused loudly and the database plus all its data is
kept — ``DROP DATABASE WITH (FORCE)`` is never used, so an external
client is never force-disconnected and a database still in use is never
deleted. Only when the database is verifiably idle is a plain ``DROP
DATABASE`` issued; a client that obtains a connection in the residual
check→drop race window therefore makes PostgreSQL itself reject the drop
(SQLSTATE 55006), which again preserves the data. The advisory lock is
held until the drop or the refusal completes, so a failed teardown leaves
an unambiguous, safely reclaimable state for the next run.
"""
from __future__ import annotations

import hashlib
import json
import struct
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import psycopg
from psycopg import sql
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from app.models import Base

SUITE_NAME = "contract-registry-concurrency"
MARKER_TABLE = "_contracts_suite_owner"

# A heartbeat older than this many seconds, together with a backend PID
# that no longer exists, proves the owning run is dead and its database
# is a reclaimable leftover.
STALE_AFTER_SECONDS = 30.0

# Marker payload keys
K_SUITE = "suite"
K_RUN_ID = "run_id"
K_GENERATION = "generation"
K_OWNER_PID = "owner_pid"
K_CREATED_AT = "created_at"
K_HEARTBEAT_AT = "heartbeat_at"


class ScratchDatabaseError(RuntimeError):
    """The suite cannot prove it owns the scratch database, or another
    run is still using it. Raised BEFORE any destructive statement, so
    the database and all of its rows are preserved."""


@dataclass(frozen=True)
class ClaimSummary:
    """What was found when claiming the scratch database."""

    action: str  # "created" | "reclaimed_stale" | "reclaimed_orphan"
    generation: int
    previous_run_id: str | None


def owner_comment(dbname: str) -> str:
    return f"owned-by:{SUITE_NAME};db:{dbname}"


def _parse_server_url(server_url: str):
    try:
        url = make_url(server_url)
    except Exception as exc:
        raise ScratchDatabaseError(
            f"TEST_DATABASE_URL ({server_url!r}) is not a valid database URL"
        ) from exc
    if not url.drivername.startswith("postgresql"):
        raise ScratchDatabaseError(
            f"TEST_DATABASE_URL ({server_url}) is not a PostgreSQL URL"
        )
    if not url.database:
        raise ScratchDatabaseError(
            f"TEST_DATABASE_URL ({server_url}) must name a database on the "
            "PostgreSQL server (e.g. .../contracts or .../postgres); it is "
            "only used to locate the server and is never modified"
        )
    return url


def _psycopg_dsn(url) -> str:
    """SQLAlchemy URL -> libpq URI accepted by psycopg.connect."""
    return url.render_as_string(hide_password=False).replace(
        "postgresql+psycopg://", "postgresql://", 1
    )


def _list_user_tables(conn) -> list[str]:
    """Persistent ordinary tables across the non-system schemas visible
    on the connection (public by default)."""
    rows = conn.execute(
        """
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND n.nspname NOT LIKE 'pg_toast%'
          AND n.nspname NOT LIKE 'pg_temp%'
          AND c.relkind IN ('r', 'p')
        """
    ).fetchall()
    return sorted(r[0] for r in rows)


def _hash64_key(name: str) -> tuple[int, int]:
    """Stable (int4, int4) advisory-lock key for a scratch name."""
    (value,) = struct.unpack(">Q", hashlib.sha256(name.encode()).digest()[:8])
    hi = value >> 32
    lo = value & 0xFFFFFFFF
    if hi >= 1 << 31:
        hi -= 1 << 32
    if lo >= 1 << 31:
        lo -= 1 << 32
    return hi, lo


class ScratchDatabaseLease:
    """Ownership lease for one scratch database on one PostgreSQL server.

    Prefer the context manager::

        with claim_scratch_database(SERVER_URL, dbname) as lease:
            lease.reset_app_tables()   # TRUNCATE, ownership re-checked
            ...
        # exiting closes our connections, refuses while an external client
        # is attached and otherwise runs a plain (never FORCE) DROP DATABASE
    """

    def __init__(
        self,
        server_url: str,
        dbname: str,
        *,
        lock_timeout: float = 60.0,
        poll_interval: float = 0.1,
        heartbeat_interval: float = 2.0,
        stale_after: float = STALE_AFTER_SECONDS,
        drop_settle_timeout: float = 5.0,
    ) -> None:
        self.server_url = server_url
        self.dbname = dbname
        self.lock_timeout = lock_timeout
        self.poll_interval = poll_interval
        self.heartbeat_interval = heartbeat_interval
        self.stale_after = stale_after
        self.drop_settle_timeout = drop_settle_timeout

        url = _parse_server_url(server_url)
        if url.database == dbname:
            raise ScratchDatabaseError(
                f"TEST_DATABASE_URL ({server_url}) must point at the "
                "PostgreSQL server (e.g. .../contracts or .../postgres), "
                f"not at the scratch test database {dbname!r} itself"
            )
        self._server_url_obj = url
        self.run_id = str(uuid.uuid4())
        self.generation = 1
        self.summary: ClaimSummary | None = None

        self._lock_conn: psycopg.Connection | None = None
        self._owner_conn: psycopg.Connection | None = None
        self._heartbeat_stop: threading.Event | None = None
        self._heartbeat_thread: threading.Thread | None = None
        self._closed = False
        self.engine = None

    # ------------------------------------------------------------------
    # connection helpers
    # ------------------------------------------------------------------

    def _connect_direct(
        self, database: str, *, application_name: str | None = None
    ) -> psycopg.Connection:
        """Raw psycopg connection. CREATE/DROP DATABASE and advisory
        locks cannot run inside a transaction block, so callers set
        autocommit.

        Every connection this lease opens carries an ``application_name``
        starting with the suite prefix. Teardown uses that label to tell
        its *own* backends apart from genuine other clients: only unnamed
        (foreign) sessions count as occupants that block the exit drop.
        """
        url = self._server_url_obj.set(database=database)
        conn = psycopg.connect(_psycopg_dsn(url), autocommit=True)
        if application_name is not None:
            conn.execute(
                "SELECT set_config('application_name', %s, false)",
                (application_name,),
            )
        return conn

    # ------------------------------------------------------------------
    # cluster-level introspection
    # ------------------------------------------------------------------

    def _lock_key(self) -> tuple[int, int]:
        return _hash64_key(f"{SUITE_NAME}:{self.dbname}")

    def _acquire_advisory_lock(self) -> None:
        """Take the cluster-wide advisory lock, polling so a parallel run
        produces a bounded, explicit failure instead of hanging."""
        conn = self._connect_direct(self._server_url_obj.database)
        conn.execute(
            "SELECT set_config('application_name', %s, false)",
            (f"{SUITE_NAME}:lock",),
        )
        conn.execute("SET lock_timeout = '500ms'")
        key = self._lock_key()
        deadline = time.monotonic() + self.lock_timeout
        while True:
            try:
                conn.execute(
                    "SELECT pg_advisory_lock(%s, %s)", (key[0], key[1])
                )
                break
            except psycopg.errors.LockNotAvailable:
                if time.monotonic() >= deadline:
                    conn.close()
                    raise ScratchDatabaseError(
                        f"scratch database {self.dbname!r} is still in use by "
                        "another run: the cluster advisory lock did not become "
                        f"free within {self.lock_timeout:.0f}s. Refusing to "
                        "take it over; the other run's data is untouched."
                    )
                time.sleep(self.poll_interval)
        self._lock_conn = conn

    def _database_exists(self, conn) -> bool:
        return (
            conn.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s", (self.dbname,)
            ).fetchone()
            is not None
        )

    def _database_comment(self, conn) -> str | None:
        return conn.execute(
            "SELECT shobj_description(oid, 'pg_database') "
            "FROM pg_database WHERE datname = %s",
            (self.dbname,),
        ).fetchone()[0]

    def _backend_alive(self, server_conn, pid: int | None) -> bool:
        if pid is None:
            return False
        return (
            server_conn.execute(
                "SELECT 1 FROM pg_stat_activity WHERE pid = %s", (pid,)
            ).fetchone()
            is not None
        )

    def _plain_drop_database(self, server_conn, *, when: str) -> None:
        """Drop the scratch database WITHOUT ``WITH (FORCE)``.

        The drop is deliberately non-forced: any client that obtains a
        connection to the scratch database in the window after the
        occupancy pre-check makes the statement fail (``ObjectInUse``,
        SQLSTATE 55006) instead of being forcibly disconnected. That
        failure is converted into an explicit, loud refusal; the database
        and its data are kept for the next run to judge.
        """
        try:
            server_conn.execute(
                sql.SQL("DROP DATABASE IF EXISTS {}").format(
                    sql.Identifier(self.dbname)
                )
            )
        except psycopg.errors.ObjectInUse as exc:
            raise ScratchDatabaseError(
                f"a new connection to {self.dbname!r} appeared during the "
                f"race window {when} ({exc}); refusing to destroy the "
                "database without FORCE. The database and all of its data "
                "are preserved; stop the connected client and let the next "
                "run reclaim it."
            ) from exc

    def _collect_other_usage(self, server_conn):
        """Return ``(foreign_sessions, prepared)`` that would make a drop
        unsafe. Raises when the server cannot be inspected: not being able
        to prove idleness means we must not drop."""
        app_prefix = f"{SUITE_NAME}:%"
        try:
            others = server_conn.execute(
                """
                SELECT pid, coalesce(usename, ''), coalesce(application_name, ''),
                       coalesce(state, '')
                FROM pg_stat_activity
                WHERE datname = %s
                  AND pid <> pg_backend_pid()
                  AND backend_type = 'client backend'
                  AND coalesce(application_name, '') NOT LIKE %s
                ORDER BY pid
                """,
                (self.dbname, app_prefix),
            ).fetchall()
            prepared = server_conn.execute(
                """
                SELECT x.gid, coalesce(x.owner::text, '')
                FROM pg_prepared_xacts x
                WHERE x.database = %s
                ORDER BY x.gid
                """,
                (self.dbname,),
            ).fetchall()
        except Exception as exc:  # noqa: BLE001 - cannot inspect -> do not drop
            raise ScratchDatabaseError(
                f"could not verify that {self.dbname!r} is unused before the "
                f"drop ({exc}); refusing to destroy it. The database and all "
                "of its data are preserved."
            ) from exc
        return others, prepared

    def _await_idle_for_drop(self, server_conn, *, timeout: float) -> None:
        """Gate a plain (never-forced) drop while holding the advisory lock.

        Refuses at once on any foreign client backend or prepared
        transaction. Backends labelled as *this* suite are our own
        inspection probes whose sockets were just closed but whose server
        processes may take a moment to exit; wait for them to disappear
        within a bounded settle window so they cannot make PostgreSQL
        reject the drop as still-in-use (a condition FORCE used to paper
        over by killing backends).
        """
        deadline = time.monotonic() + timeout
        while True:
            others, prepared = self._collect_other_usage(server_conn)
            if others or prepared:
                details = [
                    f"pid={pid} user={user!r} application={app!r} "
                    f"state={state!r}"
                    for pid, user, app, state in others
                ]
                details.extend(
                    f"prepared transaction gid={gid!r} owner={owner!r}"
                    for gid, owner in prepared
                )
                raise ScratchDatabaseError(
                    f"scratch database {self.dbname!r} is still in use by "
                    f"external client(s) ({'; '.join(details)}). Refusing to "
                    "DROP it with FORCE: doing so would force those clients "
                    "off and delete data that is still being used. The "
                    "database and all of its data are preserved; stop the "
                    "connected client(s) and let the next run reclaim it."
                )
            own = server_conn.execute(
                """
                SELECT 1
                FROM pg_stat_activity
                WHERE datname = %s
                  AND pid <> pg_backend_pid()
                  AND backend_type = 'client backend'
                  AND coalesce(application_name, '') LIKE %s
                """,
                (self.dbname, f"{SUITE_NAME}:%"),
            ).fetchall()
            if not own:
                return
            if time.monotonic() >= deadline:
                raise ScratchDatabaseError(
                    f"this run's own connections to {self.dbname!r} did not "
                    "close within the teardown settle window; refusing to "
                    "DROP the database. The database and all of its data are "
                    "preserved for the next run to judge."
                )
            time.sleep(self.poll_interval)

    def _assert_no_other_usage(self, server_conn) -> None:
        """Refuse the exit drop while anyone else is using the database.

        This runs only after *this run's* connections (application engine,
        owner/heartbeat backend, the ownership probe) have been closed, so
        every session still attached to the scratch database is by
        definition a foreign client. Two independently visible kinds of
        occupancy are checked while holding the advisory lock:

        * foreign **client backends** connected to the scratch database
          (our own backends are excluded by their ``application_name``
          prefix; background workers such as autovacuum are excluded by
          ``backend_type`` — PostgreSQL itself asks those to leave before a
          plain DROP rather than treating them as occupants);
        * **prepared transactions** bound to the scratch database, which
          keep the database in use even without a live session.

        Any introspection failure is treated as a failed check: we cannot
        prove nobody is using the database, so we keep it.
        """
        self._await_idle_for_drop(server_conn, timeout=self.drop_settle_timeout)

    # ------------------------------------------------------------------
    # scratch-database marker
    # ------------------------------------------------------------------

    def _read_marker(self, scratch_conn) -> dict | None:
        exists = scratch_conn.execute(
            """
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relname = %s AND c.relkind = 'r'
            """,
            (MARKER_TABLE,),
        ).fetchone()
        if exists is None:
            return None
        row = scratch_conn.execute(
            f"SELECT payload FROM {MARKER_TABLE} WHERE id = 1"
        ).fetchone()
        if row is None:
            return None
        return row[0] if isinstance(row[0], dict) else json.loads(row[0])

    def _is_stale(self, server_conn, marker: dict) -> bool:
        """Dead only when BOTH signals agree: heartbeat old AND backend
        PID gone. A fresh heartbeat or a still-live PID keeps us out."""
        try:
            age = float(
                server_conn.execute(
                    "SELECT EXTRACT(EPOCH FROM now() - %s::timestamptz)",
                    (marker[K_HEARTBEAT_AT],),
                ).fetchone()[0]
            )
        except Exception:
            age = None
        fresh = age is not None and age < self.stale_after
        pid = marker.get(K_OWNER_PID)
        return not fresh and not self._backend_alive(server_conn, pid)

    def _marker_payload(self, conn, generation: int) -> dict:
        now = datetime.now(timezone.utc).isoformat()
        return {
            K_SUITE: SUITE_NAME,
            K_RUN_ID: self.run_id,
            K_GENERATION: generation,
            K_OWNER_PID: conn.info.backend_pid,
            K_CREATED_AT: now,
            K_HEARTBEAT_AT: now,
        }

    def _create_marker(self, conn, generation: int) -> None:
        conn.execute(
            f"""
            CREATE TABLE {MARKER_TABLE} (
                id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
                payload JSONB NOT NULL
            )
            """
        )
        conn.execute(
            f"INSERT INTO {MARKER_TABLE} (id, payload) VALUES (1, %s)",
            (json.dumps(self._marker_payload(conn, generation)),),
        )

    def _start_heartbeat(self) -> None:
        stop = threading.Event()

        def beat() -> None:
            while not stop.wait(self.heartbeat_interval):
                try:
                    self._owner_conn.execute(
                        f"UPDATE {MARKER_TABLE} "
                        "SET payload = payload || %s::jsonb "
                        f"WHERE id = 1 AND payload->>'{K_RUN_ID}' = %s",
                        (
                            json.dumps(
                                {
                                    K_HEARTBEAT_AT: datetime.now(
                                        timezone.utc
                                    ).isoformat()
                                }
                            ),
                            self.run_id,
                        ),
                    )
                except Exception:
                    # If the heartbeat dies the run becomes a (correctly)
                    # reclaimable leftover after STALE_AFTER_SECONDS; the
                    # test thread itself must not die with it.
                    return

        thread = threading.Thread(
            target=beat, name="scratch-db-heartbeat", daemon=True
        )
        thread.start()
        self._heartbeat_stop = stop
        self._heartbeat_thread = thread

    # ------------------------------------------------------------------
    # claim
    # ------------------------------------------------------------------

    def claim(self) -> ClaimSummary:
        # 1. Cluster-wide serialization: parallel startups block here.
        self._acquire_advisory_lock()
        assert self._lock_conn is not None
        server = self._lock_conn

        action = "created"
        previous_run_id: str | None = None
        generation = 1

        try:
            # 2. Decide the fate of any same-named database.
            if self._database_exists(server):
                comment = self._database_comment(server)
                if comment != owner_comment(self.dbname):
                    self._fail_foreign(server, comment)

                probe = self._connect_direct(
                    self.dbname, application_name=f"{SUITE_NAME}:probe"
                )
                try:
                    relations = _list_user_tables(probe)
                    marker = self._read_marker(probe)
                finally:
                    probe.close()

                app_tables = set(Base.metadata.tables)
                unexpected = [
                    t
                    for t in relations
                    if t != MARKER_TABLE and t not in app_tables
                ]
                if unexpected:
                    raise ScratchDatabaseError(
                        f"database {self.dbname!r} carries this suite's "
                        "ownership comment but also holds tables the suite "
                        f"does not own ({', '.join(unexpected)}); refusing to "
                        "DROP or TRUNCATE it. Inspect it manually."
                    )
                if marker is not None and marker.get(K_SUITE) != SUITE_NAME:
                    raise ScratchDatabaseError(
                        f"database {self.dbname!r} contains a foreign "
                        f"ownership marker (suite={marker.get(K_SUITE)!r}); "
                        "refusing to take it over."
                    )
                if marker is not None and not self._is_stale(server, marker):
                    raise ScratchDatabaseError(
                        f"database {self.dbname!r} is still claimed by a live "
                        f"run (run_id={marker.get(K_RUN_ID)!r}, heartbeat="
                        f"{marker.get(K_HEARTBEAT_AT)!r}); refusing to take it "
                        "over while that run may be using it."
                    )
                if marker is not None:
                    action = "reclaimed_stale"
                    previous_run_id = marker.get(K_RUN_ID)
                    generation = int(marker.get(K_GENERATION, 0)) + 1
                else:
                    # Our comment but no marker: an empty orphan left by a
                    # crash between CREATE DATABASE and marker creation.
                    action = "reclaimed_orphan"

                # Reclaim of a proven-dead leftover. Our inspection probe
                # has closed; wait for any own backend to fully exit and
                # refuse on a genuinely foreign occupant, then issue a
                # plain, never-forced drop.
                self._await_idle_for_drop(
                    server, timeout=self.drop_settle_timeout
                )
                self._plain_drop_database(server, when="reclaim")

            # 3. Create, then stamp ownership at cluster level immediately —
            #    a crash in the next window leaves an identifiable orphan
            #    rather than an unmarked database someone cannot trust.
            server.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.dbname))
            )
            # COMMENT IS does not accept a bind parameter; the comment is a
            # fixed constant derived from the suite name and the identifier,
            # so quoting it as a literal is safe.
            server.execute(
                sql.SQL("COMMENT ON DATABASE {} IS {}").format(
                    sql.Identifier(self.dbname),
                    sql.Literal(owner_comment(self.dbname)),
                )
            )

            # 4. Marker table + owner connection + heartbeat.
            owner = self._connect_direct(self.dbname)
            owner.execute(
                "SELECT set_config('application_name', %s, false)",
                (f"{SUITE_NAME}:owner",),
            )
            self._owner_conn = owner
            self._create_marker(owner, generation)
            self.generation = generation
            self._start_heartbeat()
        except BaseException:
            self._release_locks()
            raise

        # 5. Application schema lives only in the scratch database. The
        #    engine's connections carry the suite label too, so the exit
        #    occupancy check never mistakes a (properly disposed) engine
        #    connection for a foreign client.
        engine_url = make_url(
            _psycopg_dsn(self._server_url_obj.set(database=self.dbname))
        )
        engine_url = engine_url.update_query_dict(
            {"application_name": f"{SUITE_NAME}:app"}
        )
        self.engine = create_engine(
            engine_url.render_as_string(hide_password=False),
            pool_pre_ping=True,
        )
        Base.metadata.create_all(self.engine)

        self.summary = ClaimSummary(
            action=action, generation=generation, previous_run_id=previous_run_id
        )
        return self.summary

    def _fail_foreign(self, server, comment: str) -> None:
        """Refuse a database with no ownership comment from this suite.
        List what is actually inside so the operator can see why."""
        details: list[str] = []
        probe = None
        try:
            probe = self._connect_direct(
                self.dbname, application_name=f"{SUITE_NAME}:probe"
            )
            tables = _list_user_tables(probe)
            if tables:
                details.append("tables: " + ", ".join(tables))
        except Exception as exc:  # noqa: BLE001 - diagnostics only
            details.append(f"could not inspect contents ({exc})")
        finally:
            if probe is not None:
                probe.close()
        inside = f" It contains {'; '.join(details)}." if details else ""
        raise ScratchDatabaseError(
            f"database {self.dbname!r} already exists on the server given by "
            f"TEST_DATABASE_URL ({self.server_url}) but carries no ownership "
            "marker from this suite (ownership comment "
            f"{comment!r}, expected {owner_comment(self.dbname)!r}). A shared "
            "name — even identical table names such as 'subjects' — is not "
            "proof of ownership, so the database may hold foreign data."
            f"{inside} Refusing to DROP or TRUNCATE it. Remove or rename it "
            "manually, or point TEST_DATABASE_URL at another server."
        )

    # ------------------------------------------------------------------
    # guards for destructive statements
    # ------------------------------------------------------------------

    def assert_owned(self) -> None:
        """Re-verify ownership immediately before DROP/TRUNCATE: we must
        be connected to the scratch database and its marker must name
        THIS run."""
        if self.engine is None or self._owner_conn is None:
            raise ScratchDatabaseError(
                "scratch database lease is not active; refusing any "
                "destructive statement"
            )
        with self.engine.connect() as conn:
            current = conn.scalar(text("SELECT current_database()"))
        if current != self.dbname:
            raise ScratchDatabaseError(
                f"connected to {current!r}, not the scratch database "
                f"{self.dbname!r}; refusing the destructive statement"
            )
        marker = self._read_marker(self._owner_conn)
        if marker is None or marker.get(K_SUITE) != SUITE_NAME:
            raise ScratchDatabaseError(
                f"{self.dbname!r} no longer carries this suite's ownership "
                "marker; refusing the destructive statement"
            )
        if marker.get(K_RUN_ID) != self.run_id:
            raise ScratchDatabaseError(
                f"{self.dbname!r} is now owned by run "
                f"{marker.get(K_RUN_ID)!r} (this run is {self.run_id!r}); "
                "refusing the destructive statement"
            )

    def reset_app_tables(self) -> None:
        """TRUNCATE the application tables between tests. The marker
        table is deliberately outside the truncation list and survives."""
        from app.events import reseed_counter

        self.assert_owned()
        tables = ", ".join(
            f'"{table.name}"' for table in Base.metadata.sorted_tables
        )
        with self.engine.begin() as conn:
            current = conn.scalar(text("SELECT current_database()"))
            if current != self.dbname:
                raise ScratchDatabaseError(
                    f"refusing to clean database {current!r}: the regression "
                    f"may only run in {self.dbname!r}"
                )
            conn.execute(text(f"TRUNCATE {tables} RESTART IDENTITY CASCADE"))
            # The event counter is a singleton row the app allocates
            # sequences from; TRUNCATE removed it, so re-seed it.
            reseed_counter(conn)

    # ------------------------------------------------------------------
    # teardown
    # ------------------------------------------------------------------

    def _close_scratch_connections(self) -> None:
        """Stop the heartbeat and close every connection THIS run holds
        *inside* the scratch database (owner backend and app engine), but
        keep the cluster advisory lock.

        This must complete before the exit occupancy check: once our own
        backends are gone, every session still attached to the scratch
        database is necessarily a foreign client. The advisory lock stays
        held until the drop decision is finished, so a parallel run cannot
        sneak in and reclaim a database we are about to inspect.
        """
        if self._heartbeat_stop is not None:
            self._heartbeat_stop.set()
            if self._heartbeat_thread is not None:
                self._heartbeat_thread.join(timeout=3)
            self._heartbeat_stop = None
            self._heartbeat_thread = None
        if self._owner_conn is not None:
            try:
                self._owner_conn.close()
            finally:
                self._owner_conn = None

    def _release_locks(self) -> None:
        """Close scratch-side connections and finally release the cluster
        advisory lock. The lock is held up to the last moment — until the
        drop either succeeded or was refused — so the post-failure state
        (valid comment + marker naming this run, lock free) can be judged
        safely by the next run under its own fresh lock acquisition."""
        self._close_scratch_connections()
        if self._lock_conn is not None:
            try:
                key = self._lock_key()
                self._lock_conn.execute(
                    "SELECT pg_advisory_unlock(%s, %s)", (key[0], key[1])
                )
            finally:
                try:
                    self._lock_conn.close()
                finally:
                    self._lock_conn = None

    def release(self, *, drop: bool = True) -> None:
        """Destroy the scratch database at run exit only when it is safe.

        Safety here is stricter than "we still own it": ownership must
        still be provable (database comment + marker naming THIS run),
        THIS run's own connections must all be closed first, and no
        external client may still be attached. Only then is a plain,
        never-forced ``DROP DATABASE`` issued, so a client appearing in
        the residual race window blocks the drop instead of being killed.

        On every failure path the database, its comment, marker and data
        are preserved and an explicit :class:`ScratchDatabaseError` is
        raised; the advisory lock is released afterwards, leaving a state
        the next run can safely classify as a live/leftover lease.
        """
        if self._closed:
            return
        self._closed = True

        if self.engine is not None:
            self.engine.dispose()

        server = self._lock_conn
        if drop and server is not None and not server.closed:
            try:
                # Keep the advisory lock throughout; close only our own
                # scratch-database connections before inspecting occupants.
                self._close_scratch_connections()
                self._drop_if_still_owner(server)
            finally:
                self._release_locks()
        else:
            self._release_locks()

    def _drop_if_still_owner(self, server) -> None:
        # 1. The database may already be gone (removed out of band).
        if not self._database_exists(server):
            return
        comment = self._database_comment(server)

        # 2. Re-prove ownership from inside the database with a labelled
        #    backend of our own, then close it: the occupancy check that
        #    follows must only see genuinely foreign sessions.
        marker = None
        probe = None
        try:
            probe = self._connect_direct(
                self.dbname, application_name=f"{SUITE_NAME}:probe"
            )
            marker = self._read_marker(probe)
        except Exception:
            marker = None
        finally:
            if probe is not None:
                probe.close()
        if not (
            comment == owner_comment(self.dbname)
            and marker is not None
            and marker.get(K_SUITE) == SUITE_NAME
            and marker.get(K_RUN_ID) == self.run_id
        ):
            raise ScratchDatabaseError(
                f"cannot prove this run still owns {self.dbname!r} at "
                "teardown (comment={!r}, marker run_id={!r}); leaving the "
                "database in place rather than dropping data that may "
                "belong to another run".format(
                    comment,
                    None if marker is None else marker.get(K_RUN_ID),
                )
            )

        # 3. Our own backends are closed; any remaining client backend (or
        #    prepared transaction) in the scratch database is foreign.
        self._assert_no_other_usage(server)

        # Test-only seam: the exit occupancy race regression opens a
        # foreign connection exactly here, between the pre-check and the
        # drop statement.
        seam = getattr(self, "_teardown_race_seam", None)
        if seam is not None:
            seam()

        # 4. Plain DROP, never FORCE: a connection arriving in this window
        #    makes PostgreSQL refuse the drop (55006), and we keep the data.
        self._plain_drop_database(server, when="at exit")

    def __enter__(self) -> "ScratchDatabaseLease":
        if self.summary is None:
            self.claim()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        # Even on test failure: an idle database this run owns is destroyed
        # with a plain, non-forced drop; a database still in use or no
        # longer provably ours is preserved, with a loud refusal.
        self.release()


def probe_server(server_url: str, *, connect_timeout: int = 3) -> str | None:
    """None when a PostgreSQL server is reachable, else a skip reason."""
    try:
        url = _parse_server_url(server_url)
    except ScratchDatabaseError as exc:
        return str(exc)
    engine = create_engine(
        _psycopg_dsn(url), connect_args={"connect_timeout": connect_timeout}
    )
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return None
    except Exception:
        return f"PostgreSQL not reachable at TEST_DATABASE_URL ({server_url})"
    finally:
        engine.dispose()


def claim_scratch_database(
    server_url: str, dbname: str, **kwargs
) -> ScratchDatabaseLease:
    return ScratchDatabaseLease(server_url, dbname, **kwargs)
