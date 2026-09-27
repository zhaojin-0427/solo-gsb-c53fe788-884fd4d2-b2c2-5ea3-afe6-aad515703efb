"""Compatibility-policy migration: replay, CAS confirmation, invalidation."""
from __future__ import annotations

import itertools
import threading
import time

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient

from app import migration as migration_service
from app import release as release_service
from app.db import SessionLocal
from app.main import app
from app.models import Migration, Release, Subject, Version
from app.schemas import Mode


def rec(name, *fields):
    return {
        "type": "record",
        "name": name,
        "fields": [
            {"id": i, "type": spec, "name": f"f{i}"} if isinstance(spec, str) else {"id": i, **spec}
            for i, spec in enumerate(fields, 1)
        ],
    }


# Histories below are all registrable under plain BACKWARD.
#
# C_V1..C_V3: adjacent pairs are backward- AND forward-compatible; the only
# hidden conflict is transitive: C_V3 cannot read C_V1 data (new required
# field f2 without a default), even though each step carried a default.
C_V1 = rec("C", "int")
C_V2 = rec("C", "int", {"name": "b", "type": "int", "default": 0})
C_V3 = rec("C", "int", {"name": "b", "type": "int"})  # f2 default dropped

# C_V4 drops required field f2 (like v1) but adds a new optional f3, so it
# stays content-distinct from v1: backward-compatible with v3 (the new
# reader ignores f2; f3 carries a default) but forward-incompatible with
# v3 (the v3 reader requires f2 without a default). Against v2, whose f2
# carries a default, it is forward-compatible.
C_V4 = rec(
    "C",
    "int",
    {"id": 3, "name": "c", "type": "string", "default": ""},
)

# D_V1..D_V3: widening history. int -> long is backward-compatible but
# forward-incompatible (reader int cannot read writer long).
D_V1 = rec("D", "int")
D_V2 = rec("D", "long")
D_V3 = rec("D", "long", {"name": "b", "type": "string", "default": ""})

ORDER_V1 = rec("Order", "int", "int")
ORDER_V2 = rec(
    "Order",
    "int",
    "long",
    {"id": 5, "name": "currency", "type": "string", "default": "USD"},
)
PAYMENT_V1 = rec("Payment", "int", "int")
PAYMENT_V2 = rec(
    "Payment",
    "int",
    "long",
    {"id": 5, "name": "currency", "type": "string", "default": "USD"},
)
SIMPLE_V1 = rec("S", "int")
SIMPLE_V2 = rec("S", "long")


@pytest.fixture()
def client():
    with TestClient(app) as c:
        yield c


@pytest.fixture(autouse=True)
def clean_db():
    yield
    db = SessionLocal()
    db.query(Migration).delete()
    db.query(Release).delete()
    db.query(Version).delete()
    db.query(Subject).delete()
    db.commit()
    db.close()


def create_subject(client, name, mode="BACKWARD"):
    resp = client.post("/subjects", json={"name": name, "compatibility": mode})
    assert resp.status_code == 201, resp.text


def register(client, name, content):
    resp = client.post(f"/subjects/{name}/versions", json={"content": content})
    assert resp.status_code == 201, resp.text
    return resp.json()


def migrate(client, targets):
    return client.post("/migrations", json={"subjects": targets})


def setup_history(client, name="users", mode="BACKWARD", versions=(C_V1, C_V2, C_V3)):
    create_subject(client, name, mode=mode)
    for content in versions:
        register(client, name, content)


class TestReplayUnit:
    def _rows(self, name):
        db = SessionLocal()
        try:
            subject = db.query(Subject).filter(Subject.name == name).one()
            return (
                db.query(Version)
                .filter(Version.subject_id == subject.id)
                .order_by(Version.version)
                .all()
            )
        finally:
            db.close()

    def test_backward_transitive_finds_non_adjacent_conflict(self, client):
        setup_history(client)
        pairs = migration_service.replay_history(
            self._rows("users"), Mode.BACKWARD_TRANSITIVE
        )
        # C_V3 vs C_V1 (f2 absent from the writer, no default on the reader);
        # every other pair is compatible.
        assert [(p["from_version"], p["to_version"]) for p in pairs] == [(1, 3)]
        pair = pairs[0]
        assert pair["direction"] == "backward"
        assert pair["rule"] == "required_field_missing_default"
        assert pair["path"] == ["b"]
        assert pair["path_string"] == "$.b"

    def test_plain_backward_replays_only_immediate_predecessor(self, client):
        setup_history(client)
        assert migration_service.replay_history(self._rows("users"), Mode.BACKWARD) == []

    def test_forward_replay_reports_direction_and_rule(self, client):
        setup_history(client, versions=(C_V1, C_V2, C_V3, C_V4))
        pairs = migration_service.replay_history(self._rows("users"), Mode.FORWARD)
        # C_V3 reader requires f2 (no default) but C_V4 drops it -> (3, 4);
        # earlier adjacent pairs are forward-compatible.
        assert [(p["from_version"], p["to_version"]) for p in pairs] == [(3, 4)]
        assert pairs[0]["direction"] == "forward"
        assert pairs[0]["rule"] == "required_field_missing_default"
        assert pairs[0]["path"] == ["b"]

    def test_full_transitive_reports_all_pairs_and_sorts(self, client):
        setup_history(client, versions=(C_V1, C_V2, C_V3, C_V4))
        pairs = migration_service.replay_history(
            self._rows("users"), Mode.FULL_TRANSITIVE
        )
        keys = {(p["from_version"], p["to_version"], p["direction"]) for p in pairs}
        # transitive backward surfaces only (1, 3); forward surfaces only
        # (3, 4): v1 never knew f2 and v2's f2 carried a default, so both
        # older readers cope with v4; v3's f2 is required.
        assert keys == {
            (1, 3, "backward"),
            (3, 4, "forward"),
        }
        ordered = [(p["to_version"], p["from_version"]) for p in pairs]
        assert ordered == sorted(ordered)

    def test_widening_history_blocks_forward_and_full(self, client):
        setup_history(client, name="wide", versions=(D_V1, D_V2, D_V3))
        # non-transitive FORWARD: only the immediate predecessor (v1) of the
        # first widening version v2 is replayed
        forward = migration_service.replay_history(self._rows("wide"), Mode.FORWARD)
        assert [(p["from_version"], p["to_version"]) for p in forward] == [(1, 2)]
        assert forward[0]["direction"] == "forward"
        assert forward[0]["rule"] == "primitive_type_mismatch"
        assert forward[0]["path"] == ["f1"]
        # transitive: v3 is also checked against v1
        transitive = migration_service.replay_history(
            self._rows("wide"), Mode.FORWARD_TRANSITIVE
        )
        assert [(p["from_version"], p["to_version"]) for p in transitive] == [
            (1, 2),
            (1, 3),
        ]
        # FULL latest-only finds the same adjacent conflict
        full = migration_service.replay_history(self._rows("wide"), Mode.FULL)
        assert [(p["from_version"], p["to_version"]) for p in full] == [(1, 2)]
        assert full[0]["direction"] == "forward"

    def test_empty_and_single_version_history_have_no_conflicts(self, client):
        create_subject(client, "empty")
        db = SessionLocal()
        try:
            subject = db.query(Subject).filter(Subject.name == "empty").one()
            rows = db.query(Version).filter(Version.subject_id == subject.id).all()
        finally:
            db.close()
        assert migration_service.replay_history(rows, Mode.FULL_TRANSITIVE) == []
        register(client, "empty", SIMPLE_V1)
        assert migration_service.replay_history(self._rows("empty"), Mode.FULL) == []


class TestCreate:
    def test_clean_migration_is_planned_and_changes_nothing(self, client):
        setup_history(client)
        # C history is clean under every non-transitive mode except plain
        # FORWARD/FULL against C_V4 — without v4, FULL (latest-only) is fine.
        resp = migrate(client, {"users": {"to_compatibility": "FULL"}})
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["status"] == "planned"
        assert body["created"] is True
        assert len(body["fingerprint"]) == 64
        (subject,) = body["plan"]["subjects"]
        assert subject["subject"] == "users"
        assert subject["baseline_version"] == 3
        assert subject["baseline_fingerprint"]
        assert subject["from_compatibility"] == "BACKWARD"
        assert subject["to_compatibility"] == "FULL"
        assert body["plan"]["conflicts"] == []
        (pre,) = body["plan"]["preconditions"]
        assert pre["compatibility"] == "BACKWARD"
        # planning has no side effects
        assert client.get("/subjects/users").json()["compatibility"] == "BACKWARD"

    def test_conflicting_migration_is_stored_blocked(self, client):
        setup_history(client)
        resp = migrate(
            client, {"users": {"to_compatibility": "BACKWARD_TRANSITIVE"}}
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["status"] == "blocked"
        (conflict,) = body["plan"]["conflicts"]
        assert conflict["subject"] == "users"
        assert conflict["from_version"] == 1
        assert conflict["to_version"] == 3
        assert conflict["direction"] == "backward"
        assert conflict["rule"] == "required_field_missing_default"
        assert conflict["path"] == ["b"]
        # the candidate is immutable and queryable
        fetched = client.get(f"/migrations/{body['fingerprint']}").json()
        assert fetched["status"] == "blocked"
        assert fetched["plan"]["conflicts"] == body["plan"]["conflicts"]
        (summary,) = client.get("/migrations").json()
        assert summary["status"] == "blocked"
        assert summary["subjects"] == ["users"]
        assert summary["conflicts"] == 1
        # no policy change happened
        assert client.get("/subjects/users").json()["compatibility"] == "BACKWARD"

    def test_blocked_candidate_aggregates_conflicts_across_subjects(self, client):
        setup_history(client, name="u1", versions=(C_V1, C_V2, C_V3))
        setup_history(client, name="u2", versions=(D_V1, D_V2, D_V3))
        resp = client.post(
            "/migrations",
            json={
                "subjects": {
                    "u1": {"to_compatibility": "BACKWARD_TRANSITIVE"},
                    "u2": {"to_compatibility": "FORWARD"},
                }
            },
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["status"] == "blocked"
        by_subject = {}
        for conflict in body["plan"]["conflicts"]:
            by_subject.setdefault(conflict["subject"], []).append(
                (conflict["from_version"], conflict["to_version"], conflict["direction"])
            )
        # u1: transitive backward (1,3); u2: forward latest-only (1,2)
        assert by_subject == {
            "u1": [(1, 3, "backward")],
            "u2": [(1, 2, "forward")],
        }
        assert [s["subject"] for s in body["plan"]["subjects"]] == ["u1", "u2"]
        # neither policy may be switched
        assert client.post(f"/migrations/{body['fingerprint']}/confirm").status_code == 409
        assert client.get("/subjects/u1").json()["compatibility"] == "BACKWARD"
        assert client.get("/subjects/u2").json()["compatibility"] == "BACKWARD"

    def test_cas_idempotent_resubmission(self, client):
        setup_history(client)
        first = migrate(client, {"users": {"to_compatibility": "BACKWARD_TRANSITIVE"}})
        assert first.status_code == 201
        second = migrate(client, {"users": {"to_compatibility": "BACKWARD_TRANSITIVE"}})
        assert second.status_code == 200
        assert second.json()["created"] is False
        assert second.json()["fingerprint"] == first.json()["fingerprint"]
        assert len(client.get("/migrations").json()) == 1

    def test_resubmission_after_drift_creates_new_candidate(self, client):
        setup_history(client)
        old = migrate(client, {"users": {"to_compatibility": "FULL"}}).json()
        assert old["plan"]["subjects"][0]["baseline_version"] == 3

        register(client, "users", C_V4)
        # FULL (latest-only) now surfaces the forward conflict (3, 4)
        replay = migrate(client, {"users": {"to_compatibility": "FULL"}})
        assert replay.status_code == 201
        assert replay.json()["created"] is True
        new = replay.json()
        assert new["fingerprint"] != old["fingerprint"]
        assert new["status"] == "blocked"
        assert new["plan"]["subjects"][0]["baseline_version"] == 4
        # both coexist; the stale one remains queryable
        assert [m["fingerprint"] for m in client.get("/migrations").json()] == [
            old["fingerprint"],
            new["fingerprint"],
        ]

    def test_noop_migration_rejected_and_not_stored(self, client):
        setup_history(client)
        resp = migrate(client, {"users": {"to_compatibility": "BACKWARD"}})
        assert resp.status_code == 409
        detail = resp.json()["detail"]
        assert "already in effect" in detail["message"]
        (entry,) = detail["subjects"]
        assert entry == {"subject": "users", "compatibility": "BACKWARD"}
        assert client.get("/migrations").json() == []

    def test_unknown_subject_404(self, client):
        assert migrate(client, {"ghost": {"to_compatibility": "FULL"}}).status_code == 404

    def test_empty_subjects_body_422(self, client):
        assert client.post("/migrations", json={"subjects": {}}).status_code == 422

    def test_invalid_mode_422(self, client):
        create_subject(client, "users")
        assert migrate(client, {"users": {"to_compatibility": "SIDEWAYS"}}).status_code == 422

    def test_missing_migration_404(self, client):
        assert client.get(f"/migrations/{'0' * 64}").status_code == 404
        assert client.post(f"/migrations/{'0' * 64}/confirm").status_code == 404


class TestConfirm:
    def test_confirm_atomically_switches_all_policies(self, client):
        setup_history(client, name="orders", versions=(D_V1, D_V2, D_V3))
        setup_history(client, name="payments", versions=(C_V1, C_V2, C_V3))
        resp = client.post(
            "/migrations",
            json={
                "subjects": {
                    "orders": {"to_compatibility": "BACKWARD_TRANSITIVE"},
                    "payments": {"to_compatibility": "FULL"},
                }
            },
        )
        assert resp.status_code == 201, resp.text
        assert resp.json()["status"] == "planned"
        fp = resp.json()["fingerprint"]
        confirm = client.post(f"/migrations/{fp}/confirm")
        assert confirm.status_code == 200, confirm.text
        body = confirm.json()
        assert body["status"] == "committed"
        switched = {s["subject"]: s["to_compatibility"] for s in body["subjects"]}
        assert switched == {"orders": "BACKWARD_TRANSITIVE", "payments": "FULL"}
        assert client.get("/subjects/orders").json()["compatibility"] == "BACKWARD_TRANSITIVE"
        assert client.get("/subjects/payments").json()["compatibility"] == "FULL"
        detail = client.get(f"/migrations/{fp}").json()
        assert detail["status"] == "committed"
        assert detail["committed_at"] is not None
        # versions were never touched by a migration
        assert [v["version"] for v in client.get("/subjects/orders/versions").json()] == [1, 2, 3]

    def test_confirm_is_idempotent(self, client):
        setup_history(client)
        fp = migrate(client, {"users": {"to_compatibility": "FULL"}}).json()["fingerprint"]
        first = client.post(f"/migrations/{fp}/confirm")
        again = client.post(f"/migrations/{fp}/confirm")
        assert first.status_code == 200
        assert again.json() == first.json()

    def test_blocked_candidate_cannot_be_confirmed(self, client):
        setup_history(client)
        fp = migrate(
            client, {"users": {"to_compatibility": "BACKWARD_TRANSITIVE"}}
        ).json()["fingerprint"]
        resp = client.post(f"/migrations/{fp}/confirm")
        assert resp.status_code == 409
        detail = resp.json()["detail"]
        assert detail["rule"] == "history_replay_conflicts"
        (conflict,) = detail["conflicts"]
        assert conflict["from_version"] == 1 and conflict["to_version"] == 3
        # stays blocked; policy untouched
        assert client.get(f"/migrations/{fp}").json()["status"] == "blocked"
        assert client.get("/subjects/users").json()["compatibility"] == "BACKWARD"

    def test_confirm_rejects_version_drift_without_changes(self, client):
        setup_history(client)
        fp = migrate(client, {"users": {"to_compatibility": "FULL"}}).json()["fingerprint"]
        register(client, "users", C_V4)
        resp = client.post(f"/migrations/{fp}/confirm")
        assert resp.status_code == 409
        (conflict,) = resp.json()["detail"]["conflicts"]
        assert conflict == {
            "subject": "users",
            "kind": "version_drift",
            "expected_baseline": 3,
            "actual_latest": 4,
        }
        assert client.get("/subjects/users").json()["compatibility"] == "BACKWARD"
        assert client.get(f"/migrations/{fp}").json()["status"] == "planned"

    def test_confirm_rejects_policy_drift(self, client):
        setup_history(client)
        fp = migrate(client, {"users": {"to_compatibility": "FULL"}}).json()["fingerprint"]
        # policy moved out of band via the existing config API
        client.put("/subjects/users/config", json={"compatibility": "FORWARD"})
        resp = client.post(f"/migrations/{fp}/confirm")
        assert resp.status_code == 409
        (conflict,) = resp.json()["detail"]["conflicts"]
        assert conflict == {
            "subject": "users",
            "kind": "compatibility_drift",
            "expected_compatibility": "BACKWARD",
            "actual_compatibility": "FORWARD",
        }

    def test_any_subject_drift_fails_all_subjects(self, client):
        setup_history(client, name="a", versions=(C_V1, C_V2, C_V3))
        setup_history(client, name="b", versions=(C_V1, C_V2, C_V3))
        fp = client.post(
            "/migrations",
            json={
                "subjects": {
                    "a": {"to_compatibility": "FULL"},
                    "b": {"to_compatibility": "FULL"},
                }
            },
        ).json()["fingerprint"]
        register(client, "b", C_V4)
        resp = client.post(f"/migrations/{fp}/confirm")
        assert resp.status_code == 409
        (conflict,) = resp.json()["detail"]["conflicts"]
        assert conflict["subject"] == "b"
        assert conflict["kind"] == "version_drift"
        # neither subject was switched
        assert client.get("/subjects/a").json()["compatibility"] == "BACKWARD"
        assert client.get("/subjects/b").json()["compatibility"] == "BACKWARD"

    def test_migration_on_subject_without_history(self, client):
        create_subject(client, "fresh")
        fp = migrate(
            client, {"fresh": {"to_compatibility": "FULL_TRANSITIVE"}}
        ).json()["fingerprint"]
        resp = client.post(f"/migrations/{fp}/confirm")
        assert resp.status_code == 200
        assert client.get("/subjects/fresh").json()["compatibility"] == "FULL_TRANSITIVE"


class TestReleaseInvalidation:
    """Committing a policy migration changes the snapshot; coordinated
    release candidates planned on the old snapshot keep their original
    global-CAS failure semantics (compatibility_drift)."""

    def setup_release(self, client):
        create_subject(client, "orders")
        create_subject(client, "payments")
        register(client, "orders", ORDER_V1)
        register(client, "payments", PAYMENT_V1)
        body = {
            "subjects": {
                "orders": {"content": ORDER_V2},
                "payments": {"content": PAYMENT_V2},
            },
            "edges": [{"producer": "orders", "consumer": "payments"}],
        }
        resp = client.post("/releases", json=body)
        assert resp.status_code == 201, resp.text
        return resp.json()["fingerprint"]

    def test_committed_migration_invalidates_planned_release(self, client):
        release_fp = self.setup_release(client)
        migration_fp = migrate(
            client, {"orders": {"to_compatibility": "FORWARD_TRANSITIVE"}}
        ).json()["fingerprint"]
        assert client.post(f"/migrations/{migration_fp}/confirm").status_code == 200
        stale = client.post(f"/releases/{release_fp}/confirm")
        assert stale.status_code == 409
        (conflict,) = stale.json()["detail"]["conflicts"]
        assert conflict == {
            "subject": "orders",
            "kind": "compatibility_drift",
            "expected_compatibility": "BACKWARD",
            "actual_compatibility": "FORWARD_TRANSITIVE",
        }
        # the release is still queryable; no versions were written by it
        assert client.get(f"/releases/{release_fp}").json()["status"] == "planned"

    def test_uncommitted_migration_does_not_invalidate_release(self, client):
        release_fp = self.setup_release(client)
        migrate(client, {"orders": {"to_compatibility": "FORWARD_TRANSITIVE"}})
        assert client.post(f"/releases/{release_fp}/confirm").status_code == 200


@pytest.fixture()
def live_server():
    config = uvicorn.Config(app, host="127.0.0.1", port=8175, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(100):
        try:
            httpx.get("http://127.0.0.1:8175/health", timeout=0.2)
            break
        except Exception:
            time.sleep(0.05)
    yield "http://127.0.0.1:8175"
    server.should_exit = True
    thread.join(timeout=5)


def synchronise_baseline_reads(monkeypatch):
    """Barrier the first two baseline reads so both confirms check the
    snapshot before either commits the policy switch."""
    barrier = threading.Barrier(2)
    calls = itertools.count()
    original = release_service.baseline_conflicts

    def synchronised(db, entries):
        result = original(db, entries)
        if next(calls) < 2:
            barrier.wait(timeout=10)
        return result

    monkeypatch.setattr(release_service, "baseline_conflicts", synchronised)
    # migration imported the function object by name into its own namespace
    monkeypatch.setattr(migration_service, "baseline_conflicts", synchronised)


def run_concurrently(*tasks):
    threads = [threading.Thread(target=t) for t in tasks]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


class TestConfirmConcurrency:
    def test_same_migration_confirmed_concurrently_converges(
        self, live_server, monkeypatch
    ):
        base = live_server
        assert httpx.post(f"{base}/subjects", json={"name": "mx"}).status_code == 201
        assert (
            httpx.post(f"{base}/subjects/mx/versions", json={"content": SIMPLE_V1}).status_code
            == 201
        )
        fp = httpx.post(
            f"{base}/migrations",
            json={"subjects": {"mx": {"to_compatibility": "FULL"}}},
        ).json()["fingerprint"]

        synchronise_baseline_reads(monkeypatch)
        results: list[int] = []

        def confirm():
            results.append(
                httpx.post(f"{base}/migrations/{fp}/confirm", timeout=15).status_code
            )

        run_concurrently(confirm, confirm)

        assert sorted(results) == [200, 200]  # idempotent convergence
        assert httpx.get(f"{base}/subjects/mx").json()["compatibility"] == "FULL"

    def test_conflicting_migrations_exactly_one_commits(
        self, live_server, monkeypatch
    ):
        base = live_server
        assert httpx.post(f"{base}/subjects", json={"name": "my"}).status_code == 201
        assert (
            httpx.post(f"{base}/subjects/my/versions", json={"content": SIMPLE_V1}).status_code
            == 201
        )

        def create(mode):
            return httpx.post(
                f"{base}/migrations",
                json={"subjects": {"my": {"to_compatibility": mode}}},
            ).json()["fingerprint"]

        fp_full = create("FULL")
        fp_forward = create("FORWARD")

        synchronise_baseline_reads(monkeypatch)
        results: list[int] = []

        def confirm(fp):
            results.append(
                httpx.post(f"{base}/migrations/{fp}/confirm", timeout=15).status_code
            )

        run_concurrently(lambda: confirm(fp_full), lambda: confirm(fp_forward))

        assert sorted(results) == [200, 409]
        mode = httpx.get(f"{base}/subjects/my").json()["compatibility"]
        assert mode in ("FULL", "FORWARD")
        # the loser is now reported stale against whichever mode won
        loser = fp_forward if mode == "FULL" else fp_full
        assert httpx.post(f"{base}/migrations/{loser}/confirm").status_code == 409

    def test_overlapping_candidates_same_target_mode_exactly_one_commits(
        self, live_server, monkeypatch
    ):
        """Different candidates may share a subject AND its target mode;
        a row already carrying the target mode must not be read as 'this
        candidate committed' — the baseline CAS decides, the loser rolls
        back entirely."""
        base = live_server
        for name in ("m1", "m2"):
            assert httpx.post(f"{base}/subjects", json={"name": name}).status_code == 201
            assert (
                httpx.post(
                    f"{base}/subjects/{name}/versions", json={"content": SIMPLE_V1}
                ).status_code
                == 201
            )

        def create(targets):
            return httpx.post(
                f"{base}/migrations", json={"subjects": targets}
            ).json()["fingerprint"]

        fp_one = create({"m1": {"to_compatibility": "FULL"}})
        fp_two = create(
            {"m1": {"to_compatibility": "FULL"}, "m2": {"to_compatibility": "FULL"}}
        )
        assert fp_one != fp_two

        synchronise_baseline_reads(monkeypatch)
        results: list[int] = []

        def confirm(fp):
            results.append(
                httpx.post(f"{base}/migrations/{fp}/confirm", timeout=15).status_code
            )

        run_concurrently(lambda: confirm(fp_one), lambda: confirm(fp_two))

        assert sorted(results) == [200, 409]
        assert httpx.get(f"{base}/subjects/m1").json()["compatibility"] == "FULL"
        # m2 is switched iff the two-subject candidate won; the loser's own
        # switches were rolled back wholesale
        m2_mode = httpx.get(f"{base}/subjects/m2").json()["compatibility"]
        winner, loser = (fp_one, fp_two) if m2_mode == "BACKWARD" else (fp_two, fp_one)
        assert httpx.get(f"{base}/migrations/{winner}").json()["status"] == "committed"
        assert httpx.get(f"{base}/migrations/{loser}").json()["status"] == "planned"
        # the loser never becomes re-confirmable: its baseline CAS now
        # reports the winner's switch as compatibility drift
        assert httpx.post(f"{base}/migrations/{loser}/confirm").status_code == 409
