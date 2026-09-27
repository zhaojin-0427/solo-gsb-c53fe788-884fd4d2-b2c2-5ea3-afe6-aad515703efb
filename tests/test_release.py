"""Coordinated cross-subject releases: planning, idempotency, global CAS."""
from __future__ import annotations

import itertools
import threading
import time

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient

from app import release as release_service
from app.db import SessionLocal
from app.main import app
from app.models import Release, Subject, Version


def record(name, fields):
    return {"type": "record", "name": name, "fields": fields}


ORDER_V1 = record(
    "Order",
    [
        {"id": 1, "name": "order_id", "type": "int"},
        {"id": 2, "name": "amount", "type": "int"},
    ],
)
# amount widened int -> long, optional currency added: BACKWARD compatible
ORDER_V2 = record(
    "Order",
    [
        {"id": 1, "name": "order_id", "type": "int"},
        {"id": 2, "name": "amount", "type": "long"},
        {"id": 5, "name": "currency", "type": "string", "default": "USD"},
    ],
)
PAYMENT_V1 = record(
    "Payment",
    [
        {"id": 1, "name": "payment_id", "type": "int"},
        {"id": 2, "name": "amount", "type": "int"},
    ],
)
# compatible with its own history AND able to read ORDER_V2
PAYMENT_V2 = record(
    "Payment",
    [
        {"id": 1, "name": "payment_id", "type": "int"},
        {"id": 2, "name": "amount", "type": "long"},
        {"id": 5, "name": "currency", "type": "string", "default": "USD"},
    ],
)
# fine against its own history, but cannot read ORDER_V2's string currency
PAYMENT_V2_BAD = record(
    "Payment",
    [
        {"id": 1, "name": "payment_id", "type": "int"},
        {"id": 2, "name": "amount", "type": "long"},
        {"id": 5, "name": "currency", "type": "int", "default": 0},
    ],
)
AUDIT_V1 = record("Audit", [{"id": 1, "name": "seq", "type": "int"}])
AUDIT_V2 = record(
    "Audit",
    [
        {"id": 1, "name": "seq", "type": "int"},
        {"id": 2, "name": "amount", "type": "long", "default": 0},
    ],
)
# an intermediate orders evolution: amount widened int -> long only.
# lets ORDER_V2 remain a genuine advance when it is replayed after a drift.
ORDER_MID = record(
    "Order",
    [
        {"id": 1, "name": "order_id", "type": "int"},
        {"id": 2, "name": "amount", "type": "long"},
    ],
)
SIMPLE_V1 = record("R", [{"id": 1, "name": "x", "type": "int"}])
SIMPLE_V2 = record("R", [{"id": 1, "name": "x", "type": "long"}])
ALT_V2 = record(
    "R",
    [
        {"id": 1, "name": "x", "type": "long"},
        {"id": 2, "name": "y", "type": "string", "default": ""},
    ],
)


@pytest.fixture()
def client():
    with TestClient(app) as c:
        yield c


@pytest.fixture(autouse=True)
def clean_db():
    yield
    db = SessionLocal()
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


def setup_orders_payments(client):
    create_subject(client, "orders")
    create_subject(client, "payments")
    register(client, "orders", ORDER_V1)
    register(client, "payments", PAYMENT_V1)


def orders_payments_candidate(payment_content=PAYMENT_V2):
    return {
        "subjects": {
            "orders": {"content": ORDER_V2},
            "payments": {"content": payment_content},
        },
        "edges": [{"producer": "orders", "consumer": "payments"}],
    }


class TestPlanning:
    def test_chain_produces_minimal_batches(self, client):
        setup_orders_payments(client)
        create_subject(client, "audit")
        register(client, "audit", AUDIT_V1)
        body = orders_payments_candidate()
        body["subjects"]["audit"] = {"content": AUDIT_V2}
        body["edges"].append({"producer": "payments", "consumer": "audit"})

        resp = client.post("/releases", json=body)
        assert resp.status_code == 201, resp.text
        plan = resp.json()["plan"]
        batches = plan["batches"]
        assert [[s["subject"] for s in b["subjects"]] for b in batches] == [
            ["orders"],
            ["payments"],
            ["audit"],
        ]
        assert [b["batch"] for b in batches] == [0, 1, 2]
        # every batch lists the data-flow edges it satisfies
        assert batches[0]["edges"] == []
        assert batches[1]["edges"] == [
            {
                "producer": "orders",
                "consumer": "payments",
                "producer_version": 2,
                "consumer_version": 2,
            }
        ]
        assert batches[2]["edges"][0]["producer"] == "payments"
        # preconditions capture the snapshot baselines for the global CAS
        v1_fps = {
            name: client.get(f"/subjects/{name}/versions/1").json()["fingerprint"]
            for name in ("audit", "orders", "payments")
        }
        assert plan["preconditions"] == [
            {
                "subject": "audit",
                "baseline_version": 1,
                "baseline_fingerprint": v1_fps["audit"],
                "compatibility": "BACKWARD",
            },
            {
                "subject": "orders",
                "baseline_version": 1,
                "baseline_fingerprint": v1_fps["orders"],
                "compatibility": "BACKWARD",
            },
            {
                "subject": "payments",
                "baseline_version": 1,
                "baseline_fingerprint": v1_fps["payments"],
                "compatibility": "BACKWARD",
            },
        ]
        # the plan subjects carry the same snapshot fingerprint
        plan_subjects = {s["subject"]: s for s in plan["subjects"]}
        assert plan_subjects["orders"]["baseline_fingerprint"] == v1_fps["orders"]
        # planning has no side effects on the subjects
        assert [v["version"] for v in client.get("/subjects/orders/versions").json()] == [1]

    def test_diamond_minimises_batches_and_sorts_within_batch(self, client):
        for name in "abcd":
            create_subject(client, name)
            register(client, name, SIMPLE_V1)
        body = {
            "subjects": {name: {"content": SIMPLE_V2} for name in "abcd"},
            "edges": [
                {"producer": "a", "consumer": "b"},
                {"producer": "a", "consumer": "c"},
                {"producer": "b", "consumer": "d"},
                {"producer": "c", "consumer": "d"},
            ],
        }
        resp = client.post("/releases", json=body)
        assert resp.status_code == 201, resp.text
        batches = resp.json()["plan"]["batches"]
        assert [[s["subject"] for s in b["subjects"]] for b in batches] == [
            ["a"],
            ["b", "c"],
            ["d"],
        ]

    def test_independent_subjects_share_one_batch(self, client):
        setup_orders_payments(client)
        resp = client.post(
            "/releases",
            json={
                "subjects": {
                    "payments": {"content": PAYMENT_V2},
                    "orders": {"content": ORDER_V2},
                },
                "edges": [],
            },
        )
        assert resp.status_code == 201, resp.text
        (batch,) = resp.json()["plan"]["batches"]
        assert [s["subject"] for s in batch["subjects"]] == ["orders", "payments"]
        assert batch["edges"] == []

    def test_plan_is_deterministic_across_endpoints(self, client):
        setup_orders_payments(client)
        created = client.post("/releases", json=orders_payments_candidate()).json()
        fp = created["fingerprint"]
        assert len(fp) == 64
        fetched = client.get(f"/releases/{fp}").json()
        assert fetched["plan"] == created["plan"]
        assert fetched["status"] == "planned"
        assert fetched["committed_at"] is None
        listing = client.get("/releases").json()
        assert [r["fingerprint"] for r in listing] == [fp]
        assert listing[0]["subjects"] == ["orders", "payments"]

    def test_unknown_release_404(self, client):
        assert client.get(f"/releases/{'0' * 64}").status_code == 404
        assert client.post(f"/releases/{'0' * 64}/confirm").status_code == 404


class TestIdempotency:
    def test_identical_candidate_hits_cas(self, client):
        setup_orders_payments(client)
        first = client.post("/releases", json=orders_payments_candidate())
        assert first.status_code == 201
        assert first.json()["created"] is True

        # semantically identical: field order shuffled, longhand type form,
        # subjects/edges order swapped, duplicate edge collapsed
        shuffled_order = record(
            "Order",
            [
                {"id": 5, "name": "currency", "type": {"type": "string"}, "default": "USD"},
                {"id": 2, "name": "amount", "type": "long"},
                {"id": 1, "name": "order_id", "type": "int"},
            ],
        )
        edge = {"producer": "orders", "consumer": "payments"}
        replay = {
            "subjects": {
                "payments": {"content": PAYMENT_V2},
                "orders": {"content": shuffled_order},
            },
            "edges": [edge, dict(edge)],
        }
        second = client.post("/releases", json=replay)
        assert second.status_code == 200
        assert second.json()["created"] is False
        assert second.json()["fingerprint"] == first.json()["fingerprint"]
        assert len(client.get("/releases").json()) == 1


class TestSnapshotReplay:
    """Resubmitting the same target contracts after a baseline change must
    produce a *new* candidate bound to the new snapshot — never silently
    reuse the stale plan. The stale candidate stays queryable and its
    confirm keeps failing the global CAS."""

    def test_replay_after_version_drift_creates_fresh_candidate(self, client):
        setup_orders_payments(client)
        first = client.post("/releases", json=orders_payments_candidate())
        assert first.status_code == 201
        old_fp = first.json()["fingerprint"]
        old_plan = first.json()["plan"]
        assert [s["baseline_version"] for s in old_plan["subjects"]] == [1, 1]

        # a participating subject publishes a new version out of band
        register(client, "orders", ORDER_MID)
        order_v2_fp = client.get("/subjects/orders/versions/2").json()["fingerprint"]

        # replay the *same* target contracts and edge -> new candidate/plan
        replay = client.post("/releases", json=orders_payments_candidate())
        assert replay.status_code == 201
        assert replay.json()["created"] is True
        new_fp = replay.json()["fingerprint"]
        assert new_fp != old_fp
        new_plan = replay.json()["plan"]
        order_plan = next(s for s in new_plan["subjects"] if s["subject"] == "orders")
        payment_plan = next(
            s for s in new_plan["subjects"] if s["subject"] == "payments"
        )
        assert order_plan["baseline_version"] == 2
        assert order_plan["baseline_fingerprint"] == order_v2_fp
        assert order_plan["target_version"] == 3
        # the untouched subject keeps its snapshot
        assert payment_plan["baseline_version"] == 1
        assert payment_plan["target_version"] == 2
        order_pre = next(
            p for p in new_plan["preconditions"] if p["subject"] == "orders"
        )
        assert order_pre == {
            "subject": "orders",
            "baseline_version": 2,
            "baseline_fingerprint": order_v2_fp,
            "compatibility": "BACKWARD",
        }

        # the new candidate is itself CAS-idempotent on the new snapshot
        again = client.post("/releases", json=orders_payments_candidate())
        assert again.status_code == 200
        assert again.json()["created"] is False
        assert again.json()["fingerprint"] == new_fp

        # both candidates coexist; the old one is still queryable
        listing = client.get("/releases").json()
        assert [r["fingerprint"] for r in listing] == [old_fp, new_fp]
        assert client.get(f"/releases/{old_fp}").json()["status"] == "planned"
        assert client.get(f"/releases/{new_fp}").json()["status"] == "planned"

        # the old candidate still cannot confirm (bound to the old baseline)
        stale = client.post(f"/releases/{old_fp}/confirm")
        assert stale.status_code == 409
        (conflict,) = stale.json()["detail"]["conflicts"]
        assert conflict["subject"] == "orders"
        assert conflict["kind"] == "version_drift"
        assert conflict["expected_baseline"] == 1
        assert conflict["actual_latest"] == 2

        # the fresh candidate confirms against the new baseline
        committed = client.post(f"/releases/{new_fp}/confirm")
        assert committed.status_code == 200
        versions = {v["subject"]: v["version"] for v in committed.json()["versions"]}
        assert versions == {"orders": 3, "payments": 2}
        assert client.get(f"/releases/{old_fp}").json()["status"] == "planned"

    def test_replay_when_drift_made_target_a_noop_is_conflict(self, client):
        setup_orders_payments(client)
        first = client.post("/releases", json=orders_payments_candidate())
        old_fp = first.json()["fingerprint"]

        # someone registers exactly what the candidate planned for orders
        register(client, "orders", ORDER_V2)

        # the same submission is now a no-op on the new snapshot
        replay = client.post("/releases", json=orders_payments_candidate())
        assert replay.status_code == 409
        (entry,) = replay.json()["detail"]["subjects"]
        assert entry["subject"] == "orders"
        assert entry["existing_version"] == 2
        # the stale candidate is untouched and still unconfirmable
        assert [r["fingerprint"] for r in client.get("/releases").json()] == [old_fp]
        assert client.post(f"/releases/{old_fp}/confirm").status_code == 409

    def test_replay_after_compatibility_change_creates_fresh_candidate(self, client):
        setup_orders_payments(client)
        old_fp = client.post(
            "/releases", json=orders_payments_candidate()
        ).json()["fingerprint"]

        # switching to a strictly stronger policy changes the snapshot
        resp = client.put(
            "/subjects/orders/config",
            json={"compatibility": "BACKWARD_TRANSITIVE"},
        )
        assert resp.status_code == 200

        replay = client.post("/releases", json=orders_payments_candidate())
        assert replay.status_code == 201
        new_fp = replay.json()["fingerprint"]
        assert new_fp != old_fp
        preconditions = {
            p["subject"]: p["compatibility"]
            for p in replay.json()["plan"]["preconditions"]
        }
        assert preconditions == {"orders": "BACKWARD_TRANSITIVE", "payments": "BACKWARD"}

        # old candidate is rejected: only the compatibility term moved
        stale = client.post(f"/releases/{old_fp}/confirm")
        assert stale.status_code == 409
        (conflict,) = stale.json()["detail"]["conflicts"]
        assert conflict == {
            "subject": "orders",
            "kind": "compatibility_drift",
            "expected_compatibility": "BACKWARD",
            "actual_compatibility": "BACKWARD_TRANSITIVE",
        }

        # the new candidate is planned under the new mode and confirms
        assert client.post(f"/releases/{new_fp}/confirm").status_code == 200

    def test_replay_under_incompatible_new_mode_is_rejected(self, client):
        setup_orders_payments(client)
        old_fp = client.post(
            "/releases", json=orders_payments_candidate()
        ).json()["fingerprint"]

        # FORWARD: the old reader must read the candidate — widening
        # amount int -> long is illegal in that direction
        assert (
            client.put(
                "/subjects/orders/config", json={"compatibility": "FORWARD"}
            ).status_code
            == 200
        )
        replay = client.post("/releases", json=orders_payments_candidate())
        assert replay.status_code == 409
        (entry,) = replay.json()["detail"]["subjects"]
        assert entry["subject"] == "orders"
        assert entry["errors"][0]["rule"] == "primitive_type_mismatch"
        # no extra candidate was stored by the failed replay
        assert [r["fingerprint"] for r in client.get("/releases").json()] == [old_fp]


class TestValidationFailures:
    def test_unknown_subject_404(self, client):
        resp = client.post(
            "/releases", json={"subjects": {"ghost": {"content": SIMPLE_V2}}}
        )
        assert resp.status_code == 404

    def test_edge_endpoint_outside_release_422(self, client):
        create_subject(client, "orders")
        resp = client.post(
            "/releases",
            json={
                "subjects": {"orders": {"content": ORDER_V2}},
                "edges": [{"producer": "orders", "consumer": "payments"}],
            },
        )
        assert resp.status_code == 422

    def test_invalid_contract_422_names_the_subject(self, client):
        create_subject(client, "orders")
        bad = record("R", [{"id": 1, "name": "x", "type": "Ghost"}])
        resp = client.post(
            "/releases", json={"subjects": {"orders": {"content": bad}}}
        )
        assert resp.status_code == 422
        assert "orders" in resp.json()["detail"]

    def test_noop_subject_rejected(self, client):
        create_subject(client, "orders")
        register(client, "orders", ORDER_V1)
        resp = client.post(
            "/releases", json={"subjects": {"orders": {"content": ORDER_V1}}}
        )
        assert resp.status_code == 409
        (entry,) = resp.json()["detail"]["subjects"]
        assert entry["subject"] == "orders"
        assert entry["existing_version"] == 1

    def test_subject_policy_violation_409(self, client):
        setup_orders_payments(client)
        bad_order = record(
            "Order",
            ORDER_V2["fields"] + [{"id": 9, "name": "region", "type": "string"}],
        )
        body = orders_payments_candidate()
        body["subjects"]["orders"] = {"content": bad_order}
        resp = client.post("/releases", json=body)
        assert resp.status_code == 409
        (entry,) = resp.json()["detail"]["subjects"]
        assert entry["subject"] == "orders"
        assert entry["errors"][0]["rule"] == "required_field_missing_default"
        assert client.get("/releases").json() == []  # nothing stored

    def test_incompatible_edge_reports_versions_and_rule(self, client):
        setup_orders_payments(client)
        resp = client.post("/releases", json=orders_payments_candidate(PAYMENT_V2_BAD))
        assert resp.status_code == 409
        (edge,) = resp.json()["detail"]["edges"]
        assert edge["producer"] == "orders"
        assert edge["consumer"] == "payments"
        assert edge["producer_version"] == 2
        assert edge["consumer_version"] == 2
        assert edge["rule"] == "primitive_type_mismatch"
        assert edge["path"] == ["currency"]
        assert edge["path_string"] == "$.currency"
        assert client.get("/releases").json() == []

    def test_cycle_reports_shortest_cycle(self, client):
        for name in "xyzw":
            create_subject(client, name)
            register(client, name, SIMPLE_V1)
        body = {
            "subjects": {name: {"content": SIMPLE_V2} for name in "xyzw"},
            "edges": [
                {"producer": "x", "consumer": "y"},
                {"producer": "y", "consumer": "x"},  # 2-cycle: x -> y -> x
                {"producer": "y", "consumer": "z"},
                {"producer": "z", "consumer": "w"},
                {"producer": "w", "consumer": "y"},  # 3-cycle: y -> z -> w -> y
            ],
        }
        resp = client.post("/releases", json=body)
        assert resp.status_code == 409
        detail = resp.json()["detail"]
        assert detail["rule"] == "cyclic_dependency"
        assert detail["cycle"] == [
            {"subject": "x", "target_version": 2},
            {"subject": "y", "target_version": 2},
            {"subject": "x", "target_version": 2},
        ]

    def test_self_loop_is_a_cycle(self, client):
        create_subject(client, "s")
        register(client, "s", SIMPLE_V1)
        resp = client.post(
            "/releases",
            json={
                "subjects": {"s": {"content": SIMPLE_V2}},
                "edges": [{"producer": "s", "consumer": "s"}],
            },
        )
        assert resp.status_code == 409
        cycle = resp.json()["detail"]["cycle"]
        assert [node["subject"] for node in cycle] == ["s", "s"]


class TestConfirm:
    def create_candidate(self, client) -> str:
        resp = client.post("/releases", json=orders_payments_candidate())
        assert resp.status_code == 201, resp.text
        return resp.json()["fingerprint"]

    def test_confirm_commits_all_subjects_atomically(self, client):
        setup_orders_payments(client)
        fp = self.create_candidate(client)
        plan = client.get(f"/releases/{fp}").json()["plan"]

        resp = client.post(f"/releases/{fp}/confirm")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["status"] == "committed"
        assert body["versions"] == [
            {
                "subject": s["subject"],
                "version": s["target_version"],
                "fingerprint": s["fingerprint"],
            }
            for s in plan["subjects"]
        ]
        # the regular single-subject API sees the published versions
        latest = client.get("/subjects/orders/versions/latest").json()
        assert latest["version"] == 2
        assert latest["content"] == ORDER_V2
        assert client.get("/subjects/payments/versions/latest").json()["version"] == 2
        detail = client.get(f"/releases/{fp}").json()
        assert detail["status"] == "committed"
        assert detail["committed_at"] is not None

    def test_confirm_is_idempotent(self, client):
        setup_orders_payments(client)
        fp = self.create_candidate(client)
        first = client.post(f"/releases/{fp}/confirm")
        again = client.post(f"/releases/{fp}/confirm")
        assert again.status_code == 200
        assert again.json() == first.json()
        # no extra versions were written by the replay
        assert [v["version"] for v in client.get("/subjects/orders/versions").json()] == [1, 2]

    def test_confirm_rejects_when_any_subject_moved(self, client):
        setup_orders_payments(client)
        fp = self.create_candidate(client)
        # someone publishes orders v2 through the single-subject API
        register(client, "orders", ORDER_V2)

        resp = client.post(f"/releases/{fp}/confirm")
        assert resp.status_code == 409
        (conflict,) = resp.json()["detail"]["conflicts"]
        assert conflict == {
            "subject": "orders",
            "kind": "version_drift",
            "expected_baseline": 1,
            "actual_latest": 2,
        }
        # nothing was written: payments is untouched, the release stays planned
        assert [v["version"] for v in client.get("/subjects/payments/versions").json()] == [1]
        assert client.get(f"/releases/{fp}").json()["status"] == "planned"


@pytest.fixture()
def live_server():
    config = uvicorn.Config(app, host="127.0.0.1", port=8174, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(100):
        try:
            httpx.get("http://127.0.0.1:8174/health", timeout=0.2)
            break
        except Exception:
            time.sleep(0.05)
    yield "http://127.0.0.1:8174"
    server.should_exit = True
    thread.join(timeout=5)


def synchronise_latest_versions(monkeypatch):
    """Barrier the first two baseline reads so both confirms plan on the
    same snapshot before either commits."""
    barrier = threading.Barrier(2)
    calls = itertools.count()
    original = release_service._latest_versions

    def synchronised(db, subject_ids):
        if next(calls) < 2:
            barrier.wait(timeout=10)
        return original(db, subject_ids)

    monkeypatch.setattr(release_service, "_latest_versions", synchronised)


def run_concurrently(*tasks):
    threads = [threading.Thread(target=t) for t in tasks]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


class TestConfirmConcurrency:
    def test_same_candidate_confirmed_concurrently_converges(
        self, live_server, monkeypatch
    ):
        base = live_server
        assert httpx.post(f"{base}/subjects", json={"name": "rcx"}).status_code == 201
        assert (
            httpx.post(f"{base}/subjects/rcx/versions", json={"content": SIMPLE_V1}).status_code
            == 201
        )
        fp = httpx.post(
            f"{base}/releases",
            json={"subjects": {"rcx": {"content": SIMPLE_V2}}, "edges": []},
        ).json()["fingerprint"]

        synchronise_latest_versions(monkeypatch)
        results: list[int] = []

        def confirm():
            resp = httpx.post(f"{base}/releases/{fp}/confirm", timeout=15)
            results.append(resp.status_code)

        run_concurrently(confirm, confirm)

        assert sorted(results) == [200, 200]  # idempotent converge, one write
        versions = httpx.get(f"{base}/subjects/rcx/versions").json()
        assert [v["version"] for v in versions] == [1, 2]

    def test_conflicting_releases_exactly_one_commits(self, live_server, monkeypatch):
        base = live_server
        for name in ("ra", "rb"):
            assert httpx.post(f"{base}/subjects", json={"name": name}).status_code == 201
            assert (
                httpx.post(f"{base}/subjects/{name}/versions", json={"content": SIMPLE_V1}).status_code
                == 201
            )

        def create(content):
            return httpx.post(
                f"{base}/releases",
                json={
                    "subjects": {
                        "ra": {"content": content},
                        "rb": {"content": content},
                    },
                    "edges": [],
                },
            ).json()["fingerprint"]

        fp_a = create(SIMPLE_V2)
        fp_b = create(ALT_V2)

        synchronise_latest_versions(monkeypatch)
        results: list[int] = []

        def confirm(fp):
            resp = httpx.post(f"{base}/releases/{fp}/confirm", timeout=15)
            results.append(resp.status_code)

        run_concurrently(lambda: confirm(fp_a), lambda: confirm(fp_b))

        assert sorted(results) == [200, 409]
        for name in ("ra", "rb"):
            versions = httpx.get(f"{base}/subjects/{name}/versions").json()
            assert [v["version"] for v in versions] == [1, 2]  # exactly one landed


def synchronise_history_reads(monkeypatch, on_both=None):
    """Park the first two release snapshot reads until both requests have
    observed the same history; ``on_both`` runs exactly once, after both
    reads but before either request is allowed to continue."""
    barrier = threading.Barrier(2)
    fired = threading.Event()
    calls = itertools.count()
    original = release_service._history_by_subject

    def synchronised(db, subject_ids):
        result = original(db, subject_ids)
        if next(calls) < 2:
            if not fired.is_set():
                fired.set()
                if on_both is not None:
                    on_both()
            barrier.wait(timeout=10)
        return result

    monkeypatch.setattr(release_service, "_history_by_subject", synchronised)


class TestCreateConcurrency:
    """Concurrent ``POST /releases`` must stay CAS-idempotent even though
    candidate identity now includes the history snapshot."""

    def test_identical_candidate_created_concurrently_converges(
        self, live_server, monkeypatch
    ):
        base = live_server
        for name in ("pa", "pb"):
            assert httpx.post(f"{base}/subjects", json={"name": name}).status_code == 201
            assert (
                httpx.post(
                    f"{base}/subjects/{name}/versions", json={"content": SIMPLE_V1}
                ).status_code
                == 201
            )
        synchronise_history_reads(monkeypatch)

        body = {
            "subjects": {
                "pa": {"content": SIMPLE_V2},
                "pb": {"content": SIMPLE_V2},
            },
            "edges": [],
        }
        responses: list[httpx.Response] = []

        def post():
            responses.append(httpx.post(f"{base}/releases", json=body, timeout=15))

        run_concurrently(post, post)

        assert sorted(r.status_code for r in responses) == [200, 201]
        fps = {r.json()["fingerprint"] for r in responses}
        assert len(fps) == 1  # both converge on the single stored candidate
        (fp,) = fps
        assert [r["fingerprint"] for r in httpx.get(f"{base}/releases").json()] == [fp]

    def test_create_racing_an_out_of_band_registration_replans(
        self, live_server, monkeypatch
    ):
        """Two identical creates read baseline v1; before either inserts,
        another request registers v2. Both responses must be confirmable
        candidates: the loser re-validates against the new snapshot instead
        of returning a stale candidate bound to v1."""
        base = live_server
        for name in ("qa", "qb"):
            assert httpx.post(f"{base}/subjects", json={"name": name}).status_code == 201
            assert (
                httpx.post(
                    f"{base}/subjects/{name}/versions", json={"content": ORDER_V1}
                ).status_code
                == 201
            )

        # qa advances out of band while both releases are parked after
        # their snapshot reads; gate2 blocks release inserts until that
        # registration has committed, making the race deterministic.
        released = threading.Event()
        registration_done = threading.Event()

        def register_mid():
            released.wait(timeout=10)
            resp = httpx.post(
                f"{base}/subjects/qa/versions",
                json={"content": ORDER_MID},
                timeout=15,
            )
            assert resp.status_code == 201, resp.text
            registration_done.set()

        synchronise_history_reads(monkeypatch, on_both=released.set)
        original_build_plan = release_service._build_plan
        build_calls = itertools.count()

        def gated_build_plan(*args, **kwargs):
            # both releases validate fully, then wait here (post snapshot,
            # pre insert) until the out-of-band registration has landed
            if next(build_calls) < 2:
                registration_done.wait(timeout=10)
            return original_build_plan(*args, **kwargs)

        monkeypatch.setattr(release_service, "_build_plan", gated_build_plan)

        body = {
            "subjects": {
                "qa": {"content": ORDER_V2},
                "qb": {"content": ORDER_V2},
            },
            "edges": [{"producer": "qa", "consumer": "qb"}],
        }
        responses: list[httpx.Response] = []

        def post():
            responses.append(httpx.post(f"{base}/releases", json=body, timeout=15))

        run_concurrently(post, post, register_mid)

        assert sorted(r.status_code for r in responses) == [201, 201], [
            r.text for r in responses
        ]
        fps = [r.json()["fingerprint"] for r in responses]
        assert len(set(fps)) == 2  # one stale-at-birth, one replanned
        stale, fresh = sorted(
            (r.json() for r in responses),
            key=lambda b: next(
                s["baseline_version"]
                for s in b["plan"]["subjects"]
                if s["subject"] == "qa"
            ),
        )
        # the replanned candidate is bound to qa v2 and targets v3
        qa_fresh = next(s for s in fresh["plan"]["subjects"] if s["subject"] == "qa")
        assert qa_fresh["baseline_version"] == 2
        assert qa_fresh["target_version"] == 3
        # the candidate built on the old snapshot remains stored/queryable...
        qa_stale = next(s for s in stale["plan"]["subjects"] if s["subject"] == "qa")
        assert qa_stale["baseline_version"] == 1
        assert httpx.get(f"{base}/releases/{stale['fingerprint']}").status_code == 200
        # ...but can only fail the global CAS (that is the documented escape)
        assert (
            httpx.post(
                f"{base}/releases/{stale['fingerprint']}/confirm", timeout=15
            ).status_code
            == 409
        )
        # and the replanned candidate commits cleanly
        committed = httpx.post(
            f"{base}/releases/{fresh['fingerprint']}/confirm", timeout=15
        )
        assert committed.status_code == 200, committed.text
        versions = {v["subject"]: v["version"] for v in committed.json()["versions"]}
        assert versions == {"qa": 3, "qb": 2}

