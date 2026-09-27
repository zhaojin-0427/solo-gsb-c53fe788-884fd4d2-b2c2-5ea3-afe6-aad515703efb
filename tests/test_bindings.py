"""Consumer bindings: idempotent creation, drift-impact replay, CAS revisions."""
from __future__ import annotations

import threading

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient

from app import binding as binding_service
from app.db import SessionLocal
from app.main import app
from app.models import ConsumerBinding, ImpactReport, Subject, Version


def rec(name, *fields):
    return {
        "type": "record",
        "name": name,
        "fields": [
            {"id": i, "type": spec, "name": f"f{i}"} if isinstance(spec, str) else {"id": i, **spec}
            for i, spec in enumerate(fields, 1)
        ],
    }


# Producer history registrable under plain BACKWARD: int -> long widening,
# then an optional field with a default.
P_V1 = rec("P", "int")
P_V2 = rec("P", "long")
P_V3 = rec("P", "long", {"name": "note", "type": "string", "default": ""})

# Reader contracts (consumer side). READER_INT reads v1 only; READER_LONG
# reads every version above.
READER_INT = rec("C", "int")
READER_LONG = rec("C", "long")


@pytest.fixture()
def client():
    with TestClient(app) as c:
        yield c


@pytest.fixture(autouse=True)
def clean_db():
    yield
    db = SessionLocal()
    db.query(ImpactReport).delete()
    db.query(ConsumerBinding).delete()
    db.query(Version).delete()
    db.query(Subject).delete()
    db.commit()
    db.close()


def create_subject(client, name="orders", mode="BACKWARD"):
    resp = client.post("/subjects", json={"name": name, "compatibility": mode})
    assert resp.status_code == 201, resp.text


def register(client, name, content):
    resp = client.post(f"/subjects/{name}/versions", json={"content": content})
    assert resp.status_code == 201, resp.text
    return resp.json()


def bind(client, key="key-1", producer="orders", baseline=1, reader=READER_LONG):
    return client.post(
        "/consumer-bindings",
        json={
            "producer_subject": producer,
            "baseline_version": baseline,
            "reader": reader,
            "idempotency_key": key,
        },
    )


def setup_producer(client, name="orders", versions=(P_V1,)):
    create_subject(client, name)
    for content in versions:
        register(client, name, content)


class TestBindingCreation:
    def test_create_and_get(self, client):
        setup_producer(client)
        resp = bind(client)
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["created"] is True
        assert body["status"] == "active"
        assert body["producer_subject"] == "orders"
        assert body["baseline_version"] == 1
        assert body["bound_latest_version"] == 1
        assert len(body["binding_id"]) == 64
        assert len(body["baseline_fingerprint"]) == 64

        got = client.get(f"/consumer-bindings/{body['binding_id']}")
        assert got.status_code == 200
        detail = got.json()
        assert detail["binding_id"] == body["binding_id"]
        assert detail["idempotency_key"] == "key-1"
        assert detail["reader"] == READER_LONG
        assert detail["status"] == "active"
        assert detail["reports"] == 0
        assert detail["latest_revision"] == 0

    def test_get_unknown_binding_404(self, client):
        assert client.get(f"/consumer-bindings/{'0' * 64}").status_code == 404
        assert client.get(f"/consumer-bindings/{'0' * 64}/impact").status_code == 404

    def test_unknown_producer_404(self, client):
        resp = bind(client, producer="ghost")
        assert resp.status_code == 404

    def test_unknown_baseline_404(self, client):
        setup_producer(client)
        resp = bind(client, baseline=7)
        assert resp.status_code == 404

    def test_baseline_must_be_positive(self, client):
        setup_producer(client)
        assert bind(client, baseline=0).status_code == 422

    def test_invalid_reader_contract_422(self, client):
        setup_producer(client)
        resp = bind(client, reader={"type": "record", "name": "R", "fields": [
            {"id": 1, "name": "x", "type": "Ghost"}]})
        assert resp.status_code == 422

    def test_reader_incompatible_with_baseline_409(self, client):
        setup_producer(client)
        # reader requires a field the baseline writer never emits
        reader = rec("C", "int", {"name": "req", "type": "string"})
        resp = bind(client, reader=reader)
        assert resp.status_code == 409
        detail = resp.json()["detail"]
        assert detail["kind"] == "baseline_incompatible"
        (error,) = detail["errors"]
        assert error["version"] == 1
        assert error["rule"] == "required_field_missing_default"
        assert error["path"] == ["req"]
        assert error["path_string"] == "$.req"
        # nothing was stored
        assert client.get("/subjects/orders").status_code == 200

    def test_reader_narrower_than_baseline_409(self, client):
        setup_producer(client, versions=(P_V1, P_V2))
        # baseline v2 writes long; an int reader cannot read it
        resp = bind(client, baseline=2, reader=READER_INT)
        assert resp.status_code == 409
        assert resp.json()["detail"]["errors"][0]["rule"] == "primitive_type_mismatch"


class TestIdempotency:
    def test_same_key_same_content_reuses_binding(self, client):
        setup_producer(client)
        first = bind(client)
        assert first.status_code == 201
        replay = bind(client)
        assert replay.status_code == 200
        assert replay.json()["created"] is False
        assert replay.json()["binding_id"] == first.json()["binding_id"]

    def test_replay_survives_producer_drift(self, client):
        setup_producer(client)
        first = bind(client)
        register(client, "orders", P_V2)  # producer moved on
        replay = bind(client)
        assert replay.status_code == 200
        assert replay.json()["binding_id"] == first.json()["binding_id"]
        # the stored snapshot metadata is not rewritten
        assert replay.json()["bound_latest_version"] == 1

    def test_same_key_different_reader_conflicts(self, client):
        setup_producer(client)
        assert bind(client).status_code == 201
        resp = bind(client, reader=READER_INT)
        assert resp.status_code == 409
        detail = resp.json()["detail"]
        assert detail["kind"] == "idempotency_conflict"
        assert detail["idempotency_key"] == "key-1"

    def test_same_key_different_baseline_conflicts(self, client):
        setup_producer(client, versions=(P_V1, P_V2))
        assert bind(client, baseline=1).status_code == 201
        assert bind(client, baseline=2).status_code == 409

    def test_same_key_different_producer_conflicts(self, client):
        setup_producer(client)
        setup_producer(client, name="payments")
        assert bind(client, producer="orders").status_code == 201
        assert bind(client, producer="payments").status_code == 409

    def test_different_keys_create_distinct_bindings(self, client):
        setup_producer(client)
        one = bind(client, key="key-1")
        two = bind(client, key="key-2")
        assert one.status_code == 201 and two.status_code == 201
        assert one.json()["binding_id"] != two.json()["binding_id"]


class TestImpact:
    def test_first_report_no_drift(self, client):
        setup_producer(client)
        binding = bind(client).json()
        resp = client.get(f"/consumer-bindings/{binding['binding_id']}/impact")
        assert resp.status_code == 200
        body = resp.json()
        assert body["created"] is True
        assert body["revision"] == 1
        assert body["revisions"] == 1
        assert body["status"] == "active"
        assert body["producer_intact"] is True
        assert body["producer_latest_version"] == 1
        assert body["result"]["checked_versions"] == []
        assert body["result"]["affected_versions"] == []
        assert body["result"]["first_impacted_version"] is None

    def test_unchanged_snapshot_reuses_revision(self, client):
        setup_producer(client)
        binding = bind(client).json()
        first = client.get(f"/consumer-bindings/{binding['binding_id']}/impact").json()
        second = client.get(f"/consumer-bindings/{binding['binding_id']}/impact").json()
        assert second["created"] is False
        assert second["revision"] == first["revision"] == 1
        assert second["revisions"] == 1

    def test_compatible_drift_generates_new_revision(self, client):
        setup_producer(client)
        binding = bind(client).json()  # READER_LONG reads int and long
        assert client.get(f"/consumer-bindings/{binding['binding_id']}/impact").json()["revision"] == 1
        register(client, "orders", P_V2)
        register(client, "orders", P_V3)
        resp = client.get(f"/consumer-bindings/{binding['binding_id']}/impact")
        body = resp.json()
        assert body["created"] is True
        assert body["revision"] == 2
        assert body["revisions"] == 2
        assert body["status"] == "active"
        assert body["producer_latest_version"] == 3
        assert body["result"]["checked_versions"] == [2, 3]
        assert body["result"]["affected_versions"] == []

    def test_incompatible_drift_marks_binding_impacted(self, client):
        setup_producer(client)
        binding = bind(client, reader=READER_INT).json()
        register(client, "orders", P_V2)  # long: the int reader breaks
        resp = client.get(f"/consumer-bindings/{binding['binding_id']}/impact")
        body = resp.json()
        assert body["status"] == "impacted"
        assert body["result"]["first_impacted_version"] == 2
        (affected,) = body["result"]["affected_versions"]
        assert affected["version"] == 2
        assert affected["rule"] == "primitive_type_mismatch"
        assert affected["path"] == ["f1"]
        assert affected["path_string"] == "$.f1"
        # the binding itself now reports the impacted status
        detail = client.get(f"/consumer-bindings/{binding['binding_id']}").json()
        assert detail["status"] == "impacted"
        assert detail["latest_revision"] == 1

    def test_all_failing_versions_reported_in_order(self, client):
        setup_producer(client)
        binding = bind(client, reader=READER_INT).json()
        register(client, "orders", P_V2)
        register(client, "orders", P_V3)
        body = client.get(f"/consumer-bindings/{binding['binding_id']}/impact").json()
        result = body["result"]
        assert result["checked_versions"] == [2, 3]
        assert [a["version"] for a in result["affected_versions"]] == [2, 3]
        assert result["first_impacted_version"] == 2
        assert all(a["rule"] == "primitive_type_mismatch" for a in result["affected_versions"])

    def test_impacted_status_is_sticky(self, client):
        setup_producer(client)
        binding = bind(client, reader=READER_INT).json()
        register(client, "orders", P_V2)
        first = client.get(f"/consumer-bindings/{binding['binding_id']}/impact").json()
        assert first["status"] == "impacted"
        # a later compatible-for-some reader state does not un-impact:
        # replay always covers the whole post-baseline history
        register(client, "orders", P_V3)
        second = client.get(f"/consumer-bindings/{binding['binding_id']}/impact").json()
        assert second["status"] == "impacted"
        assert second["result"]["first_impacted_version"] == 2

    def test_revision_cas(self, client):
        setup_producer(client)
        binding = bind(client).json()
        url = f"/consumer-bindings/{binding['binding_id']}/impact"
        # expected_revision=0 matches "no report yet"
        assert client.get(url, params={"expected_revision": 0}).json()["revision"] == 1
        # stale expectation is rejected and stores nothing
        stale = client.get(url, params={"expected_revision": 0})
        assert stale.status_code == 409
        detail = stale.json()["detail"]
        assert detail["kind"] == "report_revision_conflict"
        assert detail["expected_revision"] == 0
        assert detail["actual_revision"] == 1
        # matching expectation on an unchanged snapshot reuses the revision
        reused = client.get(url, params={"expected_revision": 1}).json()
        assert reused["created"] is False
        # ...and on a moved snapshot generates the next one
        register(client, "orders", P_V2)
        moved = client.get(url, params={"expected_revision": 1}).json()
        assert moved["created"] is True
        assert moved["revision"] == 2
        assert client.get(url, params={"expected_revision": 1}).status_code == 409
        assert client.get(url, params={"expected_revision": 2}).status_code == 200


class TestSubjectLifecycle:
    def test_delete_subject_keeps_binding_and_reports(self, client):
        setup_producer(client)
        binding = bind(client, reader=READER_INT).json()
        register(client, "orders", P_V2)
        impacted = client.get(f"/consumer-bindings/{binding['binding_id']}/impact").json()
        assert impacted["status"] == "impacted"

        assert client.delete("/subjects/orders").status_code == 204

        detail = client.get(f"/consumer-bindings/{binding['binding_id']}")
        assert detail.status_code == 200
        assert detail.json()["status"] == "impacted"
        assert detail.json()["reports"] == 1

        # the stored report is served as the historical record; nothing new
        # is generated and nothing is rewritten
        resp = client.get(f"/consumer-bindings/{binding['binding_id']}/impact")
        assert resp.status_code == 200
        body = resp.json()
        assert body["created"] is False
        assert body["producer_intact"] is False
        assert body["revision"] == impacted["revision"]
        assert body["result"] == impacted["result"]

    def test_delete_subject_without_report_gives_409(self, client):
        setup_producer(client)
        binding = bind(client).json()
        assert client.delete("/subjects/orders").status_code == 204
        resp = client.get(f"/consumer-bindings/{binding['binding_id']}/impact")
        assert resp.status_code == 409
        assert resp.json()["detail"]["kind"] == "producer_missing"

    def test_config_change_does_not_rewrite_reports(self, client):
        setup_producer(client)
        binding = bind(client).json()
        first = client.get(f"/consumer-bindings/{binding['binding_id']}/impact").json()
        resp = client.put(
            "/subjects/orders/config", json={"compatibility": "FULL_TRANSITIVE"}
        )
        assert resp.status_code == 200
        second = client.get(f"/consumer-bindings/{binding['binding_id']}/impact").json()
        assert second["created"] is False
        assert second["revision"] == first["revision"]
        assert second["producer_intact"] is True

    def test_recreated_subject_with_replaced_history_keeps_old_report(self, client):
        setup_producer(client)
        binding = bind(client).json()
        first = client.get(f"/consumer-bindings/{binding['binding_id']}/impact").json()
        assert client.delete("/subjects/orders").status_code == 204
        # same name, but the new v1 carries different content
        create_subject(client, "orders")
        register(client, "orders", rec("P", "string"))
        resp = client.get(f"/consumer-bindings/{binding['binding_id']}/impact")
        assert resp.status_code == 200
        body = resp.json()
        assert body["producer_intact"] is False
        assert body["created"] is False
        assert body["revision"] == first["revision"]

    def test_idempotent_replay_after_subject_deletion(self, client):
        setup_producer(client)
        first = bind(client).json()
        assert client.delete("/subjects/orders").status_code == 204
        replay = bind(client)
        assert replay.status_code == 200
        assert replay.json()["binding_id"] == first["binding_id"]


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
            import time

            time.sleep(0.05)
    yield "http://127.0.0.1:8174"
    server.should_exit = True
    thread.join(timeout=5)


class TestConcurrency:
    def test_concurrent_same_key_creation_converges(self, live_server, monkeypatch):
        base = live_server
        assert httpx.post(f"{base}/subjects", json={"name": "orders"}).status_code == 201
        assert (
            httpx.post(f"{base}/subjects/orders/versions", json={"content": P_V1}).status_code
            == 201
        )
        payload = {
            "producer_subject": "orders",
            "baseline_version": 1,
            "reader": READER_LONG,
            "idempotency_key": "race-key",
        }

        # Force both requests past the idempotency lookup before either
        # commits: the unique idempotency-key constraint then decides.
        barrier = threading.Barrier(2)
        original = binding_service._binding_id

        def synchronised_binding_id(*args):
            barrier.wait(timeout=10)
            return original(*args)

        monkeypatch.setattr(binding_service, "_binding_id", synchronised_binding_id)

        results: list[tuple[int, str]] = []

        def create():
            resp = httpx.post(f"{base}/consumer-bindings", json=payload, timeout=15)
            results.append((resp.status_code, resp.json()["binding_id"]))

        t1 = threading.Thread(target=create)
        t2 = threading.Thread(target=create)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert sorted(code for code, _ in results) == [200, 201]
        assert len({binding_id for _, binding_id in results}) == 1

    def test_concurrent_impact_generation_converges(self, live_server, monkeypatch):
        base = live_server
        assert httpx.post(f"{base}/subjects", json={"name": "orders"}).status_code == 201
        assert (
            httpx.post(f"{base}/subjects/orders/versions", json={"content": P_V1}).status_code
            == 201
        )
        binding = httpx.post(
            f"{base}/consumer-bindings",
            json={
                "producer_subject": "orders",
                "baseline_version": 1,
                "reader": READER_LONG,
                "idempotency_key": "impact-race",
            },
        ).json()

        # Force both evaluations to compute their replay before either
        # commits: the unique (binding, revision) constraint then decides.
        barrier = threading.Barrier(2)
        original = binding_service._replay_reader

        def synchronised_replay(*args):
            barrier.wait(timeout=10)
            return original(*args)

        monkeypatch.setattr(binding_service, "_replay_reader", synchronised_replay)

        results: list[dict] = []

        def evaluate():
            resp = httpx.get(
                f"{base}/consumer-bindings/{binding['binding_id']}/impact", timeout=15
            )
            results.append(resp.json())

        t1 = threading.Thread(target=evaluate)
        t2 = threading.Thread(target=evaluate)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert {r["revision"] for r in results} == {1}
        assert sorted(r["created"] for r in results) == [False, True]

        db = SessionLocal()
        try:
            assert db.query(ImpactReport).count() == 1
        finally:
            db.close()
