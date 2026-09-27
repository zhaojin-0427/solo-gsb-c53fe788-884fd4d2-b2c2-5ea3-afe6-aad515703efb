"""Event subscriptions: emission, replay, checkpoints and concurrency."""
from __future__ import annotations

import threading

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient

from app import service
from app.db import SessionLocal, init_db
from app.main import app
from app.models import (
    ConsumerBinding,
    Event,
    EventCounter,
    EventSubscription,
    ImpactReport,
    Migration,
    Release,
    Subject,
    Version,
)


def rec(name, *fields):
    return {
        "type": "record",
        "name": name,
        "fields": [
            {"id": i, "type": spec, "name": f"f{i}"}
            if isinstance(spec, str)
            else {"id": i, **spec}
            for i, spec in enumerate(fields, 1)
        ],
    }


V1 = rec("R", "int")
V2 = rec("R", "long", {"name": "note", "type": "string", "default": ""})
V3 = rec("R", "long", {"name": "note", "type": "string", "default": ""},
         {"name": "tag", "type": "int", "default": 0})
# incompatible with V1/V2 under BACKWARD: required field without default
V_BAD = rec("R", "long", {"name": "owner", "type": "string"})

ORDER_V1 = rec("Order", "int")
ORDER_V2 = rec("Order", "long", {"name": "cur", "type": "string", "default": "USD"})
PAYMENT_V1 = rec("Payment", "int")
PAYMENT_V2 = rec("Payment", "long", {"name": "cur", "type": "string", "default": "USD"})


@pytest.fixture()
def client():
    with TestClient(app) as c:
        yield c


def _clean():
    # init_db() guarantees the schema exists even before any TestClient
    # lifespan ran (the very first test of the session)
    init_db()
    db = SessionLocal()
    db.query(ImpactReport).delete()
    db.query(ConsumerBinding).delete()
    db.query(Release).delete()
    db.query(Migration).delete()
    db.query(Version).delete()
    db.query(Subject).delete()
    db.query(Event).delete()
    db.query(EventSubscription).delete()
    db.query(EventCounter).delete()
    db.add(EventCounter(id=1, value=0))
    db.commit()
    db.close()


@pytest.fixture(autouse=True)
def clean_db():
    _clean()
    yield
    _clean()


def create_subject(client, name, mode="BACKWARD"):
    resp = client.post("/subjects", json={"name": name, "compatibility": mode})
    assert resp.status_code == 201, resp.text


def register(client, name, content, expected=201):
    resp = client.post(f"/subjects/{name}/versions", json={"content": content})
    assert resp.status_code == expected, resp.text
    return resp


def subscribe(client, subjects=None, event_types=None):
    body = {}
    if subjects is not None:
        body["subjects"] = subjects
    if event_types is not None:
        body["event_types"] = event_types
    resp = client.post("/event-subscriptions", json=body)
    assert resp.status_code == 201, resp.text
    return resp.json()["subscription_id"]


def read_page(client, sid, **params):
    resp = client.get(f"/event-subscriptions/{sid}/events", params=params)
    assert resp.status_code == 200, resp.text
    return resp.json()


def read_all(client, sid, limit=100):
    """Exhaust the cursor pagination and return every event once."""
    events, after = [], None
    while True:
        params = {"limit": limit}
        if after is not None:
            params["after_seq"] = after
        page = read_page(client, sid, **params)
        events.extend(page["events"])
        after = page["next_after_seq"]
        if not page["has_more"]:
            return events


class TestSubscriptions:
    def test_create_defaults_and_get(self, client):
        resp = client.post("/event-subscriptions", json={})
        assert resp.status_code == 201
        body = resp.json()
        assert body["subjects"] == []
        assert body["event_types"] == []
        assert body["checkpoint"] == 0
        got = client.get(f"/event-subscriptions/{body['subscription_id']}")
        assert got.status_code == 200
        assert got.json() == body

    def test_create_with_filters(self, client):
        resp = client.post(
            "/event-subscriptions",
            json={
                "subjects": ["users", "orders"],
                "event_types": ["version_registered", "compatibility_changed"],
            },
        )
        assert resp.status_code == 201
        body = resp.json()
        assert body["subjects"] == ["users", "orders"]
        assert body["event_types"] == ["version_registered", "compatibility_changed"]

    def test_unknown_event_type_rejected(self, client):
        resp = client.post(
            "/event-subscriptions", json={"event_types": ["not_a_type"]}
        )
        assert resp.status_code == 422

    def test_unknown_subscription_404(self, client):
        assert client.get("/event-subscriptions/nope").status_code == 404
        assert client.get("/event-subscriptions/nope/events").status_code == 404
        resp = client.post("/event-subscriptions/nope/checkpoint", json={"seq": 0})
        assert resp.status_code == 404


class TestEmission:
    def test_version_registered_and_cas_hit(self, client):
        create_subject(client, "users")
        register(client, "users", V1)
        again = register(client, "users", V1, expected=200)  # CAS hit
        assert again.json()["created"] is False
        register(client, "users", V2)

        events = read_all(client, subscribe(client))
        assert [e["seq"] for e in events] == [1, 2]  # CAS hit emitted nothing
        first = events[0]
        assert first["type"] == "version_registered"
        assert first["subject"] == "users"
        assert first["version"] == 1
        assert first["fingerprint"] == events[0]["payload"]["fingerprint"]
        assert first["payload"]["subject"] == "users"
        assert first["payload"]["compatibility"] == "BACKWARD"
        assert "canonical" in first["payload"]
        assert events[1]["version"] == 2

    def test_config_change_only_when_changed(self, client):
        create_subject(client, "users")
        resp = client.put(
            "/subjects/users/config", json={"compatibility": "FULL"}
        )
        assert resp.status_code == 200
        # no-op write: same mode again -> no event
        client.put("/subjects/users/config", json={"compatibility": "FULL"})

        events = read_all(client, subscribe(client))
        assert len(events) == 1
        (event,) = events
        assert event["type"] == "compatibility_changed"
        assert event["subject"] == "users"
        assert event["version"] is None
        assert event["fingerprint"] is None
        assert event["payload"] == {
            "subject": "users",
            "from_compatibility": "BACKWARD",
            "to_compatibility": "FULL",
        }

    def test_release_commit_fans_out_per_subject(self, client):
        create_subject(client, "orders")
        create_subject(client, "payments")
        register(client, "orders", ORDER_V1)
        register(client, "payments", PAYMENT_V1)
        resp = client.post(
            "/releases",
            json={
                "subjects": {
                    "orders": {"content": ORDER_V2},
                    "payments": {"content": PAYMENT_V2},
                },
                "edges": [{"producer": "orders", "consumer": "payments"}],
            },
        )
        assert resp.status_code == 201, resp.text
        fp = resp.json()["fingerprint"]
        # candidate creation itself emitted nothing
        assert read_all(client, subscribe(client))[-1]["seq"] == 2

        resp = client.post(f"/releases/{fp}/confirm")
        assert resp.status_code == 200, resp.text

        events = read_all(client, subscribe(client))
        release_events = [e for e in events if e["type"] == "release_committed"]
        assert len(release_events) == 2  # one per participating subject
        assert [e["seq"] for e in release_events] == [3, 4]  # consecutive
        by_subject = {e["subject"]: e for e in release_events}
        assert set(by_subject) == {"orders", "payments"}
        for event in release_events:
            assert event["fingerprint"] == fp
            assert event["payload"]["release"] == fp
            assert len(event["payload"]["versions"]) == 2
        assert by_subject["orders"]["version"] == 2
        assert by_subject["payments"]["version"] == 2
        # idempotent re-confirm emits nothing more
        client.post(f"/releases/{fp}/confirm")
        assert len(read_all(client, subscribe(client))) == 4

    def test_stale_release_confirm_emits_nothing(self, client):
        create_subject(client, "orders")
        create_subject(client, "payments")
        register(client, "orders", ORDER_V1)
        register(client, "payments", PAYMENT_V1)
        resp = client.post(
            "/releases",
            json={
                "subjects": {
                    "orders": {"content": ORDER_V2},
                    "payments": {"content": PAYMENT_V2},
                },
                "edges": [],
            },
        )
        fp = resp.json()["fingerprint"]
        register(client, "orders", ORDER_V2)  # drift the baseline
        resp = client.post(f"/releases/{fp}/confirm")
        assert resp.status_code == 409

        events = read_all(client, subscribe(client))
        assert [e["type"] for e in events] == ["version_registered"] * 3

    def test_migration_commit_and_blocked_candidate(self, client):
        create_subject(client, "users")
        register(client, "users", rec("R", "int"))
        register(client, "users", rec("R", "int", {"name": "b", "type": "int", "default": 0}))
        register(client, "users", rec("R", "int", {"name": "b", "type": "int"}))

        # BACKWARD_TRANSITIVE replays v3 against v1 -> conflict -> blocked
        resp = client.post(
            "/migrations",
            json={"subjects": {"users": {"to_compatibility": "BACKWARD_TRANSITIVE"}}},
        )
        assert resp.status_code == 201 and resp.json()["status"] == "blocked"
        blocked_fp = resp.json()["fingerprint"]
        resp = client.post(f"/migrations/{blocked_fp}/confirm")
        assert resp.status_code == 409
        # neither the blocked candidate nor its failed confirm emitted events
        assert [e["type"] for e in read_all(client, subscribe(client))] == [
            "version_registered"
        ] * 3

        resp = client.post(
            "/migrations", json={"subjects": {"users": {"to_compatibility": "FULL"}}}
        )
        assert resp.json()["status"] == "planned"
        fp = resp.json()["fingerprint"]
        resp = client.post(f"/migrations/{fp}/confirm")
        assert resp.status_code == 200

        events = read_all(client, subscribe(client))
        (event,) = [e for e in events if e["type"] == "migration_committed"]
        assert event["subject"] == "users"
        assert event["fingerprint"] == fp
        assert event["payload"]["migration"] == fp
        assert event["payload"]["from_compatibility"] == "BACKWARD"
        assert event["payload"]["to_compatibility"] == "FULL"
        # idempotent re-confirm emits nothing more
        client.post(f"/migrations/{fp}/confirm")
        assert len(read_all(client, subscribe(client))) == 4

    def test_binding_and_impact_report(self, client):
        create_subject(client, "orders")
        register(client, "orders", ORDER_V1)
        body = {
            "producer_subject": "orders",
            "baseline_version": 1,
            "reader": rec("C", "int"),
            "idempotency_key": "svc-orders-v1",
        }
        resp = client.post("/consumer-bindings", json=body)
        assert resp.status_code == 201, resp.text
        binding_id = resp.json()["binding_id"]
        # idempotent replay stores nothing -> emits nothing
        resp = client.post("/consumer-bindings", json=body)
        assert resp.status_code == 200

        events = read_all(client, subscribe(client))
        (event,) = [e for e in events if e["type"] == "consumer_binding_created"]
        assert event["subject"] == "orders"
        assert event["version"] == 1
        assert event["fingerprint"] == binding_id
        assert event["payload"]["binding_id"] == binding_id
        assert event["payload"]["idempotency_key"] == "svc-orders-v1"

        # first evaluation stores revision 1 -> one impact event
        resp = client.get(f"/consumer-bindings/{binding_id}/impact")
        assert resp.status_code == 200 and resp.json()["created"] is True
        # snapshot unchanged -> reuse, no new revision, no new event
        resp = client.get(f"/consumer-bindings/{binding_id}/impact")
        assert resp.json()["created"] is False
        events = read_all(client, subscribe(client))
        impacts = [e for e in events if e["type"] == "impact_report_created"]
        assert len(impacts) == 1
        assert impacts[0]["subject"] == "orders"
        assert impacts[0]["payload"]["revision"] == 1
        assert impacts[0]["payload"]["status"] == "active"

        # producer drifts -> next evaluation stores revision 2 -> new event
        register(client, "orders", rec("Order", "long"))
        resp = client.get(f"/consumer-bindings/{binding_id}/impact")
        assert resp.json()["created"] is True
        assert resp.json()["status"] == "impacted"
        events = read_all(client, subscribe(client))
        impacts = [e for e in events if e["type"] == "impact_report_created"]
        assert len(impacts) == 2
        assert impacts[1]["payload"]["revision"] == 2
        assert impacts[1]["payload"]["status"] == "impacted"
        assert impacts[1]["payload"]["result"]["first_impacted_version"] == 2


class TestRollbackLeavesNoEvents:
    def test_incompatible_registration(self, client):
        create_subject(client, "users")
        register(client, "users", V1)
        register(client, "users", V_BAD, expected=409)
        events = read_all(client, subscribe(client))
        assert [e["seq"] for e in events] == [1]

    def test_invalid_schema(self, client):
        create_subject(client, "users")
        register(client, "users", {"type": "record", "name": "R"}, expected=422)
        assert read_all(client, subscribe(client)) == []

    def test_failed_binding_validation(self, client):
        create_subject(client, "orders")
        register(client, "orders", rec("Order", "long"))
        # reader (int) cannot read baseline (long) -> 409, nothing stored
        resp = client.post(
            "/consumer-bindings",
            json={
                "producer_subject": "orders",
                "baseline_version": 1,
                "reader": rec("C", "int"),
                "idempotency_key": "bad-reader",
            },
        )
        assert resp.status_code == 409
        events = read_all(client, subscribe(client))
        assert [e["type"] for e in events] == ["version_registered"]


class TestSubjectDeletionPreservesHistory:
    def test_delete_and_recreate_keeps_events(self, client):
        create_subject(client, "users")
        register(client, "users", V1)
        client.put("/subjects/users/config", json={"compatibility": "FULL"})
        assert client.delete("/subjects/users").status_code == 204

        sid = subscribe(client, subjects=["users"])
        before = read_all(client, sid)
        assert [e["type"] for e in before] == [
            "version_registered",
            "compatibility_changed",
        ]

        # recreate under the same name: history is preserved and the new
        # life simply continues the same global sequence
        create_subject(client, "users")
        register(client, "users", V2)
        after = read_all(client, sid)
        assert [e["seq"] for e in after] == [1, 2, 3]
        assert after[-1]["type"] == "version_registered"
        assert after[-1]["version"] == 1  # version numbering restarted...


class TestPaginationAndSnapshot:
    def test_cursor_pagination_covers_every_event_once(self, client):
        for i in range(5):
            create_subject(client, f"s{i}")
            register(client, f"s{i}", V1)
        sid = subscribe(client)
        pages, after = [], None
        while True:
            params = {"limit": 2}
            if after is not None:
                params["after_seq"] = after
            page = read_page(client, sid, **params)
            pages.append(page)
            after = page["next_after_seq"]
            if not page["has_more"]:
                break
        assert [len(p["events"]) for p in pages] == [2, 2, 1]
        assert [p["has_more"] for p in pages] == [True, True, False]
        seqs = [e["seq"] for p in pages for e in p["events"]]
        assert seqs == [1, 2, 3, 4, 5]  # no loss, no duplicates
        assert all(p["snapshot_seq"] == 5 for p in pages)

    def test_snapshot_cutoff_and_byte_identical_reads(self, client):
        create_subject(client, "users")
        register(client, "users", V1)
        sid = subscribe(client)
        first = client.get(f"/event-subscriptions/{sid}/events")
        assert first.json()["snapshot_seq"] == 1

        # more writes move the committed snapshot...
        register(client, "users", V2)
        client.put("/subjects/users/config", json={"compatibility": "FULL"})

        # ...but re-reading the same pinned range is byte-identical
        pinned = client.get(
            f"/event-subscriptions/{sid}/events",
            params={"upto_seq": first.json()["snapshot_seq"]},
        )
        assert pinned.content == first.content
        # and so is any repeated read of the same range
        again = client.get(f"/event-subscriptions/{sid}/events")
        assert client.get(f"/event-subscriptions/{sid}/events").content == again.content
        assert again.json()["snapshot_seq"] == 3
        assert [e["seq"] for e in again.json()["events"]] == [1, 2, 3]

    def test_paginating_a_fixed_snapshot_is_byte_identical(self, client):
        for i in range(4):
            create_subject(client, f"s{i}")
            register(client, f"s{i}", V1)
        sid = subscribe(client)
        page1 = client.get(f"/event-subscriptions/{sid}/events", params={"limit": 2})
        snapshot = page1.json()["snapshot_seq"]
        page2 = client.get(
            f"/event-subscriptions/{sid}/events",
            params={"limit": 2, "after_seq": page1.json()["next_after_seq"]},
        )
        # new commits do not perturb re-reads of the pinned snapshot
        create_subject(client, "late")
        register(client, "late", V1)
        re1 = client.get(
            f"/event-subscriptions/{sid}/events",
            params={"limit": 2, "upto_seq": snapshot},
        )
        re2 = client.get(
            f"/event-subscriptions/{sid}/events",
            params={
                "limit": 2,
                "after_seq": page1.json()["next_after_seq"],
                "upto_seq": snapshot,
            },
        )
        assert re1.content == page1.content
        assert re2.content == page2.content
        seqs = [e["seq"] for e in page1.json()["events"] + page2.json()["events"]]
        assert seqs == [1, 2, 3, 4]


class TestCheckpoint:
    def test_resume_from_checkpoint_and_rewind(self, client):
        for i in range(3):
            create_subject(client, f"s{i}")
            register(client, f"s{i}", V1)
        sid = subscribe(client)

        page = read_page(client, sid, limit=2)
        assert [e["seq"] for e in page["events"]] == [1, 2]
        resp = client.post(
            f"/event-subscriptions/{sid}/checkpoint",
            json={"seq": page["next_after_seq"]},
        )
        assert resp.status_code == 200 and resp.json()["checkpoint"] == 2

        # no explicit cursor -> the stored checkpoint drives the read
        page = read_page(client, sid)
        assert page["after_seq"] == 2
        assert [e["seq"] for e in page["events"]] == [3]

        # rewind to 0 replays the whole log (local-index rebuild)
        client.post(f"/event-subscriptions/{sid}/checkpoint", json={"seq": 0})
        page = read_page(client, sid)
        assert [e["seq"] for e in page["events"]] == [1, 2, 3]

    def test_checkpoint_beyond_snapshot_rejected(self, client):
        create_subject(client, "users")
        register(client, "users", V1)
        sid = subscribe(client)
        resp = client.post(f"/event-subscriptions/{sid}/checkpoint", json={"seq": 2})
        assert resp.status_code == 409
        detail = resp.json()["detail"]
        assert detail["kind"] == "checkpoint_beyond_snapshot"
        assert detail["snapshot_seq"] == 1
        # exactly at the snapshot is fine
        resp = client.post(f"/event-subscriptions/{sid}/checkpoint", json={"seq": 1})
        assert resp.status_code == 200
        # negative is a validation error
        resp = client.post(f"/event-subscriptions/{sid}/checkpoint", json={"seq": -1})
        assert resp.status_code == 422

    def test_rebuild_local_index_from_replay(self, client):
        create_subject(client, "users")
        register(client, "users", V1)
        register(client, "users", V2)
        client.put("/subjects/users/config", json={"compatibility": "FULL"})
        create_subject(client, "orders")
        create_subject(client, "payments")
        register(client, "orders", ORDER_V1)
        register(client, "payments", PAYMENT_V1)
        resp = client.post(
            "/releases",
            json={
                "subjects": {
                    "orders": {"content": ORDER_V2},
                    "payments": {"content": PAYMENT_V2},
                },
                "edges": [{"producer": "orders", "consumer": "payments"}],
            },
        )
        client.post(f"/releases/{resp.json()['fingerprint']}/confirm")
        resp = client.post(
            "/migrations",
            json={"subjects": {"users": {"to_compatibility": "BACKWARD_TRANSITIVE"}}},
        )
        assert resp.json()["status"] == "planned"
        client.post(f"/migrations/{resp.json()['fingerprint']}/confirm")

        sid = subscribe(client)
        client.post(f"/event-subscriptions/{sid}/checkpoint", json={"seq": 0})
        versions, modes = {}, {}
        for event in read_all(client, sid, limit=3):
            payload = event["payload"]
            if event["type"] in ("version_registered", "release_committed"):
                versions[event["subject"]] = max(
                    versions.get(event["subject"], 0), event["version"]
                )
            if event["type"] == "version_registered":
                modes[event["subject"]] = payload["compatibility"]
            elif event["type"] == "compatibility_changed":
                modes[event["subject"]] = payload["to_compatibility"]
            elif event["type"] == "migration_committed":
                modes[event["subject"]] = payload["to_compatibility"]

        actual = {
            s["name"]: (s["latest_version"], s["compatibility"])
            for s in client.get("/subjects").json()
        }
        assert actual == {
            "users": (2, "BACKWARD_TRANSITIVE"),
            "orders": (2, "BACKWARD"),
            "payments": (2, "BACKWARD"),
        }
        assert versions == {name: v for name, (v, _) in actual.items()}
        assert modes == {name: m for name, (_, m) in actual.items()}


class TestFilters:
    def test_subject_and_type_filters(self, client):
        create_subject(client, "users")
        create_subject(client, "orders")
        register(client, "users", V1)
        register(client, "orders", ORDER_V1)
        client.put("/subjects/users/config", json={"compatibility": "FULL"})

        users_only = read_all(client, subscribe(client, subjects=["users"]))
        assert [e["type"] for e in users_only] == [
            "version_registered",
            "compatibility_changed",
        ]
        config_only = read_all(
            client, subscribe(client, event_types=["compatibility_changed"])
        )
        assert len(config_only) == 1 and config_only[0]["subject"] == "users"
        combined = read_all(
            client,
            subscribe(
                client, subjects=["orders"], event_types=["compatibility_changed"]
            ),
        )
        assert combined == []

    def test_subject_filter_matches_release_fanout(self, client):
        create_subject(client, "orders")
        create_subject(client, "payments")
        register(client, "orders", ORDER_V1)
        register(client, "payments", PAYMENT_V1)
        resp = client.post(
            "/releases",
            json={
                "subjects": {
                    "orders": {"content": ORDER_V2},
                    "payments": {"content": PAYMENT_V2},
                },
                "edges": [],
            },
        )
        client.post(f"/releases/{resp.json()['fingerprint']}/confirm")

        orders = read_all(client, subscribe(client, subjects=["orders"]))
        assert [e["type"] for e in orders] == [
            "version_registered",
            "release_committed",
        ]
        assert orders[-1]["version"] == 2


@pytest.fixture()
def live_server():
    config = uvicorn.Config(app, host="127.0.0.1", port=8179, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(100):
        try:
            httpx.get("http://127.0.0.1:8179/health", timeout=0.2)
            break
        except Exception:
            import time

            time.sleep(0.05)
    yield "http://127.0.0.1:8179"
    server.should_exit = True
    thread.join(timeout=5)


class TestConcurrentWrites:
    def test_no_loss_no_duplicates(self, live_server):
        base = live_server
        names = [f"c{i}" for i in range(8)]
        for name in names:
            assert httpx.post(f"{base}/subjects", json={"name": name}).status_code == 201

        barrier = threading.Barrier(len(names))
        results: list[int] = []

        def publish(name):
            barrier.wait(timeout=10)
            results.append(
                httpx.post(
                    f"{base}/subjects/{name}/versions",
                    json={"content": V1},
                    timeout=15,
                ).status_code
            )

        threads = [threading.Thread(target=publish, args=(n,)) for n in names]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert results == [201] * len(names)
        sid = httpx.post(f"{base}/event-subscriptions", json={}).json()[
            "subscription_id"
        ]
        page = httpx.get(f"{base}/event-subscriptions/{sid}/events").json()
        seqs = [e["seq"] for e in page["events"]]
        assert seqs == list(range(1, len(names) + 1))  # gapless, commit order
        assert sorted(e["subject"] for e in page["events"]) == sorted(names)

    def test_lost_race_leaves_no_event(self, live_server, monkeypatch):
        base = live_server
        assert httpx.post(f"{base}/subjects", json={"name": "race"}).status_code == 201
        assert (
            httpx.post(f"{base}/subjects/race/versions", json={"content": V1}).status_code
            == 201
        )

        barrier = threading.Barrier(2)
        original = service._next_version

        def synchronised_next_version(db, subject_id):
            barrier.wait(timeout=10)
            return original(db, subject_id)

        monkeypatch.setattr(service, "_next_version", synchronised_next_version)

        results: list[int] = []

        def publish(content):
            results.append(
                httpx.post(
                    f"{base}/subjects/race/versions",
                    json={"content": content},
                    timeout=15,
                ).status_code
            )

        t1 = threading.Thread(target=publish, args=(V2,))
        t2 = threading.Thread(target=publish, args=(V3,))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert sorted(results) == [201, 409]
        sid = httpx.post(f"{base}/event-subscriptions", json={}).json()[
            "subscription_id"
        ]
        page = httpx.get(f"{base}/event-subscriptions/{sid}/events").json()
        # v1 plus exactly one winner — the rolled-back loser left no event
        assert [e["seq"] for e in page["events"]] == [1, 2]
        assert [e["version"] for e in page["events"]] == [1, 2]
