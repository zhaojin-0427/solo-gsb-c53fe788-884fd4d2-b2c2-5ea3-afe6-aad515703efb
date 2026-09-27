"""End-to-end API tests against a SQLite-backed app instance."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.db import SessionLocal
from app.main import app
from app.models import Subject, Version

USER_V1 = {
    "type": "record",
    "name": "User",
    "fields": [
        {"id": 1, "name": "email", "type": "string"},
        {"id": 2, "name": "age", "type": "int"},
    ],
}

# age widened int -> long, new optional field with default: BACKWARD compatible
USER_V2 = {
    "type": "record",
    "name": "User",
    "fields": [
        {"id": 1, "name": "email", "type": "string"},
        {"id": 2, "name": "age", "type": "long"},
        {"id": 3, "name": "nickname", "type": "string", "default": ""},
    ],
}

# new required field without default: BACKWARD incompatible
USER_V3_BAD = {
    "type": "record",
    "name": "User",
    "fields": [
        {"id": 1, "name": "email", "type": "string"},
        {"id": 2, "name": "age", "type": "long"},
        {"id": 3, "name": "nickname", "type": "string", "default": ""},
        {"id": 4, "name": "address", "type": "string"},
    ],
}


@pytest.fixture()
def client():
    with TestClient(app) as c:
        yield c


@pytest.fixture(autouse=True)
def clean_db():
    yield
    db = SessionLocal()
    db.query(Version).delete()
    db.query(Subject).delete()
    db.commit()
    db.close()


def create_subject(client, name="users", mode="BACKWARD"):
    resp = client.post("/subjects", json={"name": name, "compatibility": mode})
    assert resp.status_code == 201, resp.text
    return resp


class TestSubjects:
    def test_create_and_get(self, client):
        create_subject(client)
        resp = client.get("/subjects/users")
        assert resp.status_code == 200
        body = resp.json()
        assert body["name"] == "users"
        assert body["compatibility"] == "BACKWARD"
        assert body["versions"] == []

    def test_duplicate_subject_conflict(self, client):
        create_subject(client)
        resp = client.post("/subjects", json={"name": "users"})
        assert resp.status_code == 409

    def test_missing_subject_404(self, client):
        assert client.get("/subjects/ghost").status_code == 404

    def test_list_and_config_update(self, client):
        create_subject(client)
        resp = client.put(
            "/subjects/users/config", json={"compatibility": "FULL_TRANSITIVE"}
        )
        assert resp.status_code == 200
        assert resp.json()["compatibility"] == "FULL_TRANSITIVE"
        listing = client.get("/subjects").json()
        assert listing[0]["name"] == "users"
        assert listing[0]["versions"] == 0


class TestRegistration:
    def test_first_version_always_compatible(self, client):
        create_subject(client)
        resp = client.post("/subjects/users/versions", json={"content": USER_V1})
        assert resp.status_code == 201
        body = resp.json()
        assert body["version"] == 1
        assert body["created"] is True
        assert len(body["fingerprint"]) == 64

    def test_cas_idempotent_republish(self, client):
        create_subject(client)
        client.post("/subjects/users/versions", json={"content": USER_V1})
        resp = client.post("/subjects/users/versions", json={"content": USER_V1})
        assert resp.status_code == 200
        body = resp.json()
        assert body["version"] == 1
        assert body["created"] is False
        # semantically identical document (field order shuffled) -> same CAS key
        shuffled = {
            "type": "record",
            "name": "User",
            "fields": [
                {"id": 2, "name": "age", "type": {"type": "int"}},
                {"id": 1, "name": "email", "type": "string"},
            ],
        }
        resp = client.post("/subjects/users/versions", json={"content": shuffled})
        assert resp.status_code == 200
        assert resp.json()["version"] == 1

    def test_backward_compatible_evolution(self, client):
        create_subject(client)
        client.post("/subjects/users/versions", json={"content": USER_V1})
        resp = client.post("/subjects/users/versions", json={"content": USER_V2})
        assert resp.status_code == 201
        assert resp.json()["version"] == 2

    def test_required_field_without_default_rejected(self, client):
        create_subject(client)
        client.post("/subjects/users/versions", json={"content": USER_V1})
        client.post("/subjects/users/versions", json={"content": USER_V2})
        resp = client.post("/subjects/users/versions", json={"content": USER_V3_BAD})
        assert resp.status_code == 409
        detail = resp.json()["detail"]
        (error,) = detail["errors"]
        assert error["version"] == 2  # the historical version checked
        assert error["rule"] == "required_field_missing_default"
        assert error["direction"] == "backward"
        assert error["path"] == ["address"]
        assert error["path_string"] == "$.address"

    def test_reader_ignores_extra_writer_fields(self, client):
        create_subject(client)
        client.post("/subjects/users/versions", json={"content": USER_V2})
        # v2 drops 'nickname': new reader simply ignores old writer's field
        reduced = {
            "type": "record",
            "name": "User",
            "fields": [
                {"id": 1, "name": "email", "type": "string"},
                {"id": 2, "name": "age", "type": "long"},
            ],
        }
        resp = client.post("/subjects/users/versions", json={"content": reduced})
        assert resp.status_code == 201

    def test_field_rename_aligned_by_id(self, client):
        create_subject(client)
        client.post("/subjects/users/versions", json={"content": USER_V1})
        renamed = {
            "type": "record",
            "name": "User",
            "fields": [
                {"id": 1, "name": "emailAddress", "type": "string"},
                {"id": 2, "name": "age", "type": "int"},
            ],
        }
        resp = client.post("/subjects/users/versions", json={"content": renamed})
        assert resp.status_code == 201

    def test_long_to_int_rejected(self, client):
        create_subject(client)
        client.post("/subjects/users/versions", json={"content": USER_V2})
        narrowed = {
            "type": "record",
            "name": "User",
            "fields": [
                {"id": 1, "name": "email", "type": "string"},
                {"id": 2, "name": "age", "type": "int"},
                {"id": 3, "name": "nickname", "type": "string", "default": ""},
            ],
        }
        resp = client.post("/subjects/users/versions", json={"content": narrowed})
        assert resp.status_code == 409
        error = resp.json()["detail"]["errors"][0]
        assert error["rule"] == "primitive_type_mismatch"
        assert error["path"] == ["age"]

    def test_invalid_schema_422(self, client):
        create_subject(client)
        bad = {
            "type": "record",
            "name": "R",
            "fields": [{"id": 1, "name": "x", "type": "Ghost"}],
        }
        resp = client.post("/subjects/users/versions", json={"content": bad})
        assert resp.status_code == 422

    def test_versions_listing_and_fetch(self, client):
        create_subject(client)
        client.post("/subjects/users/versions", json={"content": USER_V1})
        client.post("/subjects/users/versions", json={"content": USER_V2})
        versions = client.get("/subjects/users/versions").json()
        assert [v["version"] for v in versions] == [1, 2]
        latest = client.get("/subjects/users/versions/latest").json()
        assert latest["version"] == 2
        assert latest["content"]["fields"][2]["name"] == "nickname"
        assert "canonical" in latest
        first = client.get("/subjects/users/versions/1").json()
        assert first["fingerprint"] != latest["fingerprint"]
        assert client.get("/subjects/users/versions/99").status_code == 404


class TestEnumUnionContainers:
    def test_enum_must_cover_writer_symbols(self, client):
        create_subject(client)
        v1 = {"type": "enum", "name": "Color", "symbols": ["RED", "GREEN"]}
        v2_ok = {"type": "enum", "name": "Color", "symbols": ["RED", "GREEN", "BLUE"]}
        v3_bad = {"type": "enum", "name": "Color", "symbols": ["RED"]}
        assert client.post("/subjects/users/versions", json={"content": v1}).status_code == 201
        assert client.post("/subjects/users/versions", json={"content": v2_ok}).status_code == 201
        resp = client.post("/subjects/users/versions", json={"content": v3_bad})
        assert resp.status_code == 409
        error = resp.json()["detail"]["errors"][0]
        assert error["rule"] == "enum_symbols_not_covered"
        assert "GREEN" in error["message"]

    def test_union_branch_mapping(self, client):
        create_subject(client)
        v1 = {
            "type": "union",
            "branches": [
                {"tag": "card", "type": "string"},
                {"tag": "cash", "type": "int"},
            ],
        }
        # cash widened int -> long, new branch added: compatible
        v2 = {
            "type": "union",
            "branches": [
                {"tag": "card", "type": "string"},
                {"tag": "cash", "type": "long"},
                {"tag": "crypto", "type": "bytes"},
            ],
        }
        # drops the 'card' branch: writer data with tag card is unreadable
        v3_bad = {
            "type": "union",
            "branches": [
                {"tag": "cash", "type": "long"},
                {"tag": "crypto", "type": "bytes"},
            ],
        }
        assert client.post("/subjects/users/versions", json={"content": v1}).status_code == 201
        assert client.post("/subjects/users/versions", json={"content": v2}).status_code == 201
        resp = client.post("/subjects/users/versions", json={"content": v3_bad})
        assert resp.status_code == 409
        error = resp.json()["detail"]["errors"][0]
        assert error["rule"] == "union_branch_missing"
        assert error["path"] == ["card"]

    def test_containers_recurse(self, client):
        create_subject(client)
        v1 = {
            "type": "record",
            "name": "Bag",
            "fields": [
                {"id": 1, "name": "vals", "type": {"type": "array", "items": "int"}},
                {"id": 2, "name": "lookup", "type": {"type": "map", "values": "int"}},
            ],
        }
        v2 = {
            "type": "record",
            "name": "Bag",
            "fields": [
                {"id": 1, "name": "vals", "type": {"type": "array", "items": "long"}},
                {"id": 2, "name": "lookup", "type": {"type": "map", "values": "long"}},
            ],
        }
        assert client.post("/subjects/users/versions", json={"content": v1}).status_code == 201
        assert client.post("/subjects/users/versions", json={"content": v2}).status_code == 201

    def test_map_value_mismatch_path(self, client):
        create_subject(client)
        v1 = {
            "type": "record",
            "name": "Bag",
            "fields": [
                {"id": 1, "name": "lookup", "type": {"type": "map", "values": "string"}}
            ],
        }
        v2 = {
            "type": "record",
            "name": "Bag",
            "fields": [
                {"id": 1, "name": "lookup", "type": {"type": "map", "values": "int"}}
            ],
        }
        client.post("/subjects/users/versions", json={"content": v1})
        resp = client.post("/subjects/users/versions", json={"content": v2})
        assert resp.status_code == 409
        error = resp.json()["detail"]["errors"][0]
        assert error["path"] == ["lookup", "{}"]
        assert error["path_string"] == "$.lookup{}"


class TestRecursion:
    LINKED_V1 = {
        "type": "record",
        "name": "Node",
        "fields": [
            {"id": 1, "name": "value", "type": "int"},
            {"id": 2, "name": "next", "type": {"type": "ref", "name": "Node"}},
        ],
    }
    LINKED_V2 = {
        "type": "record",
        "name": "Node",
        "fields": [
            {"id": 1, "name": "value", "type": "long"},
            {"id": 2, "name": "next", "type": {"type": "ref", "name": "Node"}},
            {"id": 3, "name": "label", "type": "string", "default": ""},
        ],
    }
    LINKED_BAD = {
        "type": "record",
        "name": "Node",
        "fields": [
            {"id": 1, "name": "value", "type": "string"},
            {"id": 2, "name": "next", "type": {"type": "ref", "name": "Node"}},
        ],
    }

    def test_recursive_schema_evolves(self, client):
        create_subject(client)
        r1 = client.post("/subjects/users/versions", json={"content": self.LINKED_V1})
        assert r1.status_code == 201
        r2 = client.post("/subjects/users/versions", json={"content": self.LINKED_V2})
        assert r2.status_code == 201

    def test_recursive_schema_violation_terminates(self, client):
        create_subject(client)
        client.post("/subjects/users/versions", json={"content": self.LINKED_V1})
        resp = client.post("/subjects/users/versions", json={"content": self.LINKED_BAD})
        assert resp.status_code == 409
        error = resp.json()["detail"]["errors"][0]
        assert error["rule"] == "primitive_type_mismatch"
        assert error["path"] == ["value"]


class TestModes:
    def test_forward_mode(self, client):
        create_subject(client, mode="FORWARD")
        client.post("/subjects/users/versions", json={"content": USER_V2})
        # dropping a field the old reader requires is a FORWARD violation
        dropped = {
            "type": "record",
            "name": "User",
            "fields": [{"id": 1, "name": "email", "type": "string"}],
        }
        resp = client.post("/subjects/users/versions", json={"content": dropped})
        assert resp.status_code == 409
        error = resp.json()["detail"]["errors"][0]
        assert error["direction"] == "forward"
        assert error["rule"] == "required_field_missing_default"

    def test_full_mode_checks_both_directions(self, client):
        create_subject(client, mode="FULL")
        client.post("/subjects/users/versions", json={"content": USER_V1})
        # backward ok (reader ignores extra writer fields is not the issue here;
        # new reader requires nothing new) but old reader needs 'email'... use a
        # schema that drops a required field: backward-compatible, forward-broken
        dropped = {
            "type": "record",
            "name": "User",
            "fields": [{"id": 2, "name": "age", "type": "int"}],
        }
        resp = client.post("/subjects/users/versions", json={"content": dropped})
        assert resp.status_code == 409
        assert resp.json()["detail"]["errors"][0]["direction"] == "forward"

    def test_transitive_checks_all_history(self, client):
        create_subject(client, mode="BACKWARD_TRANSITIVE")
        v1 = {
            "type": "record",
            "name": "R",
            "fields": [{"id": 1, "name": "a", "type": "int"}],
        }
        # v2 adds field id 9 *with* a default: compatible with v1
        v2 = {
            "type": "record",
            "name": "R",
            "fields": [
                {"id": 1, "name": "a", "type": "int"},
                {"id": 9, "name": "b", "type": "int", "default": 0},
            ],
        }
        # v3 requires id 9 without default: fine against v2 (writer has it),
        # broken against v1 (writer lacks it, no default)
        v3 = {
            "type": "record",
            "name": "R",
            "fields": [
                {"id": 1, "name": "a", "type": "int"},
                {"id": 9, "name": "b", "type": "int"},
            ],
        }
        assert client.post("/subjects/users/versions", json={"content": v1}).status_code == 201
        assert client.post("/subjects/users/versions", json={"content": v2}).status_code == 201
        resp = client.post("/subjects/users/versions", json={"content": v3})
        assert resp.status_code == 409
        errors = resp.json()["detail"]["errors"]
        assert [e["version"] for e in errors] == [1]  # only v1 conflicts
        assert errors[0]["rule"] == "required_field_missing_default"
        assert errors[0]["path"] == ["b"]

    def test_non_transitive_only_checks_latest(self, client):
        # same scenario as above passes under plain BACKWARD
        create_subject(client, mode="BACKWARD")
        v1 = {"type": "record", "name": "R", "fields": [{"id": 1, "name": "a", "type": "int"}]}
        v2 = {
            "type": "record",
            "name": "R",
            "fields": [
                {"id": 1, "name": "a", "type": "int"},
                {"id": 9, "name": "b", "type": "int", "default": 0},
            ],
        }
        v3 = {
            "type": "record",
            "name": "R",
            "fields": [
                {"id": 1, "name": "a", "type": "int"},
                {"id": 9, "name": "b", "type": "int"},
            ],
        }
        client.post("/subjects/users/versions", json={"content": v1})
        client.post("/subjects/users/versions", json={"content": v2})
        resp = client.post("/subjects/users/versions", json={"content": v3})
        assert resp.status_code == 201


class TestCheckEndpoint:
    def test_dry_run_does_not_register(self, client):
        create_subject(client)
        client.post("/subjects/users/versions", json={"content": USER_V1})
        resp = client.post("/subjects/users/check", json={"content": USER_V3_BAD})
        assert resp.status_code == 200
        body = resp.json()
        assert body["is_compatible"] is False
        assert body["errors"][0]["rule"] == "required_field_missing_default"
        # nothing was registered
        assert len(client.get("/subjects/users/versions").json()) == 1

    def test_dry_run_compatible(self, client):
        create_subject(client)
        client.post("/subjects/users/versions", json={"content": USER_V1})
        resp = client.post("/subjects/users/check", json={"content": USER_V2})
        assert resp.json() == {"is_compatible": True, "errors": []}
