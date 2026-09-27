"""Coordinated cross-subject releases: planning and confirmation.

A *release candidate* bundles, for several subjects, the next contract
each of them should publish, plus directed *data-flow edges*
(producer -> consumer). Everything is planned against one history
snapshot:

* each subject's candidate content is checked against that subject's own
  history under its configured compatibility policy (the same rules as
  single-subject registration);
* each edge is checked reader/writer — the consumer's candidate contract
  must be able to read data written under the producer's candidate
  contract;
* the edges induce a dependency graph. The upgrade plan minimises the
  number of simultaneous-switch batches (every subject switches as early
  as its dependencies allow); ties inside a batch break by
  (subject, version) lexicographic order, so the plan is deterministic.
  A cyclic graph is rejected with the shortest conflict cycle.

Candidates are immutable and content-addressed **against the history
snapshot they were planned on**. The fingerprint covers, per subject,
not only the canonical target contract and the edge set, but also the
snapshot itself — the latest version number, that version's fingerprint
and the subject's compatibility mode at planning time. Consequently:

* resubmitting identical content while nothing moved returns the stored
  candidate (CAS idempotency);
* once a participating subject registers a new version or its policy
  mode changes, resubmitting the same target contracts produces a *new*
  candidate with a *new* plan bound to the new snapshot. The stale
  candidate stays stored and queryable, but its confirm is rejected by
  the global CAS.

Confirmation is a *global* compare-and-swap: if any participating
subject's latest version (or that version's fingerprint), or its
compatibility mode, moved since the snapshot, the release is rejected
and nothing is written; otherwise every subject advances to its target
version in a single transaction.
"""
from __future__ import annotations

import json
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .contract import (
    SchemaError,
    canonical_document,
    check_schema,
    fingerprint,
    parse_schema,
    render_path,
    select_violation,
)
from .contract.types import Schema
from .events import RELEASE_COMMITTED, emit_event
from .models import Release, Subject, Version
from .service import evaluate_compatibility

STALE_MESSAGE = (
    "release candidate is stale: a participating subject's latest version "
    "or compatibility mode changed since the plan was built; nothing was written"
)
DRIFTED_MESSAGE = (
    "concurrent release planning raced with a history change: replan against "
    "the current snapshot and retry"
)

# bound the "lost the insert race -> re-read the snapshot" loop
_MAX_CREATE_ATTEMPTS = 10


class ReleaseConflictError(Exception):
    """A 409-style failure carrying a structured ``detail`` payload."""

    def __init__(self, detail: dict) -> None:
        super().__init__(detail["message"])
        self.detail = detail


class ReleaseSubjectMissing(Exception):
    """A subject named by the release candidate does not exist."""


@dataclass(frozen=True)
class _Candidate:
    """One subject's parsed target contract."""

    schema: Schema
    canonical: str
    fingerprint: str


@dataclass(frozen=True)
class _SnapshotEntry:
    """One subject's history snapshot at planning time."""

    subject: Subject
    baseline_version: int
    baseline_fingerprint: str | None  # fingerprint of the latest version, if any

    def as_identity(self) -> dict:
        """Stable snapshot material included in the candidate fingerprint."""
        return {
            "baseline_version": self.baseline_version,
            "baseline_fingerprint": self.baseline_fingerprint,
            "compatibility": self.subject.compatibility,
        }


# ---------------------------------------------------------------------------
# dependency graph (pure functions)
# ---------------------------------------------------------------------------


def topological_layers(
    nodes: list[str], edges: list[tuple[str, str]]
) -> list[list[str]] | None:
    """Kahn's algorithm: each node is placed as early as its dependencies
    allow, which minimises the number of simultaneous-switch batches.
    Returns ``None`` when the graph contains a cycle."""
    indegree = {n: 0 for n in nodes}
    outgoing: dict[str, list[str]] = {n: [] for n in nodes}
    for producer, consumer in edges:
        indegree[consumer] += 1
        outgoing[producer].append(consumer)
    current = sorted(n for n in nodes if indegree[n] == 0)
    layers: list[list[str]] = []
    placed = 0
    while current:
        layers.append(current)
        placed += len(current)
        nxt: list[str] = []
        for node in current:
            for consumer in outgoing[node]:
                indegree[consumer] -= 1
                if indegree[consumer] == 0:
                    nxt.append(consumer)
        current = sorted(nxt)
    if placed != len(nodes):
        return None
    return layers


def shortest_cycle(
    nodes: list[str], edges: list[tuple[str, str]]
) -> list[str] | None:
    """Shortest directed cycle as a closed walk ``[a, b, ..., a]``.

    For every edge ``u -> v`` the shortest path ``v ~> u`` closes a cycle
    through that edge; the minimum over all edges (fewest nodes, then
    lexicographic) is the shortest conflict cycle. Deterministic.
    """
    outgoing: dict[str, list[str]] = {n: [] for n in nodes}
    for producer, consumer in edges:
        outgoing[producer].append(consumer)
    for targets in outgoing.values():
        targets.sort()

    best: list[str] | None = None
    for start, first in edges:
        path = _shortest_path(first, start, outgoing)
        if path is None:
            continue
        walk = [start] + path
        if best is None or (len(walk), walk) < (len(best), best):
            best = walk
    return best


def _shortest_path(
    source: str, target: str, outgoing: dict[str, list[str]]
) -> list[str] | None:
    """BFS shortest path ``source ~> target`` (inclusive); sorted adjacency
    keeps the result deterministic."""
    if source == target:
        return [source]
    parent: dict[str, str | None] = {source: None}
    queue = deque([source])
    while queue:
        node = queue.popleft()
        for nxt in outgoing[node]:
            if nxt in parent:
                continue
            parent[nxt] = node
            if nxt == target:
                path = [nxt]
                while path[-1] != source:
                    path.append(parent[path[-1]])  # type: ignore[arg-type]
                return path[::-1]
            queue.append(nxt)
    return None


# ---------------------------------------------------------------------------
# candidate creation
# ---------------------------------------------------------------------------


def _release_fingerprint(
    canonicals: dict[str, str],
    edges: list[tuple[str, str]],
    snapshots: dict[str, _SnapshotEntry],
) -> str:
    """Content hash of a candidate.

    Covers the canonical target contracts, the sorted edge set **and** the
    per-subject history snapshot the plan is bound to (latest version, its
    fingerprint, compatibility mode). The snapshot terms make a candidate
    change identity as soon as any participating subject drifts, so
    resubmission after a drift can never silently reuse a stale plan.
    """
    doc = json.dumps(
        {
            "subjects": canonicals,
            "edges": [list(edge) for edge in edges],
            "snapshots": {name: snapshots[name].as_identity() for name in sorted(snapshots)},
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return fingerprint(doc)


def _history_by_subject(
    db: Session, subject_ids: list[int]
) -> dict[int, list[Version]]:
    versions = db.query(Version).filter(Version.subject_id.in_(subject_ids)).all()
    by_subject: dict[int, list[Version]] = {}
    for version in versions:
        by_subject.setdefault(version.subject_id, []).append(version)
    return by_subject


def _read_snapshot(
    db: Session, names: list[str], lock: bool
) -> dict[str, _SnapshotEntry]:
    """Read the current history snapshot of ``names``.

    With ``lock=True`` the subject rows are taken in id order with
    ``SELECT ... FOR UPDATE``: on PostgreSQL this serialises planning
    against concurrent registrations and policy changes, so a candidate
    cannot be born stale (SQLite ignores the clause and relies on the
    unique-constraint race below).
    """
    query = db.query(Subject).filter(Subject.name.in_(names)).order_by(Subject.id)
    if lock:
        query = query.with_for_update()
    subjects = query.all()
    by_name = {s.name: s for s in subjects}
    missing = [name for name in names if name not in by_name]
    if missing:
        # subject deleted between the up-front existence check and the
        # locked snapshot read (possible under READ COMMITTED)
        raise ReleaseSubjectMissing(f"subject '{missing[0]}' not found")
    history = _history_by_subject(db, [s.id for s in subjects])
    snapshots: dict[str, _SnapshotEntry] = {}
    for name in names:
        subject = by_name[name]
        rows = history.get(subject.id, [])
        if rows:
            latest = max(rows, key=lambda v: v.version)
            baseline_version = latest.version
            baseline_fingerprint: str | None = latest.fingerprint
        else:
            baseline_version = 0
            baseline_fingerprint = None
        snapshots[name] = _SnapshotEntry(
            subject=subject,
            baseline_version=baseline_version,
            baseline_fingerprint=baseline_fingerprint,
        )
    return snapshots


def snapshot_matches_preconditions(
    snapshots: dict[str, _SnapshotEntry], preconditions: list[dict]
) -> bool:
    """Whether ``snapshots`` are exactly the stored preconditions.

    Shared by releases and migrations: both bind their candidates to the
    same per-subject snapshot terms (latest version, that version's
    fingerprint, compatibility mode at planning time).
    """
    preconditions = {p["subject"]: p for p in preconditions}
    if set(preconditions) != set(snapshots):
        return False
    for name, snap in snapshots.items():
        pre = preconditions[name]
        if (
            pre["baseline_version"] != snap.baseline_version
            or pre.get("baseline_fingerprint") != snap.baseline_fingerprint
            or pre["compatibility"] != snap.subject.compatibility
        ):
            return False
    return True


def _snapshot_matches_release(
    release: Release, snapshots: dict[str, _SnapshotEntry]
) -> bool:
    """Whether ``snapshots`` are exactly the preconditions of ``release``."""
    return snapshot_matches_preconditions(snapshots, release.plan["preconditions"])


def _fail(db: Session, message: str, **extra: Any) -> None:
    """Roll back any snapshot locks and raise a structured 409 error."""
    db.rollback()
    raise ReleaseConflictError({"message": message, **extra})


class _SnapshotDrifted(Exception):
    """Insert raced a concurrent snapshot change; the caller must replan."""

    def __init__(self, fingerprint: str) -> None:
        super().__init__("snapshot drifted during release creation")
        self.fingerprint = fingerprint


def create_release(
    db: Session,
    contents: dict[str, Any],
    edges: Iterable[tuple[str, str]],
) -> tuple[Release, bool]:
    """Validate and store a release candidate. Returns ``(release, created)``.

    Candidate identity includes the per-subject history snapshot (latest
    version, its fingerprint, compatibility mode):

    * identical canonical content on the **same snapshot** -> existing
      candidate, ``created=False``;
    * identical content after a subject drifted -> a **new** candidate
      bound to the new snapshot; the old one remains stored and queryable;
    * unknown subject -> :class:`ReleaseSubjectMissing`;
    * invalid contract -> :class:`SchemaError`;
    * no-op subject / policy violation / incompatible edge / dependency
      cycle -> :class:`ReleaseConflictError` (nothing is stored).

    The history lookup and the unique-fingerprint insert are re-validated
    in a bounded retry loop, so concurrent creates racing a history
    change converge on candidates whose plans match the stored snapshot.
    """
    candidates: dict[str, _Candidate] = {}
    for name in sorted(contents):
        try:
            schema = parse_schema(contents[name])
        except SchemaError as exc:
            raise SchemaError(f"subject '{name}': {exc}") from exc
        canonical = canonical_document(schema)
        candidates[name] = _Candidate(schema, canonical, fingerprint(canonical))
    names = sorted(candidates)
    edge_set = sorted(set(edges))

    # Existence is checked up front (it never changes under a lock here);
    # snapshot reads below can assume every name resolves.
    existing_subjects = {
        s.name for s in db.query(Subject).filter(Subject.name.in_(names)).all()
    }
    for name in names:
        if name not in existing_subjects:
            raise ReleaseSubjectMissing(f"subject '{name}' not found")

    for attempt in range(_MAX_CREATE_ATTEMPTS):
        try:
            return _create_release_once(db, contents, candidates, names, edge_set)
        except _SnapshotDrifted as drifted:
            # Another request won the unique-fingerprint insert after a
            # concurrent history change: roll back snapshot locks, re-read
            # and replan on the new snapshot (which may turn this into a
            # CAS hit or a validation failure, e.g. the candidate just
            # became a no-op).
            db.rollback()
            winner = (
                db.query(Release)
                .filter(Release.fingerprint == drifted.fingerprint)
                .first()
            )
            if winner is not None:
                snapshots = _read_snapshot(db, names, lock=False)
                if _snapshot_matches_release(winner, snapshots):
                    return winner, False
            # no matching winner (or it was stored against another
            # snapshot): the next attempt replans from scratch
            continue
    raise ReleaseConflictError({"message": DRIFTED_MESSAGE})


def _create_release_once(
    db: Session,
    contents: dict[str, Any],
    candidates: dict[str, _Candidate],
    names: list[str],
    edge_set: list[tuple[str, str]],
) -> tuple[Release, bool]:
    snapshots = _read_snapshot(db, names, lock=True)
    baselines = {name: snapshots[name].baseline_version for name in names}
    targets = {name: baselines[name] + 1 for name in names}
    fp = _release_fingerprint(
        {name: c.canonical for name, c in candidates.items()},
        edge_set,
        snapshots,
    )
    existing = db.query(Release).filter(Release.fingerprint == fp).first()
    if existing is not None:
        if _snapshot_matches_release(existing, snapshots):
            db.rollback()  # release snapshot locks taken above
            return existing, False
        # Fingerprint collision with a candidate whose stored snapshot is
        # not the one we just read: the row was stored against a different
        # baseline — replan on the current snapshot.
        db.rollback()
        raise _SnapshotDrifted(fp)

    noop: list[dict] = []
    for name in names:
        snap = snapshots[name]
        if snap.baseline_fingerprint == candidates[name].fingerprint:
            noop.append(
                {
                    "subject": name,
                    "existing_version": snap.baseline_version,
                    "fingerprint": snap.baseline_fingerprint,
                }
            )
    if noop:
        _fail(
            db,
            "release must advance every subject, but submitted content is "
            "already registered",
            subjects=noop,
        )

    subject_errors = []
    for name in names:
        errors = evaluate_compatibility(
            snapshots[name].subject, candidates[name].schema, db
        )
        if errors:
            subject_errors.append({"subject": name, "errors": errors})
    if subject_errors:
        _fail(
            db,
            "release candidate is not compatible with the subjects' history",
            subjects=subject_errors,
        )

    edge_errors = []
    for producer, consumer in edge_set:
        # the consumer's next contract must read the producer's next contract
        violations = check_schema(
            candidates[consumer].schema, candidates[producer].schema
        )
        if violations:
            best = select_violation(violations)
            edge_errors.append(
                {
                    "producer": producer,
                    "consumer": consumer,
                    "producer_version": targets[producer],
                    "consumer_version": targets[consumer],
                    "rule": best.rule,
                    "path": list(best.path),
                    "path_string": render_path(best.path),
                    "message": best.message,
                }
            )
    if edge_errors:
        _fail(db, "release candidate has incompatible data-flow edges", edges=edge_errors)

    layers = topological_layers(names, edge_set)
    if layers is None:
        cycle = shortest_cycle(names, edge_set) or []
        _fail(
            db,
            "release candidate has a cyclic data-flow dependency",
            rule="cyclic_dependency",
            cycle=[
                {"subject": n, "target_version": targets[n]} for n in cycle
            ],
        )

    plan = _build_plan(names, snapshots, candidates, targets, edge_set, layers)
    request_doc = {
        "subjects": {
            name: {
                "content": contents[name],
                "canonical": candidates[name].canonical,
                "fingerprint": candidates[name].fingerprint,
            }
            for name in names
        },
        "edges": [[p, c] for p, c in edge_set],
    }
    release = Release(
        fingerprint=fp, status="planned", request=request_doc, plan=plan
    )
    db.add(release)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        # Identical candidate landed concurrently -> CAS makes it idempotent,
        # provided the winner really matches the snapshot we just read.
        winner = db.query(Release).filter(Release.fingerprint == fp).first()
        if winner is not None:
            current = _read_snapshot(db, names, lock=False)
            if _snapshot_matches_release(winner, current):
                return winner, False
        raise _SnapshotDrifted(fp) from None
    db.refresh(release)
    return release, True


def _build_plan(
    names: list[str],
    snapshots: dict[str, _SnapshotEntry],
    candidates: dict[str, _Candidate],
    targets: dict[str, int],
    edges: list[tuple[str, str]],
    layers: list[list[str]],
) -> dict:
    plan_edges = [
        {
            "producer": producer,
            "consumer": consumer,
            "producer_version": targets[producer],
            "consumer_version": targets[consumer],
        }
        for producer, consumer in edges
    ]
    batches = []
    for index, layer in enumerate(layers):
        members = sorted(layer, key=lambda n: (n, targets[n]))
        in_batch = set(members)
        batches.append(
            {
                "batch": index,
                "subjects": [
                    {"subject": n, "target_version": targets[n]} for n in members
                ],
                # data-flow edges satisfied once this batch has switched
                "edges": [e for e in plan_edges if e["consumer"] in in_batch],
            }
        )
    return {
        "subjects": [
            {
                "subject": name,
                "baseline_version": snapshots[name].baseline_version,
                "baseline_fingerprint": snapshots[name].baseline_fingerprint,
                "target_version": targets[name],
                "fingerprint": candidates[name].fingerprint,
                "compatibility": snapshots[name].subject.compatibility,
            }
            for name in names
        ],
        "batches": batches,
        "edges": plan_edges,
        "preconditions": [
            {
                "subject": name,
                "baseline_version": snapshots[name].baseline_version,
                "baseline_fingerprint": snapshots[name].baseline_fingerprint,
                "compatibility": snapshots[name].subject.compatibility,
            }
            for name in names
        ],
    }


# ---------------------------------------------------------------------------
# confirmation (global compare-and-swap)
# ---------------------------------------------------------------------------


def _latest_versions(
    db: Session, subject_ids: list[int]
) -> dict[int, tuple[int, str | None]]:
    """``subject_id -> (latest version, that version's fingerprint)``."""
    rows = (
        db.query(Version.subject_id, Version.version, Version.fingerprint)
        .filter(Version.subject_id.in_(subject_ids))
        .all()
    )
    latest: dict[int, tuple[int, str | None]] = {}
    for subject_id, version, version_fp in rows:
        current = latest.get(subject_id)
        if current is None or version > current[0]:
            latest[subject_id] = (version, version_fp)
    return latest


def baseline_conflicts(db: Session, entries: list[dict]) -> list[dict]:
    """Snapshot terms that no longer match: latest version (+ fingerprint)
    and the subject's compatibility mode.

    Each entry must carry ``subject``, ``baseline_version``,
    ``baseline_fingerprint`` and ``compatibility``. Subject rows are locked
    in id order so concurrent confirmations serialise instead of deadlocking.
    Shared by release and migration confirmation (their global CAS is the
    same comparison).
    """
    names = [entry["subject"] for entry in entries]
    subjects = (
        db.query(Subject)
        .filter(Subject.name.in_(names))
        .order_by(Subject.id)
        .with_for_update()
        .all()
    )
    by_name = {s.name: s for s in subjects}
    latest = _latest_versions(db, [s.id for s in subjects])
    conflicts = []
    for entry in entries:
        subject = by_name.get(entry["subject"])
        if subject is None:
            conflicts.append(
                {
                    "subject": entry["subject"],
                    "kind": "subject_missing",
                    "expected_baseline": entry["baseline_version"],
                    "actual_latest": None,
                }
            )
            continue
        actual_version, actual_fp = latest.get(subject.id, (0, None))
        if actual_version != entry["baseline_version"]:
            conflicts.append(
                {
                    "subject": entry["subject"],
                    "kind": "version_drift",
                    "expected_baseline": entry["baseline_version"],
                    "actual_latest": actual_version,
                }
            )
        elif actual_fp != entry.get("baseline_fingerprint"):
            # Same version number, different content fingerprint: the
            # history behind the baseline was replaced.
            conflicts.append(
                {
                    "subject": entry["subject"],
                    "kind": "fingerprint_drift",
                    "expected_baseline": entry["baseline_version"],
                    "expected_fingerprint": entry.get("baseline_fingerprint"),
                    "actual_fingerprint": actual_fp,
                }
            )
        if subject.compatibility != entry["compatibility"]:
            conflicts.append(
                {
                    "subject": entry["subject"],
                    "kind": "compatibility_drift",
                    "expected_compatibility": entry["compatibility"],
                    "actual_compatibility": subject.compatibility,
                }
            )
    return conflicts


def _baseline_conflicts(db: Session, release: Release) -> list[dict]:
    """CAS precondition check for a release candidate's plan subjects."""
    return baseline_conflicts(db, release.plan["subjects"])


def _committed_result(release: Release) -> dict:
    return {
        "fingerprint": release.fingerprint,
        "status": release.status,
        "versions": [
            {
                "subject": s["subject"],
                "version": s["target_version"],
                "fingerprint": s["fingerprint"],
            }
            for s in release.plan["subjects"]
        ],
    }


def confirm_release(db: Session, release: Release) -> dict:
    """Commit a planned release via a global compare-and-swap.

    Every participating subject's latest version (with its fingerprint)
    and compatibility mode must still equal the snapshot preconditions.
    If so, all target versions are inserted in one transaction. If any
    subject moved, the release is rejected and nothing is written.
    Confirming an already committed release replays its stored result
    (idempotent).
    """
    if release.status == "committed":
        return _committed_result(release)

    conflicts = _baseline_conflicts(db, release)
    if conflicts:
        raise ReleaseConflictError({"message": STALE_MESSAGE, "conflicts": conflicts})

    names = [s["subject"] for s in release.plan["subjects"]]
    subjects = {
        s.name: s for s in db.query(Subject).filter(Subject.name.in_(names)).all()
    }
    for entry in release.plan["subjects"]:
        spec = release.request["subjects"][entry["subject"]]
        db.add(
            Version(
                subject_id=subjects[entry["subject"]].id,
                version=entry["target_version"],
                fingerprint=spec["fingerprint"],
                content=spec["content"],
                canonical=spec["canonical"],
            )
        )
    release.status = "committed"
    release.committed_at = datetime.now(timezone.utc)
    # one event per participating subject, all in the same transaction as
    # the version inserts and the status flip: consumers filtering by
    # subject see exactly the slice that concerns them
    committed_versions = [
        {
            "subject": entry["subject"],
            "version": entry["target_version"],
            "fingerprint": entry["fingerprint"],
        }
        for entry in release.plan["subjects"]
    ]
    for committed in committed_versions:
        emit_event(
            db,
            RELEASE_COMMITTED,
            subject=committed["subject"],
            version=committed["version"],
            fingerprint=release.fingerprint,
            payload={
                "release": release.fingerprint,
                "subject": committed["subject"],
                "version": committed["version"],
                "fingerprint": committed["fingerprint"],
                "versions": committed_versions,
            },
        )
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        db.refresh(release)
        if release.status == "committed":
            # an identical confirm won the race — converge on its result
            return _committed_result(release)
        raise ReleaseConflictError(
            {"message": STALE_MESSAGE, "conflicts": _baseline_conflicts(db, release)}
        ) from None
    return _committed_result(release)
