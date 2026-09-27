"""Compatibility-policy migration candidates: planning and confirmation.

A *migration candidate* bundles, for one or more subjects, the target
compatibility policy each subject should switch to. The plan is produced
by **replaying each subject's history in version order under the target
policy** against a single history snapshot (the same one for every
participating subject):

* for plain modes (``BACKWARD`` / ``FORWARD`` / ``FULL``) every historical
  version is replayed against its **immediate predecessor** — the latest
  version at the time it was published;
* for ``*_TRANSITIVE`` modes every historical version is replayed against
  **all** of its predecessors.

Each conflicting ordered **version pair** is reported with the direction
of the violated check (``backward``: the later version is the reader,
``forward``: the earlier version is the reader), the violated rule and the
shortest field path (shortest path wins, lexicographic tie-break — exactly
as in single-subject registration). These are the conflicts a policy
switch would newly introduce; the first version has no predecessor and is
therefore never in a conflict pair.

Candidates are immutable and content-addressed **against the history
snapshot they were planned on**. As with coordinated releases the
fingerprint covers, per subject, the from/to policies and the snapshot
itself (latest version number, that version's fingerprint, the current
compatibility mode). Consequently:

* resubmitting identical targets while nothing moved returns the stored
  candidate (CAS idempotency);
* once a participating subject registers a new version or changes its
  policy, resubmitting the same targets produces a *new* candidate bound
  to the new snapshot. The stale candidate stays stored and queryable, but
  its confirm is rejected by the global CAS.

A candidate whose replay finds conflicts is stored with status
``blocked`` (rather than rejected): it is an immutable audit record of
which version pairs would become illegal under the proposed policy. A
blocked candidate **cannot be confirmed** — there is no history repair
that could make it valid — and a confirm attempt returns its conflict
pairs with HTTP 409. Only a ``planned`` (conflict-free) candidate can be
confirmed.

Confirmation is a *global* compare-and-swap, identical in spirit to
coordinated releases: the candidate row is locked first and its status
re-read under the lock, then subject rows are locked in id order, and
every participating subject's latest version (with its fingerprint) and
current policy must still equal the snapshot precondition. If any subject
moved, the migration is rejected with the full set of drift conflicts and
no policy is changed; otherwise every subject's policy is switched to its
target mode in a single atomic transaction. Locking the candidate row
first serialises concurrent confirms of the **same** candidate: the loser
observes the committed status once the winner commits and replays the
stored result (idempotent) instead of misreading the winner's policy
switch as ``compatibility_drift``. Confirms of **different** candidates
are still decided purely by the baseline CAS — a subject row already
carrying the target mode is never taken as proof that *this* candidate
committed. Switching a subject's policy here changes that subject's
snapshot and therefore invalidates any previously planned coordinated
release candidate (its global CAS will report ``compatibility_drift``) —
coordinated releases keep their original snapshot-failure semantics
unchanged.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .contract import check_schema, fingerprint, parse_schema, render_path
from .events import MIGRATION_COMMITTED, emit_event
from .models import Migration, Subject, Version
from .release import (
    ReleaseConflictError,
    ReleaseSubjectMissing,
    _MAX_CREATE_ATTEMPTS,
    _SnapshotDrifted,
    _read_snapshot,
    baseline_conflicts,
    snapshot_matches_preconditions,
)
from .schemas import Mode
from .service import _directions

PLANNED = "planned"
BLOCKED = "blocked"
COMMITTED = "committed"

BLOCKED_MESSAGE = (
    "migration candidate is blocked: replaying the subject history under "
    "the target policies found conflicting version pairs; the candidate is "
    "stored for inspection but cannot be confirmed"
)
STALE_MESSAGE = (
    "migration candidate is stale: a participating subject's latest version "
    "or compatibility mode changed since the plan was built; nothing was written"
)
DRIFTED_MESSAGE = (
    "concurrent migration planning raced with a history change: replan "
    "against the current snapshot and retry"
)
NOOP_MESSAGE = (
    "migration must change every subject's policy, but the target policy is "
    "already in effect"
)


# ---------------------------------------------------------------------------
# history replay (pure-ish: reads Version rows from the snapshot)
# ---------------------------------------------------------------------------


def replay_history(
    versions: list[Version], target_mode: Mode
) -> list[dict]:
    """Replay a subject's history in version order under ``target_mode``.

    ``versions`` are the snapshot rows (already filtered to one subject),
    ordered by version. For version ``v`` the predecessors are ``v-1`` for
    plain modes and ``v-1, ..., 1`` for ``*_TRANSITIVE`` modes. Every
    conflicting ordered pair ``(from_version, to_version)`` is reported
    once with the minimal violation (shortest field path, lexicographic
    tie-break) and its direction. Results are sorted by
    ``(to_version, from_version)``.
    """
    history = sorted(versions, key=lambda row: row.version)
    backward, forward = _directions(target_mode)
    transitive = target_mode.value.endswith("_TRANSITIVE")
    parsed = {row.version: parse_schema(row.content) for row in history}

    pairs: list[dict] = []
    for index, row in enumerate(history):
        if index == 0:
            continue
        predecessors = history[:index] if transitive else history[index - 1 : index]
        candidate = parsed[row.version]
        for old in predecessors:
            old_schema = parsed[old.version]
            found: list[tuple[str, object]] = []
            if backward:  # the later version reads data written by the old one
                found += [("backward", v) for v in check_schema(candidate, old_schema)]
            if forward:  # the old version reads data written by the later one
                found += [("forward", v) for v in check_schema(old_schema, candidate)]
            if not found:
                continue
            direction, best = min(
                found, key=lambda dv: (len(dv[1].path), dv[1].path, dv[0])
            )
            pairs.append(
                {
                    "from_version": old.version,
                    "to_version": row.version,
                    "direction": direction,
                    "rule": best.rule,
                    "path": list(best.path),
                    "path_string": render_path(best.path),
                    "message": best.message,
                }
            )
    pairs.sort(key=lambda p: (p["to_version"], p["from_version"]))
    return pairs


# ---------------------------------------------------------------------------
# candidate creation
# ---------------------------------------------------------------------------


def _migration_fingerprint(
    targets: dict[str, str], snapshots: dict
) -> str:
    """Content hash of a migration candidate.

    Covers the per-subject target policies **and** the history snapshot the
    plan is bound to (latest version, its fingerprint, current policy), so
    a candidate changes identity as soon as any participating subject
    drifts; resubmission after a drift can never silently reuse a stale
    plan.
    """
    doc = json.dumps(
        {
            "kind": "compatibility-migration",
            "targets": {name: targets[name] for name in sorted(targets)},
            "snapshots": {
                name: {
                    "baseline_version": snapshots[name].baseline_version,
                    "baseline_fingerprint": snapshots[name].baseline_fingerprint,
                    "compatibility": snapshots[name].subject.compatibility,
                }
                for name in sorted(snapshots)
            },
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return fingerprint(doc)


def _history_rows(db: Session, subject_ids: list[int]) -> dict[int, list[Version]]:
    rows = (
        db.query(Version)
        .filter(Version.subject_id.in_(subject_ids))
        .order_by(Version.version)
        .all()
    )
    by_subject: dict[int, list[Version]] = {}
    for row in rows:
        by_subject.setdefault(row.subject_id, []).append(row)
    return by_subject


def create_migration(
    db: Session, targets: dict[str, Mode]
) -> tuple[Migration, bool]:
    """Validate and store a policy-migration candidate.

    Returns ``(migration, created)``:

    * identical targets on the **same snapshot** -> existing candidate,
      ``created=False``;
    * identical targets after a subject drifted -> a **new** candidate
      bound to the new snapshot; the old one remains stored and queryable;
    * unknown subject -> :class:`ReleaseSubjectMissing`;
    * a subject whose current policy already equals the target ->
      :class:`ReleaseConflictError` (the migration must advance every
      subject; nothing is stored).

    A replay that finds conflicting version pairs is still stored — with
    status ``blocked``. The snapshot read and unique-fingerprint insert are
    re-validated in a bounded retry loop, mirroring coordinated release
    creation.
    """
    names = sorted(targets)
    target_values = {name: targets[name].value for name in names}

    existing_subjects = {
        s.name for s in db.query(Subject).filter(Subject.name.in_(names)).all()
    }
    for name in names:
        if name not in existing_subjects:
            raise ReleaseSubjectMissing(f"subject '{name}' not found")

    for attempt in range(_MAX_CREATE_ATTEMPTS):
        try:
            return _create_migration_once(db, target_values, names)
        except _SnapshotDrifted as drifted:
            db.rollback()
            winner = (
                db.query(Migration)
                .filter(Migration.fingerprint == drifted.fingerprint)
                .first()
            )
            if winner is not None:
                snapshots = _read_snapshot(db, names, lock=False)
                if snapshot_matches_preconditions(
                    snapshots, winner.plan["preconditions"]
                ):
                    return winner, False
            continue
    raise ReleaseConflictError({"message": DRIFTED_MESSAGE})


def _create_migration_once(
    db: Session, target_values: dict[str, str], names: list[str]
) -> tuple[Migration, bool]:
    snapshots = _read_snapshot(db, names, lock=True)
    fp = _migration_fingerprint(target_values, snapshots)
    existing = db.query(Migration).filter(Migration.fingerprint == fp).first()
    if existing is not None:
        if snapshot_matches_preconditions(
            snapshots, existing.plan["preconditions"]
        ):
            db.rollback()  # snapshot locks taken above
            return existing, False
        db.rollback()
        raise _SnapshotDrifted(fp)

    noop = [
        {
            "subject": name,
            "compatibility": snapshots[name].subject.compatibility,
        }
        for name in names
        if snapshots[name].subject.compatibility == target_values[name]
    ]
    if noop:
        db.rollback()
        raise ReleaseConflictError({"message": NOOP_MESSAGE, "subjects": noop})

    history = _history_rows(db, [snapshots[name].subject.id for name in names])

    plan_subjects: list[dict] = []
    preconditions: list[dict] = []
    conflicts: list[dict] = []
    for name in names:
        snap = snapshots[name]
        mode = Mode(target_values[name])
        pairs = replay_history(history.get(snap.subject.id, []), mode)
        for pair in pairs:
            conflicts.append({"subject": name, **pair})
        plan_subjects.append(
            {
                "subject": name,
                "baseline_version": snap.baseline_version,
                "baseline_fingerprint": snap.baseline_fingerprint,
                "from_compatibility": snap.subject.compatibility,
                "to_compatibility": target_values[name],
            }
        )
        preconditions.append(
            {
                "subject": name,
                "baseline_version": snap.baseline_version,
                "baseline_fingerprint": snap.baseline_fingerprint,
                "compatibility": snap.subject.compatibility,
            }
        )
    status = BLOCKED if conflicts else PLANNED

    request_doc = {
        "subjects": {
            name: {
                "from_compatibility": snapshots[name].subject.compatibility,
                "to_compatibility": target_values[name],
            }
            for name in names
        }
    }
    plan = {
        "subjects": plan_subjects,
        "conflicts": conflicts,
        "preconditions": preconditions,
    }
    migration = Migration(
        fingerprint=fp, status=status, request=request_doc, plan=plan
    )
    db.add(migration)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        winner = db.query(Migration).filter(Migration.fingerprint == fp).first()
        if winner is not None:
            current = _read_snapshot(db, names, lock=False)
            if snapshot_matches_preconditions(
                current, winner.plan["preconditions"]
            ):
                return winner, False
        raise _SnapshotDrifted(fp) from None
    db.refresh(migration)
    return migration, True


# ---------------------------------------------------------------------------
# confirmation (global compare-and-swap)
# ---------------------------------------------------------------------------


def _committed_result(migration: Migration) -> dict:
    return {
        "fingerprint": migration.fingerprint,
        "status": migration.status,
        "subjects": migration.plan["subjects"],
    }


def confirm_migration(db: Session, migration: Migration) -> dict:
    """Commit a planned migration via a global compare-and-swap.

    * an already-committed candidate replays its stored result (idempotent);
    * a blocked candidate is refused with its conflict pairs (409);
    * every participating subject's latest version (with its fingerprint)
      and current policy must still equal the snapshot precondition. If any
      subject moved, the migration is rejected with **all** drift conflicts
      and no policy is changed;
    * otherwise every subject's policy is switched to its target mode in
      one transaction.

    Locking order: the **candidate row** is locked first (``SELECT ... FOR
    UPDATE``) and its status re-read under the lock, so concurrent confirms
    of the *same* candidate serialise on it — the loser blocks until the
    winner commits, then observes the committed status and replays the
    stored result instead of misreading the winner's policy switch as
    ``compatibility_drift``. Subject rows are locked afterwards (in id
    order) for the baseline CAS, so confirms of *different* candidates are
    still decided by the snapshot preconditions alone. Policy switches are
    guarded UPDATEs requiring the **snapshot** mode — never the target
    mode: a row already carrying the target mode is not proof that *this*
    candidate committed (a different candidate, or an out-of-band change,
    may have put it there). The guard is what protects databases without
    row-level locking (SQLite, which ignores ``FOR UPDATE``); on PostgreSQL
    the row locks already serialise the switch.
    """
    if migration.status == COMMITTED:
        return _committed_result(migration)

    # Lock the candidate row and re-read its status under the lock. On
    # PostgreSQL a concurrent confirm of the same candidate blocks here
    # until the winner commits, then converges on the stored result below.
    migration = (
        db.query(Migration)
        .filter(Migration.id == migration.id)
        .populate_existing()
        .with_for_update()
        .one()
    )
    if migration.status == COMMITTED:
        return _committed_result(migration)

    if migration.status == BLOCKED:
        db.rollback()
        raise ReleaseConflictError(
            {
                "message": BLOCKED_MESSAGE,
                "rule": "history_replay_conflicts",
                "conflicts": migration.plan["conflicts"],
            }
        )

    entries = migration.plan["preconditions"]
    conflicts = baseline_conflicts(db, entries)
    if conflicts:
        db.rollback()
        raise ReleaseConflictError({"message": STALE_MESSAGE, "conflicts": conflicts})

    # Locked rows still match the preconditions. Switch every policy with a
    # guarded UPDATE that requires the snapshot mode — never the target
    # mode. A row that no longer carries the snapshot mode was changed by a
    # concurrent transaction between the CAS check and this write (only
    # reachable without row locks, i.e. SQLite).
    by_name = {entry["subject"]: entry for entry in entries}
    subjects_in_id_order = (
        db.query(Subject)
        .filter(Subject.name.in_([e["subject"] for e in entries]))
        .order_by(Subject.id)
        .all()
    )
    targets = {s["subject"]: s["to_compatibility"] for s in migration.plan["subjects"]}
    for subject in subjects_in_id_order:
        expected = by_name[subject.name]["compatibility"]
        wanted = targets[subject.name]
        result = db.execute(
            update(Subject)
            .where(
                Subject.id == subject.id,
                Subject.compatibility == expected,
            )
            .values(compatibility=wanted)
        )
        if result.rowcount != 1:
            # Re-read the candidate itself to decide what happened: if a
            # twin confirm of *this* candidate won the race, converge on
            # its committed result; anything else (a different candidate,
            # an out-of-band config change) is genuine drift and the whole
            # migration rolls back.
            db.rollback()
            db.refresh(migration)
            if migration.status == COMMITTED:
                return _committed_result(migration)
            raise ReleaseConflictError(
                {"message": STALE_MESSAGE, "conflicts": baseline_conflicts(db, entries)}
            )

    # Mark the candidate committed. On PostgreSQL the candidate row lock
    # taken above already serialises same-candidate confirms, so a
    # concurrent confirm reaches this point only after seeing the committed
    # status and returning early; the integrity/refresh handling below is a
    # belt-and-braces convergence for databases without row locks.
    migration.status = COMMITTED
    migration.committed_at = datetime.now(timezone.utc)
    # one event per migrated subject, all in the same transaction as the
    # guarded policy UPDATEs and the status flip; subject filters on the
    # replay side therefore match the migrated subjects
    switched_subjects = [
        {
            "subject": entry["subject"],
            "from_compatibility": entry["from_compatibility"],
            "to_compatibility": entry["to_compatibility"],
        }
        for entry in migration.plan["subjects"]
    ]
    for switched in switched_subjects:
        emit_event(
            db,
            MIGRATION_COMMITTED,
            subject=switched["subject"],
            version=None,
            fingerprint=migration.fingerprint,
            payload={
                "migration": migration.fingerprint,
                **switched,
                "subjects": switched_subjects,
            },
        )
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        db.refresh(migration)
        if migration.status == COMMITTED:
            return _committed_result(migration)
        raise ReleaseConflictError(
            {"message": STALE_MESSAGE, "conflicts": baseline_conflicts(db, entries)}
        ) from None
    return _committed_result(migration)
