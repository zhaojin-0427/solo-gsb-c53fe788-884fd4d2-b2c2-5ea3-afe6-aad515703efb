"""Registration and compatibility orchestration.

Mode semantics (candidate = the schema being registered):

* ``BACKWARD``  — candidate is the *reader*, latest version is the writer;
* ``FORWARD``   — candidate is the *writer*, latest version is the reader;
* ``FULL``      — both directions against the latest version;
* ``*_TRANSITIVE`` — same direction(s), but against **all** historical
  versions instead of only the latest.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .contract import (
    SchemaError,
    Violation,
    canonical_document,
    check_schema,
    fingerprint,
    parse_schema,
    render_path,
)
from .contract.types import Schema
from .events import VERSION_REGISTERED, emit_event
from .models import Subject, Version
from .schemas import Mode


class IncompatibleSchemaError(Exception):
    def __init__(self, errors: list[dict]) -> None:
        super().__init__("schema is not compatible with the subject history")
        self.errors = errors


class ConcurrentConflictError(Exception):
    """Another registration on the same baseline committed first."""


def _directions(mode: Mode) -> tuple[bool, bool]:
    """Return (check_backward, check_forward) for a compatibility mode."""
    if mode in (Mode.BACKWARD, Mode.BACKWARD_TRANSITIVE):
        return True, False
    if mode in (Mode.FORWARD, Mode.FORWARD_TRANSITIVE):
        return False, True
    return True, True


def _next_version(db: Session, subject_id: int) -> int:
    current = db.query(func.max(Version.version)).filter(
        Version.subject_id == subject_id
    ).scalar()
    return (current or 0) + 1


def evaluate_compatibility(
    subject: Subject, candidate: Schema, db: Session
) -> list[dict]:
    """Check a candidate schema against the subject's history.

    Returns one error entry per conflicting historical version, carrying
    the minimal violation (shortest field path, lexicographic tie-break).
    """
    mode = Mode(subject.compatibility)
    history = (
        db.query(Version)
        .filter(Version.subject_id == subject.id)
        .order_by(Version.version)
        .all()
    )
    if not history:
        return []
    targets = history if mode.value.endswith("_TRANSITIVE") else history[-1:]
    backward, forward = _directions(mode)
    errors: list[dict] = []
    for old in targets:
        old_schema = parse_schema(old.content)
        found: list[tuple[str, Violation]] = []
        if backward:  # candidate reads data written by the old version
            found += [("backward", v) for v in check_schema(candidate, old_schema)]
        if forward:  # the old version reads data written by the candidate
            found += [("forward", v) for v in check_schema(old_schema, candidate)]
        if found:
            direction, best = min(
                found, key=lambda dv: (len(dv[1].path), dv[1].path, dv[0])
            )
            errors.append(
                {
                    "version": old.version,
                    "direction": direction,
                    "rule": best.rule,
                    "path": list(best.path),
                    "path_string": render_path(best.path),
                    "message": best.message,
                }
            )
    return errors


def register_version(
    db: Session, subject: Subject, content: Any
) -> tuple[Version, bool]:
    """Register ``content`` under ``subject``. Returns (version, created).

    * identical canonical content -> existing version, ``created=False`` (CAS);
    * incompatible -> :class:`IncompatibleSchemaError`;
    * lost a concurrent race on the same baseline ->
      :class:`ConcurrentConflictError` (only one concurrent registration
      on the same baseline can succeed).
    """
    schema = parse_schema(content)  # raises SchemaError on invalid input
    canonical = canonical_document(schema)
    fp = fingerprint(canonical)

    existing = (
        db.query(Version)
        .filter(Version.subject_id == subject.id, Version.fingerprint == fp)
        .first()
    )
    if existing is not None:
        return existing, False

    errors = evaluate_compatibility(subject, schema, db)
    if errors:
        raise IncompatibleSchemaError(errors)

    row = Version(
        subject_id=subject.id,
        version=_next_version(db, subject.id),
        fingerprint=fp,
        content=content,
        canonical=canonical,
    )
    db.add(row)
    try:
        db.flush()
        # the event rides the same transaction as the version row; a
        # rollback (e.g. the lost CAS race below) removes it as well
        emit_event(
            db,
            VERSION_REGISTERED,
            subject=subject.name,
            version=row.version,
            fingerprint=fp,
            payload={
                "subject": subject.name,
                "version": row.version,
                "fingerprint": fp,
                "compatibility": subject.compatibility,
                "canonical": canonical,
            },
        )
        db.commit()
    except IntegrityError:
        db.rollback()
        # Same fingerprint landed concurrently -> CAS makes it idempotent.
        winner = (
            db.query(Version)
            .filter(Version.subject_id == subject.id, Version.fingerprint == fp)
            .first()
        )
        if winner is not None:
            return winner, False
        raise ConcurrentConflictError(
            "concurrent registration on the same baseline: another version "
            "was committed first; re-check compatibility against the new "
            "latest version and retry"
        ) from None
    db.refresh(row)
    return row, True
