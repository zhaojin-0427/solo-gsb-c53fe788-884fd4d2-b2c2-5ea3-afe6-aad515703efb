"""Compatibility-policy migration endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Migration
from ..migration import (
    ReleaseConflictError,
    ReleaseSubjectMissing,
    confirm_migration,
    create_migration,
)
from ..schemas import (
    MigrationConfirmResponse,
    MigrationCreate,
    MigrationDetail,
    MigrationResponse,
    MigrationSummary,
)

router = APIRouter(prefix="/migrations", tags=["migrations"])


def get_migration_or_404(db: Session, fingerprint: str) -> Migration:
    migration = (
        db.query(Migration).filter(Migration.fingerprint == fingerprint).first()
    )
    if migration is None:
        raise HTTPException(
            status_code=404, detail=f"migration '{fingerprint}' not found"
        )
    return migration


@router.post("", response_model=MigrationResponse)
def create_candidate(
    body: MigrationCreate, response: Response, db: Session = Depends(get_db)
):
    targets = {name: spec.to_compatibility for name, spec in body.subjects.items()}
    try:
        migration, created = create_migration(db, targets)
    except ReleaseSubjectMissing as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ReleaseConflictError as exc:
        raise HTTPException(status_code=409, detail=exc.detail) from exc
    # Conflict-bearing candidates ARE stored (immutable, status "blocked"):
    # 201 says a new candidate was created even though it cannot be confirmed.
    response.status_code = 201 if created else 200
    return MigrationResponse(
        fingerprint=migration.fingerprint,
        status=migration.status,
        created=created,
        plan=migration.plan,
    )


@router.get("", response_model=list[MigrationSummary])
def list_migrations(db: Session = Depends(get_db)):
    migrations = (
        db.query(Migration).order_by(Migration.created_at, Migration.id).all()
    )
    return [
        MigrationSummary(
            fingerprint=m.fingerprint,
            status=m.status,
            subjects=[s["subject"] for s in m.plan["subjects"]],
            conflicts=len(m.plan["conflicts"]),
            created_at=m.created_at,
            committed_at=m.committed_at,
        )
        for m in migrations
    ]


@router.get("/{fingerprint}", response_model=MigrationDetail)
def get_migration(fingerprint: str, db: Session = Depends(get_db)):
    return get_migration_or_404(db, fingerprint)


@router.post("/{fingerprint}/confirm", response_model=MigrationConfirmResponse)
def confirm(fingerprint: str, db: Session = Depends(get_db)):
    migration = get_migration_or_404(db, fingerprint)
    try:
        return confirm_migration(db, migration)
    except ReleaseConflictError as exc:
        raise HTTPException(status_code=409, detail=exc.detail) from exc
