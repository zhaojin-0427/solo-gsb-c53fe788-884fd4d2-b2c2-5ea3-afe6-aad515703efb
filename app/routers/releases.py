"""Coordinated cross-subject release endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy.orm import Session

from ..contract import SchemaError
from ..db import get_db
from ..models import Release
from ..release import (
    ReleaseConflictError,
    ReleaseSubjectMissing,
    confirm_release,
    create_release,
)
from ..schemas import (
    ConfirmResponse,
    ReleaseCreate,
    ReleaseDetail,
    ReleaseResponse,
    ReleaseSummary,
)

router = APIRouter(prefix="/releases", tags=["releases"])


def get_release_or_404(db: Session, fingerprint: str) -> Release:
    release = db.query(Release).filter(Release.fingerprint == fingerprint).first()
    if release is None:
        raise HTTPException(
            status_code=404, detail=f"release '{fingerprint}' not found"
        )
    return release


@router.post("", response_model=ReleaseResponse)
def create_candidate(
    body: ReleaseCreate, response: Response, db: Session = Depends(get_db)
):
    contents = {name: spec.content for name, spec in body.subjects.items()}
    edges = [(edge.producer, edge.consumer) for edge in body.edges]
    try:
        release, created = create_release(db, contents, edges)
    except SchemaError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except ReleaseSubjectMissing as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ReleaseConflictError as exc:
        raise HTTPException(status_code=409, detail=exc.detail) from exc
    response.status_code = 201 if created else 200
    return ReleaseResponse(
        fingerprint=release.fingerprint,
        status=release.status,
        created=created,
        plan=release.plan,
    )


@router.get("", response_model=list[ReleaseSummary])
def list_releases(db: Session = Depends(get_db)):
    releases = db.query(Release).order_by(Release.created_at, Release.id).all()
    return [
        ReleaseSummary(
            fingerprint=r.fingerprint,
            status=r.status,
            subjects=[s["subject"] for s in r.plan["subjects"]],
            created_at=r.created_at,
            committed_at=r.committed_at,
        )
        for r in releases
    ]


@router.get("/{fingerprint}", response_model=ReleaseDetail)
def get_release(fingerprint: str, db: Session = Depends(get_db)):
    return get_release_or_404(db, fingerprint)


@router.post("/{fingerprint}/confirm", response_model=ConfirmResponse)
def confirm(fingerprint: str, db: Session = Depends(get_db)):
    release = get_release_or_404(db, fingerprint)
    try:
        return confirm_release(db, release)
    except ReleaseConflictError as exc:
        raise HTTPException(status_code=409, detail=exc.detail) from exc
