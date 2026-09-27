"""Subject management endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy.orm import Session

from ..db import get_db
from ..events import COMPATIBILITY_CHANGED, emit_event
from ..models import Subject
from ..schemas import ConfigUpdate, SubjectCreate, SubjectOut, SubjectSummary

router = APIRouter(prefix="/subjects", tags=["subjects"])


def get_subject_or_404(db: Session, name: str) -> Subject:
    subject = db.query(Subject).filter(Subject.name == name).first()
    if subject is None:
        raise HTTPException(status_code=404, detail=f"subject '{name}' not found")
    return subject


@router.post("", status_code=201, response_model=SubjectOut)
def create_subject(body: SubjectCreate, db: Session = Depends(get_db)):
    if db.query(Subject).filter(Subject.name == body.name).first() is not None:
        raise HTTPException(
            status_code=409, detail=f"subject '{body.name}' already exists"
        )
    subject = Subject(name=body.name, compatibility=body.compatibility.value)
    db.add(subject)
    db.commit()
    db.refresh(subject)
    return subject


@router.get("", response_model=list[SubjectSummary])
def list_subjects(db: Session = Depends(get_db)):
    subjects = db.query(Subject).order_by(Subject.name).all()
    return [
        SubjectSummary(
            name=s.name,
            compatibility=s.compatibility,
            versions=len(s.versions),
            latest_version=s.versions[-1].version if s.versions else None,
        )
        for s in subjects
    ]


@router.get("/{name}", response_model=SubjectOut)
def get_subject(name: str, db: Session = Depends(get_db)):
    return get_subject_or_404(db, name)


@router.put("/{name}/config", response_model=SubjectOut)
def update_config(name: str, body: ConfigUpdate, db: Session = Depends(get_db)):
    subject = get_subject_or_404(db, name)
    old_mode = subject.compatibility
    new_mode = body.compatibility.value
    if new_mode != old_mode:
        subject.compatibility = new_mode
        db.flush()
        # same transaction as the config write; a rollback drops the event
        emit_event(
            db,
            COMPATIBILITY_CHANGED,
            subject=name,
            version=None,
            fingerprint=None,
            payload={
                "subject": name,
                "from_compatibility": old_mode,
                "to_compatibility": new_mode,
            },
        )
    db.commit()
    db.refresh(subject)
    return subject


@router.delete("/{name}", status_code=204)
def delete_subject(name: str, db: Session = Depends(get_db)):
    subject = get_subject_or_404(db, name)
    db.delete(subject)
    db.commit()
    return Response(status_code=204)
