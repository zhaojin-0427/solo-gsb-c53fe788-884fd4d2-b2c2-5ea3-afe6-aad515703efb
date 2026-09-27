"""Version registration, retrieval and dry-run compatibility checks."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy.orm import Session

from ..contract import SchemaError, parse_schema
from ..db import get_db
from ..models import Version
from ..schemas import (
    CheckRequest,
    CheckResponse,
    RegisterRequest,
    RegisterResponse,
    VersionDetail,
    VersionOut,
)
from ..service import (
    ConcurrentConflictError,
    IncompatibleSchemaError,
    evaluate_compatibility,
    register_version,
)
from .subjects import get_subject_or_404

router = APIRouter(prefix="/subjects/{name}", tags=["versions"])


@router.post("/versions", response_model=RegisterResponse)
def register(
    name: str,
    body: RegisterRequest,
    response: Response,
    db: Session = Depends(get_db),
):
    subject = get_subject_or_404(db, name)
    try:
        version, created = register_version(db, subject, body.content)
    except SchemaError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except IncompatibleSchemaError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "schema is not compatible under the subject's "
                f"{subject.compatibility} policy",
                "errors": exc.errors,
            },
        ) from exc
    except ConcurrentConflictError as exc:
        raise HTTPException(
            status_code=409, detail={"message": str(exc), "errors": []}
        ) from exc
    response.status_code = 201 if created else 200
    return RegisterResponse(
        subject=name,
        version=version.version,
        fingerprint=version.fingerprint,
        created=created,
    )


@router.get("/versions", response_model=list[VersionOut])
def list_versions(name: str, db: Session = Depends(get_db)):
    subject = get_subject_or_404(db, name)
    return subject.versions


@router.get("/versions/{version}", response_model=VersionDetail)
def get_version(name: str, version: str, db: Session = Depends(get_db)):
    subject = get_subject_or_404(db, name)
    query = db.query(Version).filter(Version.subject_id == subject.id)
    if version == "latest":
        row = query.order_by(Version.version.desc()).first()
    else:
        try:
            number = int(version)
        except ValueError:
            raise HTTPException(
                status_code=422, detail="version must be an integer or 'latest'"
            ) from None
        row = query.filter(Version.version == number).first()
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"version '{version}' not found for subject '{name}'",
        )
    return row


@router.post("/check", response_model=CheckResponse)
def check(name: str, body: CheckRequest, db: Session = Depends(get_db)):
    """Dry-run: would this content be accepted as the next version?"""
    subject = get_subject_or_404(db, name)
    try:
        schema = parse_schema(body.content)
    except SchemaError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    errors = evaluate_compatibility(subject, schema, db)
    return CheckResponse(is_compatible=not errors, errors=errors)
