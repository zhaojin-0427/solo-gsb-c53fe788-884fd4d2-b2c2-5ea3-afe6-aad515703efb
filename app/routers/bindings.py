"""Consumer binding and drift-impact endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy.orm import Session

from ..binding import (
    BindingBaselineMissing,
    BindingConflictError,
    BindingSubjectMissing,
    create_binding,
    get_impact,
)
from ..contract import SchemaError
from ..db import get_db
from ..models import ConsumerBinding
from ..schemas import (
    ConsumerBindingCreate,
    ConsumerBindingDetail,
    ConsumerBindingResponse,
    ImpactResponse,
)

router = APIRouter(prefix="/consumer-bindings", tags=["consumer-bindings"])


def get_binding_or_404(db: Session, binding_id: str) -> ConsumerBinding:
    binding = db.get(ConsumerBinding, binding_id)
    if binding is None:
        raise HTTPException(
            status_code=404, detail=f"consumer binding '{binding_id}' not found"
        )
    return binding


def _binding_response(binding: ConsumerBinding, created: bool) -> ConsumerBindingResponse:
    return ConsumerBindingResponse(
        binding_id=binding.id,
        idempotency_key=binding.idempotency_key,
        producer_subject=binding.producer_subject,
        baseline_version=binding.baseline_version,
        baseline_fingerprint=binding.baseline_fingerprint,
        bound_latest_version=binding.bound_latest_version,
        reader_fingerprint=binding.reader_fingerprint,
        status=binding.status,
        created=created,
    )


@router.post("", response_model=ConsumerBindingResponse)
def create(
    body: ConsumerBindingCreate, response: Response, db: Session = Depends(get_db)
):
    try:
        binding, created = create_binding(
            db,
            idempotency_key=body.idempotency_key,
            producer_subject=body.producer_subject,
            baseline_version=body.baseline_version,
            reader_content=body.reader,
        )
    except SchemaError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except (BindingSubjectMissing, BindingBaselineMissing) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except BindingConflictError as exc:
        raise HTTPException(status_code=409, detail=exc.detail) from exc
    response.status_code = 201 if created else 200
    return _binding_response(binding, created)


@router.get("/{binding_id}", response_model=ConsumerBindingDetail)
def get_binding(binding_id: str, db: Session = Depends(get_db)):
    binding = get_binding_or_404(db, binding_id)
    latest = binding.reports[-1] if binding.reports else None
    return ConsumerBindingDetail(
        binding_id=binding.id,
        idempotency_key=binding.idempotency_key,
        producer_subject=binding.producer_subject,
        baseline_version=binding.baseline_version,
        baseline_fingerprint=binding.baseline_fingerprint,
        bound_latest_version=binding.bound_latest_version,
        reader=binding.reader_content,
        reader_fingerprint=binding.reader_fingerprint,
        status=binding.status,
        reports=len(binding.reports),
        latest_revision=latest.revision if latest is not None else 0,
        created_at=binding.created_at,
    )


@router.get("/{binding_id}/impact", response_model=ImpactResponse)
def impact(
    binding_id: str,
    expected_revision: int | None = Query(
        default=None,
        ge=0,
        description=(
            "Optimistic CAS token: the latest report revision you observed "
            "(0 for none). A mismatch fails with 409 and stores nothing."
        ),
    ),
    db: Session = Depends(get_db),
):
    binding = get_binding_or_404(db, binding_id)
    try:
        report, created, revisions, producer_intact = get_impact(
            db, binding, expected_revision=expected_revision
        )
    except BindingConflictError as exc:
        raise HTTPException(status_code=409, detail=exc.detail) from exc
    return ImpactResponse(
        binding_id=binding.id,
        status=binding.status,
        revision=report.revision,
        revisions=revisions,
        created=created,
        producer_subject=binding.producer_subject,
        producer_intact=producer_intact,
        baseline_version=binding.baseline_version,
        producer_latest_version=report.producer_latest_version,
        result=report.result,
        created_at=report.created_at,
    )
