"""Pydantic request/response models for the HTTP API."""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Mode(str, Enum):
    BACKWARD = "BACKWARD"
    BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"
    FORWARD = "FORWARD"
    FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"
    FULL = "FULL"
    FULL_TRANSITIVE = "FULL_TRANSITIVE"


class SubjectCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    compatibility: Mode = Mode.BACKWARD


class ConfigUpdate(BaseModel):
    compatibility: Mode


class VersionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    version: int
    fingerprint: str
    created_at: datetime


class SubjectOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    compatibility: Mode
    created_at: datetime
    versions: list[VersionOut]


class SubjectSummary(BaseModel):
    name: str
    compatibility: Mode
    versions: int
    latest_version: int | None


class RegisterRequest(BaseModel):
    content: Any  # a schema document: object or primitive-type string


class CheckRequest(BaseModel):
    content: Any


class RegisterResponse(BaseModel):
    subject: str
    version: int
    fingerprint: str
    created: bool  # False when CAS returned an existing identical version


class CompatibilityError(BaseModel):
    version: int  # the historical version the candidate conflicts with
    direction: str  # "backward" (candidate reads old) or "forward"
    rule: str
    path: list[str]
    path_string: str
    message: str


class CheckResponse(BaseModel):
    is_compatible: bool
    errors: list[CompatibilityError]


class VersionDetail(VersionOut):
    content: Any
    canonical: str


# ---------------------------------------------------------------------------
# coordinated cross-subject releases
# ---------------------------------------------------------------------------


class ReleaseSubjectSpec(BaseModel):
    content: Any  # the contract this subject should publish next


class ReleaseEdge(BaseModel):
    producer: str = Field(min_length=1, max_length=255)
    consumer: str = Field(min_length=1, max_length=255)


class ReleaseCreate(BaseModel):
    subjects: dict[str, ReleaseSubjectSpec] = Field(min_length=1)
    edges: list[ReleaseEdge] = []

    @model_validator(mode="after")
    def edges_stay_within_the_release(self):
        for edge in self.edges:
            for endpoint in (edge.producer, edge.consumer):
                if endpoint not in self.subjects:
                    raise ValueError(
                        f"edge endpoint '{endpoint}' is not one of the "
                        "release subjects"
                    )
        return self


class PlanSubject(BaseModel):
    subject: str
    baseline_version: int  # latest version when the plan was built
    baseline_fingerprint: str | None  # fingerprint of that latest version (None if no history)
    target_version: int  # version this release will publish
    fingerprint: str
    compatibility: str


class PlanSubjectRef(BaseModel):
    subject: str
    target_version: int


class PlanEdge(BaseModel):
    producer: str
    consumer: str
    producer_version: int
    consumer_version: int


class PlanBatch(BaseModel):
    batch: int
    subjects: list[PlanSubjectRef]
    edges: list[PlanEdge]  # data-flow edges satisfied by this batch


class Precondition(BaseModel):
    subject: str
    baseline_version: int  # confirm fails unless latest still equals this
    baseline_fingerprint: str | None  # ...and its fingerprint still matches
    compatibility: str  # ...and the policy mode was not changed


class ReleasePlan(BaseModel):
    subjects: list[PlanSubject]
    batches: list[PlanBatch]
    edges: list[PlanEdge]
    preconditions: list[Precondition]


class ReleaseResponse(BaseModel):
    fingerprint: str
    status: str  # "planned" | "committed"
    created: bool  # False when the content hash matched a stored candidate
    plan: ReleasePlan


class ReleaseDetail(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    fingerprint: str
    status: str
    plan: ReleasePlan
    created_at: datetime
    committed_at: datetime | None


class ReleaseSummary(BaseModel):
    fingerprint: str
    status: str
    subjects: list[str]
    created_at: datetime
    committed_at: datetime | None


class ConfirmVersion(BaseModel):
    subject: str
    version: int
    fingerprint: str


class ConfirmResponse(BaseModel):
    fingerprint: str
    status: str
    versions: list[ConfirmVersion]


# ---------------------------------------------------------------------------
# compatibility-policy migrations
# ---------------------------------------------------------------------------


class MigrationSubjectSpec(BaseModel):
    to_compatibility: Mode


class MigrationCreate(BaseModel):
    # subject name -> target compatibility policy to migrate to
    subjects: dict[str, MigrationSubjectSpec] = Field(min_length=1)


class MigrationConflictPair(BaseModel):
    subject: str
    from_version: int  # the earlier (historical) version of the pair
    to_version: int  # the later (replayed) version of the pair
    direction: str  # "backward" (to reads from) or "forward" (from reads to)
    rule: str
    path: list[str]
    path_string: str
    message: str


class MigrationPlanSubject(BaseModel):
    subject: str
    baseline_version: int  # latest version when the migration was planned
    baseline_fingerprint: str | None  # fingerprint of that latest version
    from_compatibility: str  # the policy in force when the plan was built
    to_compatibility: str  # the policy the migration switches the subject to


class MigrationPrecondition(BaseModel):
    subject: str
    baseline_version: int  # confirm fails unless latest still equals this
    baseline_fingerprint: str | None  # ...and its fingerprint still matches
    compatibility: str  # ...and the policy was not changed (still == from mode)


class MigrationPlan(BaseModel):
    subjects: list[MigrationPlanSubject]
    conflicts: list[MigrationConflictPair]  # newly-introduced conflicting pairs
    preconditions: list[MigrationPrecondition]


class MigrationResponse(BaseModel):
    fingerprint: str
    status: str  # "planned" | "blocked" | "committed"
    created: bool  # False when the content hash matched a stored candidate
    plan: MigrationPlan


class MigrationDetail(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    fingerprint: str
    status: str
    plan: MigrationPlan
    created_at: datetime
    committed_at: datetime | None


class MigrationSummary(BaseModel):
    fingerprint: str
    status: str
    subjects: list[str]
    conflicts: int
    created_at: datetime
    committed_at: datetime | None


class MigrationConfirmResponse(BaseModel):
    fingerprint: str
    status: str
    subjects: list[MigrationPlanSubject]


# ---------------------------------------------------------------------------
# consumer bindings & drift-impact reports
# ---------------------------------------------------------------------------


class ConsumerBindingCreate(BaseModel):
    producer_subject: str = Field(min_length=1, max_length=255)
    baseline_version: int = Field(ge=1)  # producer version the consumer reads as of
    reader: Any  # the consumer's reader contract (a schema document)
    idempotency_key: str = Field(min_length=1, max_length=255)


class ConsumerBindingResponse(BaseModel):
    binding_id: str
    idempotency_key: str
    producer_subject: str
    baseline_version: int
    baseline_fingerprint: str
    bound_latest_version: int  # latest producer version when the binding was stored
    reader_fingerprint: str
    status: str  # "active" | "impacted"
    created: bool  # False when the idempotency key replayed a stored binding


class ConsumerBindingDetail(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    binding_id: str
    idempotency_key: str
    producer_subject: str
    baseline_version: int
    baseline_fingerprint: str
    bound_latest_version: int
    reader: Any  # the reader contract as submitted
    reader_fingerprint: str
    status: str
    reports: int  # stored impact-report revisions
    latest_revision: int  # 0 when no report was ever generated
    created_at: datetime


class AffectedVersion(BaseModel):
    version: int  # producer version the reader contract cannot read
    fingerprint: str
    rule: str
    path: list[str]  # shortest violating field path
    path_string: str
    message: str


class ImpactResult(BaseModel):
    checked_versions: list[int]  # every producer version after the baseline, in order
    affected_versions: list[AffectedVersion]  # the ones the reader cannot read
    first_impacted_version: int | None  # earliest affected version, if any


class ImpactResponse(BaseModel):
    binding_id: str
    status: str  # binding status after this evaluation
    revision: int  # the report revision served
    revisions: int  # total stored revisions for this binding
    created: bool  # False when an existing revision covers the current snapshot
    producer_subject: str
    producer_intact: bool  # producer still carries the bound baseline version
    baseline_version: int
    producer_latest_version: int  # snapshot this revision was computed on
    result: ImpactResult
    created_at: datetime


# ---------------------------------------------------------------------------
# event subscriptions & replay
# ---------------------------------------------------------------------------


class EventType(str, Enum):
    VERSION_REGISTERED = "version_registered"
    COMPATIBILITY_CHANGED = "compatibility_changed"
    RELEASE_COMMITTED = "release_committed"
    MIGRATION_COMMITTED = "migration_committed"
    CONSUMER_BINDING_CREATED = "consumer_binding_created"
    IMPACT_REPORT_CREATED = "impact_report_created"


class EventSubscriptionCreate(BaseModel):
    subjects: list[str] = Field(default_factory=list)  # empty = all subjects
    event_types: list[EventType] = Field(default_factory=list)  # empty = all types


class EventSubscriptionOut(BaseModel):
    subscription_id: str
    subjects: list[str]  # the stored subject filter ([] = all)
    event_types: list[str]  # the stored event-type filter ([] = all)
    checkpoint: int  # last sequence the client confirmed consuming
    created_at: datetime


class EventOut(BaseModel):
    seq: int  # global monotonic sequence (commit order, gapless)
    type: str
    subject: str | None  # subject name (None only for non-subject events)
    version: int | None  # version the event is about, if any
    fingerprint: str | None  # digest the event is about, if any
    payload: dict[str, Any]  # normalised event document
    recorded_at: datetime


class EventPage(BaseModel):
    subscription_id: str
    after_seq: int  # cursor this page was read from
    snapshot_seq: int  # committed-sequence cutoff this page was read against
    events: list[EventOut]
    next_after_seq: int  # cursor for the next page
    has_more: bool  # more matching events exist with seq <= snapshot_seq


class CheckpointUpdate(BaseModel):
    seq: int = Field(ge=0)  # 0 rewinds the subscription for a full replay
