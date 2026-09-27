"""ORM models.

Versions are immutable and content-addressed:

* ``(subject_id, version)`` is unique — this is what makes two concurrent
  registrations on the same baseline fail for exactly one of them;
* ``(subject_id, fingerprint)`` is unique — re-publishing identical
  canonical content returns the existing version (CAS idempotency).

Releases are cross-subject coordinated publish candidates, likewise
immutable and content-addressed (unique ``fingerprint``). The fingerprint
covers the target contracts, the producer→consumer edge set **and** the
per-subject history snapshot (latest version, its fingerprint, the
compatibility mode), so a resubmission after any participating subject
drifts becomes a new candidate rather than reusing a stale plan.

Consumer bindings tie a reader contract to a producer subject baseline and
are immutable and content-addressed as well (unique ``idempotency_key``,
content-hash primary key). Their impact reports are append-only revisions
(unique ``(binding_id, revision)``): a moved producer snapshot yields a new
revision, never an edit, and deleting or modifying the subject leaves the
stored history untouched.

Domain events are an append-only log with a global, gapless sequence:
every state-changing write path appends its events in the **same
transaction** as the business write, so a rolled-back change leaves no
events and a committed one never loses them. The sequence comes from a
singleton counter row incremented with ``UPDATE ... RETURNING`` — the row
lock is held until commit, so sequence numbers reflect commit order and a
reader that cuts off at the current counter value (``snapshot_seq``) sees
a consistent prefix. Events reference subjects **by name** (never by
foreign key), so deleting or recreating a subject preserves its history.
"""
from __future__ import annotations

from sqlalchemy import (
    JSON,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

JsonDoc = JSON().with_variant(JSONB(), "postgresql")


class Base(DeclarativeBase):
    pass


class Subject(Base):
    __tablename__ = "subjects"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    compatibility: Mapped[str] = mapped_column(
        String(32), nullable=False, default="BACKWARD"
    )
    created_at: Mapped[object] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    versions: Mapped[list["Version"]] = relationship(
        back_populates="subject",
        cascade="all, delete-orphan",
        order_by="Version.version",
    )


class Version(Base):
    __tablename__ = "versions"
    __table_args__ = (
        UniqueConstraint("subject_id", "version", name="uq_versions_subject_version"),
        UniqueConstraint("subject_id", "fingerprint", name="uq_versions_subject_fingerprint"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    subject_id: Mapped[int] = mapped_column(
        ForeignKey("subjects.id", ondelete="CASCADE"), nullable=False
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    content: Mapped[object] = mapped_column(JsonDoc, nullable=False)  # as submitted
    canonical: Mapped[str] = mapped_column(Text, nullable=False)  # canonical form
    created_at: Mapped[object] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    subject: Mapped[Subject] = relationship(back_populates="versions")


class Release(Base):
    """Immutable, content-addressed cross-subject release candidate.

    * ``fingerprint`` is unique — re-submitting a semantically identical
      candidate returns the stored one (CAS idempotency);
    * ``status`` moves ``planned`` -> ``committed`` exactly once, via the
      global compare-and-swap in ``app.release.confirm_release``;
    * ``request`` keeps the canonicalised submission (needed to materialise
      the versions at confirm time); ``plan`` is the deterministic rollout
      plan computed from the history snapshot.
    """

    __tablename__ = "releases"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    fingerprint: Mapped[str] = mapped_column(
        String(64), unique=True, nullable=False, index=True
    )
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default="planned"
    )
    request: Mapped[object] = mapped_column(JsonDoc, nullable=False)
    plan: Mapped[object] = mapped_column(JsonDoc, nullable=False)
    created_at: Mapped[object] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    committed_at: Mapped[object] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class ConsumerBinding(Base):
    """Immutable consumer binding to a producer subject baseline.

    A consumer declares: *I read subject ``producer_subject`` as of
    ``baseline_version`` with the reader contract ``reader_content``*. The
    binding is validated and stored against one history snapshot and never
    rewritten afterwards:

    * ``id`` is a content hash over (idempotency key, producer subject,
      baseline version + fingerprint, canonical reader contract) — the same
      submission always maps to the same binding;
    * ``idempotency_key`` is unique — replaying the same key with the same
      content returns the stored binding, with different content it
      conflicts (409);
    * the producer is referenced **by name**, not by foreign key, so
      deleting or recreating the subject never rewrites or cascades into
      stored bindings or their impact reports;
    * ``status`` starts ``active`` and flips to ``impacted`` exactly once,
      when an impact report first finds a producer version the reader
      contract cannot read.
    """

    __tablename__ = "consumer_bindings"
    __table_args__ = (
        UniqueConstraint(
            "idempotency_key", name="uq_consumer_bindings_idempotency_key"
        ),
    )

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    producer_subject: Mapped[str] = mapped_column(
        String(255), nullable=False, index=True
    )
    baseline_version: Mapped[int] = mapped_column(Integer, nullable=False)
    baseline_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    # latest producer version observed when the binding was created
    bound_latest_version: Mapped[int] = mapped_column(Integer, nullable=False)
    reader_content: Mapped[object] = mapped_column(JsonDoc, nullable=False)
    reader_canonical: Mapped[str] = mapped_column(Text, nullable=False)
    reader_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="active")
    created_at: Mapped[object] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    reports: Mapped[list["ImpactReport"]] = relationship(
        back_populates="binding",
        cascade="all, delete-orphan",
        order_by="ImpactReport.revision",
    )


class ImpactReport(Base):
    """Immutable impact-report revision for a consumer binding.

    Each revision replays the binding's reader contract, in version order,
    against every producer version after the baseline, computed on the
    producer snapshot ``(producer_latest_version, producer_latest_fingerprint)``:

    * ``(binding_id, revision)`` is unique — revisions are assigned
      monotonically and the constraint is the CAS guard that makes two
      concurrent report generations converge instead of overwriting;
    * revisions are immutable: a moved snapshot produces a **new** revision,
      never an edit; deleting or modifying the subject leaves every stored
      revision untouched.
    """

    __tablename__ = "impact_reports"
    __table_args__ = (
        UniqueConstraint("binding_id", "revision", name="uq_impact_reports_binding_revision"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    binding_id: Mapped[str] = mapped_column(
        ForeignKey("consumer_bindings.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    revision: Mapped[int] = mapped_column(Integer, nullable=False)
    # producer snapshot this revision was computed on
    producer_latest_version: Mapped[int] = mapped_column(Integer, nullable=False)
    producer_latest_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    result: Mapped[object] = mapped_column(JsonDoc, nullable=False)
    created_at: Mapped[object] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    binding: Mapped[ConsumerBinding] = relationship(back_populates="reports")


class Migration(Base):
    """Immutable, content-addressed compatibility-policy migration candidate.

    Bundles target compatibility policies for one or more subjects. The
    plan is computed by replaying each subject's history **in version order
    under the target policy** (immediate predecessor for plain modes, every
    predecessor for ``*_TRANSITIVE`` modes) and recording the conflicting
    version pairs.

    * ``fingerprint`` is unique and covers the target policies **and** the
      per-subject history snapshot (latest version, its fingerprint, the
      current policy), so resubmission on a moved snapshot yields a new
      candidate;
    * ``status`` is ``planned`` (replay found no conflicts) or ``blocked``
      (replay found conflicts; the candidate is stored for inspection but
      can never be confirmed), and moves ``planned`` -> ``committed``
      exactly once via the global compare-and-swap in
      ``app.migration.confirm_migration``;
    * ``request`` keeps the from/to policy per subject; ``plan`` keeps the
      snapshot preconditions and the replayed conflict pairs.
    """

    __tablename__ = "migrations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    fingerprint: Mapped[str] = mapped_column(
        String(64), unique=True, nullable=False, index=True
    )
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default="planned"
    )
    request: Mapped[object] = mapped_column(JsonDoc, nullable=False)
    plan: Mapped[object] = mapped_column(JsonDoc, nullable=False)
    created_at: Mapped[object] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    committed_at: Mapped[object] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class EventCounter(Base):
    """Singleton row (``id == 1``) handing out the global event sequence.

    Emitters increment the row with ``UPDATE ... RETURNING`` inside the
    business transaction; the row lock is held until that transaction
    commits, so

    * sequence numbers are assigned in **commit order** — a reader that
      cuts off at the current counter value never sees a later event
      while an earlier one is still invisible;
    * a rolled-back transaction rolls its increment back too — the
      sequence stays **gapless** (no event lost, none duplicated).
    """

    __tablename__ = "event_counter"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    value: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class Event(Base):
    """Immutable domain event, appended in the business transaction.

    * ``seq`` is the global monotonic sequence (see :class:`EventCounter`);
    * ``subject`` is the subject **name** — never a foreign key — so the
      history of a deleted or recreated subject is preserved;
    * ``version`` / ``fingerprint`` carry the version number or digest the
      event is about (content fingerprint for versions, candidate
      fingerprint for releases/migrations, binding id for bindings);
    * ``payload`` is the normalised (canonical JSON: sorted keys, compact
      separators) event document, so repeated reads are byte-identical.
    """

    __tablename__ = "events"

    seq: Mapped[int] = mapped_column(Integer, primary_key=True)
    type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    subject: Mapped[str | None] = mapped_column(
        String(255), nullable=True, index=True
    )
    version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)
    payload: Mapped[str] = mapped_column(Text, nullable=False)  # canonical JSON
    recorded_at: Mapped[object] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class EventSubscription(Base):
    """A client's replay cursor over the event log.

    ``subjects`` / ``event_types`` are filter lists (empty = match all).
    Subscriptions reference subjects by name and never by foreign key, so
    a subscription keeps replaying the history of deleted subjects.
    ``checkpoint`` is the last sequence the client confirmed consuming;
    reads default to it, which is what makes sync resumable.
    """

    __tablename__ = "event_subscriptions"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    subjects: Mapped[object] = mapped_column(JsonDoc, nullable=False)  # [] = all
    event_types: Mapped[object] = mapped_column(JsonDoc, nullable=False)  # [] = all
    checkpoint: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[object] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
