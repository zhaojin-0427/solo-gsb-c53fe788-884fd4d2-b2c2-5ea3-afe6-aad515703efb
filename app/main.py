"""Recursive data contract evolution registry — HTTP API."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from .db import init_db
from .routers import bindings, events, migrations, releases, subjects, versions


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="Recursive Data Contract Evolution Registry",
    version="1.4.0",
    description=(
        "Content-addressed, immutable contract versions with recursive "
        "reader/writer compatibility checking (BACKWARD / FORWARD / FULL, "
        "optionally TRANSITIVE), coordinated cross-subject releases planned "
        "against one history snapshot and committed by global CAS, "
        "compatibility-policy migrations that replay each subject's history "
        "in version order under a target policy and atomically switch the "
        "policies of one or more subjects, immutable consumer bindings "
        "whose drift-impact reports replay the reader contract against "
        "every producer version after the bound baseline, and an immutable "
        "domain-event log with a global monotonic sequence, subject/type "
        "subscriptions and cursor-based checkpoint replay."
    ),
    lifespan=lifespan,
)
app.include_router(subjects.router)
app.include_router(versions.router)
app.include_router(releases.router)
app.include_router(migrations.router)
app.include_router(bindings.router)
app.include_router(events.router)


@app.get("/", tags=["meta"])
def root():
    return {
        "service": "contract-registry",
        "docs": "/docs",
        "health": "/health",
    }


@app.get("/health", tags=["meta"])
def health():
    return {"status": "ok"}
