"""FastAPI application for cade."""
import logging
from datetime import date
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Response
from pydantic import BaseModel

from cade.backends.parquet import ParquetBackend
from cade.exceptions import (
    DuplicateSnapshotError,
    IntegrityError,
    InvalidRangeError,
    MatrixReferenceError,
    SnapshotNotFound,
)
from cade.models import COBSnapshot, ExposureSummary, SnapshotDiff

logger = logging.getLogger(__name__)

app = FastAPI(title="cade", description="Counterparty Agreement Data Engine")

# Backend is configured via DATA_DIR env var, default ./data
import os
_DATA_DIR = os.environ.get("CADE_DATA_DIR", "./data")
_repo = ParquetBackend(_DATA_DIR)


def _get_repo() -> ParquetBackend:
    return _repo


# ── health ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "data_dir": str(_DATA_DIR)}


# ── ingest ────────────────────────────────────────────────────────────────────

class IngestRequest(BaseModel):
    snapshot: COBSnapshot
    exposure_total: float = 0.0


@app.post("/ingest", response_model=COBSnapshot, status_code=201)
def ingest(body: IngestRequest):
    repo = _get_repo()
    try:
        return repo.store_snapshot(body.snapshot, body.exposure_total)
    except DuplicateSnapshotError as e:
        raise HTTPException(409, detail=str(e))
    except MatrixReferenceError as e:
        raise HTTPException(400, detail=str(e))
    except ValueError as e:
        raise HTTPException(400, detail=str(e))


# ── diff ──────────────────────────────────────────────────────────────────────
# NOTE: this route must be registered BEFORE the {cob_date} snapshot route so
# that FastAPI does not attempt to parse the literal path segment "diff" as a
# date value.

@app.get(
    "/counterparties/{counterparty_id}/agreements/{netting_set_id}/diff",
    response_model=SnapshotDiff,
)
def get_diff(
    counterparty_id: str,
    netting_set_id: str,
    from_date: date = Query(...),
    to_date: date = Query(...),
):
    repo = _get_repo()
    try:
        return repo.get_diff(counterparty_id, netting_set_id, from_date, to_date)
    except InvalidRangeError as e:
        raise HTTPException(400, detail=str(e))
    except SnapshotNotFound as e:
        raise HTTPException(404, detail=str(e))


# ── snapshots ─────────────────────────────────────────────────────────────────

@app.get(
    "/counterparties/{counterparty_id}/agreements/{netting_set_id}/{cob_date}",
    response_model=COBSnapshot,
)
def get_snapshot(counterparty_id: str, netting_set_id: str, cob_date: date):
    repo = _get_repo()
    try:
        return repo.get_snapshot(counterparty_id, netting_set_id, cob_date)
    except SnapshotNotFound as e:
        raise HTTPException(404, detail=str(e))
    except IntegrityError as e:
        logger.critical("integrity_failure cp=%s ns=%s date=%s", counterparty_id, netting_set_id, cob_date, exc_info=e)
        raise HTTPException(500, detail="Data integrity check failed — contact admin")


@app.get(
    "/counterparties/{counterparty_id}/agreements/{netting_set_id}",
    response_model=list[date],
)
def list_cob_dates(counterparty_id: str, netting_set_id: str):
    return _get_repo().list_cob_dates(counterparty_id, netting_set_id)


@app.get(
    "/counterparties/{counterparty_id}/agreements",
    response_model=list[str],
)
def list_netting_sets(counterparty_id: str):
    return _get_repo().list_netting_sets(counterparty_id)


# ── portfolio ─────────────────────────────────────────────────────────────────

@app.get("/portfolio/exposure", response_model=list[ExposureSummary])
def get_portfolio(
    response: Response,
    cob_date: date = Query(...),
    threshold: float | None = Query(default=None),
    top_n: int | None = Query(default=None, ge=1),
):
    summaries, stale = _get_repo().get_portfolio(cob_date, threshold=threshold, top_n=top_n)
    if stale:
        response.headers["X-Index-Stale"] = "true"
    return summaries
