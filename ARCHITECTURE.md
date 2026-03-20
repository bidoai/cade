# cade — Architecture and Design Decisions

This document explains the reasoning behind the key design choices in cade.
It is intended for engineers extending the codebase or evaluating a migration.

---

## Contents

- [System overview](#system-overview)
- [Storage design](#storage-design)
- [Integrity and reproducibility](#integrity-and-reproducibility)
- [Domain model](#domain-model)
- [Repository pattern](#repository-pattern)
- [API design](#api-design)
- [Diff design](#diff-design)
- [Security](#security)
- [What was deliberately left out](#what-was-deliberately-left-out)
- [Future directions](#future-directions)

---

## System overview

```
┌──────────────────────────────────────────────────────────────────┐
│                      API Layer  (FastAPI)                         │
│                                                                   │
│  POST /ingest                                                     │
│  GET  /counterparties/{id}/agreements/{ns}/{date}                 │
│  GET  /counterparties/{id}/agreements/{ns}/diff?from=&to=         │
│  GET  /counterparties/{id}/agreements/{ns}                        │
│  GET  /counterparties/{id}/agreements                             │
│  GET  /portfolio/exposure?date=&threshold=&top_n=                 │
│  GET  /health                                                     │
└─────────────────────────────┬─────────────────────────────────────┘
                              │
┌─────────────────────────────▼─────────────────────────────────────┐
│                      Domain Layer  (Pydantic)                      │
│                                                                   │
│  COBSnapshot                                                       │
│    ├── counterparty_id, netting_set_id, cob_date                  │
│    ├── ISDAgreement  (CSA terms)                                   │
│    ├── list[TradePosition]                                         │
│    ├── MarketDataSet                                               │
│    │     ├── fx_rates, inflation_rates                             │
│    │     └── price_matrices  →  dict[str, MatrixRef]              │
│    └── data_hash  (set on write, verified on read)                │
│                                                                   │
│  SnapshotDiff, ExposureSummary                                     │
└─────────────────────────────┬─────────────────────────────────────┘
                              │
┌─────────────────────────────▼─────────────────────────────────────┐
│               Storage Interface  (AgreementRepository ABC)         │
│                                                                   │
│  store_snapshot()  get_snapshot()  list_cob_dates()               │
│  list_netting_sets()  get_portfolio()  get_diff()                  │
└──────────┬───────────────────────────────────────────────────────┘
           │
┌──────────▼──────────┐        ┌─────────────────────────────────┐
│   ParquetBackend    │        │  TimescaleBackend  (planned v2)  │
│   (current)         │        │  IcebergBackend    (planned v3)  │
└─────────────────────┘        └─────────────────────────────────┘
           │
┌──────────▼──────────────────────────────────────────────────────┐
│                   Data directory  (CADE_DATA_DIR)                 │
│                                                                   │
│  snapshots/                                                       │
│    {counterparty_id}/                                             │
│      {netting_set_id}/                                            │
│        {cob_date}.parquet                                         │
│  index/                                                           │
│    {cob_date}.parquet     ← portfolio index                       │
│  matrices/                                                        │
│    {cob_date}/{counterparty_id}/{instrument}.*                    │
└──────────────────────────────────────────────────────────────────┘
```

---

## Storage design

### Why Parquet?

COB snapshots are immutable once written. Parquet is a natural fit:

- Files can be written once and never modified, which matches the
  snapshot contract
- pyarrow reads Parquet very fast — a single-row file (one snapshot) is
  read in well under 100ms
- Files are self-describing (schema embedded), portable, and human-inspectable
  with DuckDB or any Parquet reader
- No database process to manage — the data directory is the database

### Partition strategy: per-counterparty, per-netting-set, per-date

```
snapshots/ACME-CORP/RATES-USD/2024-03-15.parquet
snapshots/ACME-CORP/RATES-USD/2024-03-16.parquet
snapshots/ACME-CORP/FX-EUR/2024-03-15.parquet
```

The primary access pattern is "give me everything for this netting set on
this date." This partition maps that query to a single file read with no
scanning.

Alternative considered: one large file per COB date containing all
counterparties. This is faster for portfolio-level queries but means reading
megabytes of data to retrieve one counterparty's snapshot. Rejected in favour
of the portfolio index approach (below).

### Portfolio index

Portfolio queries (`who-matters`) need exposure totals for all counterparties
on a given date. Scanning per-counterparty files would require O(N) file opens.

Instead, a lightweight index file is written at ingestion time:

```
index/2024-03-15.parquet
  columns: counterparty_id, netting_set_id, cob_date, exposure_total,
           snapshot_path, ingested_at
```

`who-matters` reads one index file and returns. The index is fast because it
contains only scalars — no nested objects.

**Index staleness:** If a snapshot is ingested after the index was last written
(which should not happen with the normal write path but can occur if files are
manually copied), the `get_portfolio()` method detects this by comparing file
modification times and sets `index_stale = True`. The API surfaces this as an
`X-Index-Stale: true` response header.

### Nested object serialisation

Parquet is columnar and does not natively store arbitrary Python objects. The
three nested fields (`agreement`, `trades`, `market_data`) are serialised to
JSON strings and stored as `TEXT` columns.

```
agreement_json   TEXT   ← ISDAgreement as JSON
trades_json      TEXT   ← list[TradePosition] as JSON
market_data_json TEXT   ← MarketDataSet as JSON (matrix paths, not inline data)
```

The trade-off: DuckDB can still query into these columns using JSON functions,
but there is no columnar compression benefit on the nested fields. This is
acceptable for v1. The planned migration path is to Arrow nested
structs/`List[Struct]`, which Parquet supports natively and is the format
expected by Apache Iceberg.

### Price matrix references, not inline data

Price matrices (Monte Carlo scenario grids) are large — a typical grid is
thousands of scenarios × dozens of time steps × multiple instruments. Storing
them inline in the snapshot JSON would make the primary read endpoint return
tens of megabytes even when the caller only needs agreement terms or trade data.

Instead, matrices are stored as separate files and referenced from the snapshot:

```json
"price_matrices": {
  "IR_SWAP": {
    "path": "matrices/2024-03-15/ACME-CORP/IR_SWAP.npy",
    "hash": "sha256-v1:a3f8c2..."
  }
}
```

The snapshot endpoint returns the reference (fast). An `?include_matrices=true`
query parameter (planned) fetches the actual file contents. Each matrix file
has its own hash, allowing independent integrity verification.

---

## Integrity and reproducibility

### The problem

Counterparty risk investigations often happen weeks or months after the event.
A regulator may ask: "What was the exposure to ACME Corp on 15 March 2024?"
The answer must be identical whether it is produced today or in three years.
Any change to the stored data — accidental corruption, a silent migration, a
manual edit — must be detectable.

### The solution: data_hash

Every snapshot carries a `data_hash` field computed at ingestion:

```
data_hash = "sha256-v1:" + sha256(canonical_json(all_fields_except_data_hash))
```

On every read, cade recomputes the hash from the stored data and compares it
to the stored value. A mismatch raises `IntegrityError` before the data is
returned to the caller.

### Canonical JSON

SHA-256 is deterministic given identical input. The challenge is ensuring the
JSON serialisation is identical across invocations, Python versions, and
machines.

Three rules are applied:

1. **Sorted keys.** `json.dumps(..., sort_keys=True)` — dict key order is not
   guaranteed in all JSON serialisers, but it is after this step.

2. **Float rounding to 10 decimal places.** `json.dumps(0.1)` can produce
   `"0.1"` or `"0.10000000000000001"` depending on the Python version and
   platform. Rounding to 10 decimal places before serialising eliminates this
   variance. 10dp is more than sufficient precision for FX rates and inflation
   indices.

3. **NaN and Inf rejection.** These values have no canonical JSON
   representation. A `ValueError` is raised at ingestion time if any float
   field contains `NaN` or `Inf`. This prevents silent non-determinism.

### Algorithm versioning

The hash prefix `sha256-v1:` embeds the algorithm version. If the
canonicalisation rules ever change (e.g. migrating to SHA-3, or changing float
precision), the prefix changes to `sha256-v2:` and old snapshots remain
verifiable using the v1 rules. Without versioning, a hash algorithm change
would silently break verification of all historical snapshots.

### Matrix file integrity

The snapshot hash covers the matrix *references* (path and hash strings), not
the matrix file contents. The per-matrix hash covers the file bytes:

```
MatrixRef.hash = "sha256-v1:" + sha256(file_bytes)
```

This gives a two-level integrity chain:
- The snapshot hash proves the references (and all other fields) are unchanged
- The matrix hash proves each matrix file is unchanged

### What data_hash does not guarantee

- **Correctness at ingestion.** If bad data was ingested on day one, the hash
  preserves the bad data faithfully. cade stores what it is given.
- **Tamper prevention.** Someone with write access to the data directory can
  delete a snapshot and re-ingest with a freshly computed hash. For tamper
  prevention (not just detection), deploy on write-once storage (e.g.
  S3 with object lock, WORM filesystem).

---

## Domain model

### 1:N netting sets per counterparty

A single counterparty typically has multiple ISDA netting sets — one per
asset class or currency. The data model is:

```
Counterparty (string ID)
  └── 1:N NettingSet (string ID)
        └── 1 ISDAgreement per COB date
        └── N TradePosition per COB date
```

Building this correctly from day one avoids a painful migration when the first
counterparty with multiple CSAs appears.

### `extra` fields on ISDAgreement and TradePosition

Both models carry an `extra: dict[str, Any]` field for non-standard terms and
attributes. CSA terms vary significantly across counterparties and jurisdictions.
Rather than adding columns for every possible variant, uncommon fields go into
`extra`. This keeps the core schema clean while remaining extensible.

### cade does not compute exposure

`COBSnapshot` contains everything needed to compute bilateral margined exposure,
but cade does not perform the computation. The caller provides `exposure_total`
at ingestion time.

**Why:** Exposure computation is the responsibility of the risk engine upstream.
It depends on methodology choices (MtM vs FV, collateral haircuts, netting
assumptions) that cade does not own. Separating data storage from computation
keeps cade's scope clean and avoids coupling it to a specific exposure model.

The portfolio index stores the provided `exposure_total` for use in `who-matters`
queries. The full snapshot data is available for callers who want to recompute
exposure using the stored inputs.

---

## Repository pattern

`AgreementRepository` is an abstract base class defining the storage contract.
`ParquetBackend` implements it. A planned `TimescaleBackend` will implement the
same interface.

```python
class AgreementRepository(ABC):
    def store_snapshot(self, snapshot, exposure_total) -> COBSnapshot: ...
    def get_snapshot(self, counterparty_id, netting_set_id, cob_date) -> COBSnapshot: ...
    def list_cob_dates(self, counterparty_id, netting_set_id) -> list[date]: ...
    def list_netting_sets(self, counterparty_id) -> list[str]: ...
    def get_portfolio(self, cob_date, threshold, top_n) -> tuple[list, bool]: ...
    def get_diff(self, ...) -> SnapshotDiff: ...  # concrete, calls get_snapshot()
```

`get_diff` is implemented in the base class (not abstract) because it is
expressed entirely in terms of `get_snapshot()`. Every backend gets the diff
implementation for free, and all backends are guaranteed to produce identical
diffs for the same data.

### Contract tests

The test suite in `tests/test_repository.py` binds to `AgreementRepository`,
not to any concrete backend. The pytest fixture injects `ParquetBackend`:

```python
@pytest.fixture(params=["parquet"])
def repository(request, tmp_path):
    if request.param == "parquet":
        return ParquetBackend(data_dir=tmp_path)
```

When `TimescaleBackend` is added, it is added to `params`. Every test in the
suite automatically runs against both backends. This enforces behavioural
parity — the pluggable pattern is only useful if the contract is tested.

---

## API design

### Route ordering

The diff route must be registered before the snapshot retrieval route:

```python
# diff — registered first
GET /counterparties/{id}/agreements/{ns}/diff

# snapshot — registered second
GET /counterparties/{id}/agreements/{ns}/{cob_date}
```

FastAPI matches routes in registration order. If the snapshot route is
registered first, a request to `.../diff` would match with `cob_date="diff"`,
which fails date parsing and returns 422 instead of routing to the diff
handler.

### `exposure_total` at ingestion

The `POST /ingest` body is:
```json
{"snapshot": {...}, "exposure_total": 1250000.0}
```

`exposure_total` is a top-level field alongside `snapshot`, not a field inside
`COBSnapshot`. This makes explicit that cade does not own the computation —
the caller provides it separately from the snapshot data.

### Portfolio index staleness header

When `GET /portfolio/exposure` detects that the index may be stale, it adds
`X-Index-Stale: true` to the response headers rather than returning an error.
Stale data is still useful — callers that need precision can check the header
and re-request after a refresh. An error would break callers during normal
end-of-day ingestion windows.

---

## Diff design

`SnapshotDiff` is a typed Pydantic model with domain-specific fields rather
than a generic recursive diff:

```python
class SnapshotDiff(BaseModel):
    trades_added: list[TradePosition]
    trades_removed: list[TradePosition]
    agreement_changes: dict[str, tuple[Any, Any]]  # field -> (old, new)
    fx_rate_changes: dict[str, tuple[Any, Any]]
    inflation_rate_changes: dict[str, tuple[Any, Any]]
    matrix_changes: list[str]
    exposure_delta: float | None
```

A generic diff (`{"field": {"old": X, "new": Y}}` for any changed field) would
be simpler to implement but harder to consume. A risk manager investigating a
spike wants to know: "which trades were added or removed, did agreement terms
change, what moved in rates?" The typed model answers these questions directly.

### Float comparison in diff

FX rates and inflation rates are floats. Two snapshots ingested from different
source systems may have the same economic value represented with different
floating-point representations (e.g. `0.79` vs `0.7900000000000001`). These
should not appear as changes.

The diff uses an epsilon of `1e-9`:

```python
def _floats_equal(a: float, b: float) -> bool:
    return abs(a - b) < 1e-9
```

This is distinct from the hashing logic, which rounds to 10dp to ensure
deterministic hashing. The diff epsilon is deliberately looser — it suppresses
noise, whereas the hash rounding ensures determinism.

---

## Security

### ID sanitisation

`counterparty_id` and `netting_set_id` are used to construct file system
paths (`data/snapshots/{counterparty_id}/{netting_set_id}/...`). Without
sanitisation, a malicious caller could supply `../../../etc/passwd` and read
arbitrary files.

Both IDs are validated at model construction time by a Pydantic field
validator:

```python
_SAFE_ID_RE = re.compile(r'^[A-Za-z0-9_-]+$')
```

Any ID that does not match this pattern raises a `ValidationError` before it
touches the file system.

### Authentication

v1 has no application-level authentication. It assumes deployment inside a
trusted internal network. Adding API key auth (`X-API-Key` header, FastAPI
dependency injection) is tracked as a v2 item.

---

## What was deliberately left out

**Ingestion pipeline.** cade does not pull data from source systems. It
accepts snapshots pushed to it. How and when to push (scheduled job, upstream
trigger, manual script) is left to the operator.

**Exposure computation.** cade stores inputs, not outputs. Keeping computation
out of the data layer avoids coupling to a specific risk model.

**Authentication.** Internal network trust for v1. See future directions.

**UI.** A browser-based investigation dashboard is the intended v2 interface
for non-technical users. The API is designed to support it — the diff and
portfolio endpoints return structured, display-ready data.

**Anomaly detection.** Proactive spike alerting requires 30+ days of history
for a meaningful rolling baseline. Tracked for implementation once historical
data is loaded.

---

## Future directions

The following items are tracked and designed for but not yet implemented.
The architecture supports all of them without structural changes.

**TimescaleDB backend.** Add `TimescaleBackend` implementing `AgreementRepository`.
Postgres hypertables for time-series COB data. Full SQL, ACID writes, proper
indexing. The repository interface and all tests are backend-agnostic — adding
the backend is additive, not structural.

**Apache Iceberg backend.** The 12-month target architecture. Iceberg provides
native time-travel queries (no `data_hash` needed — reproducibility is
guaranteed by the storage model). Parquet → Iceberg migration is a one-time
re-ingestion job. The pluggable backend pattern exists specifically to make
this safe.

**Arrow nested struct serialisation.** Migrate `ParquetBackend` from JSON
string columns to `pyarrow.List[Struct]` for trades and `Struct` for agreement
and market data. Enables true columnar queries on nested fields and is the
natural migration step before Iceberg.

**API key authentication.** `X-API-Key` header, FastAPI dependency injection,
configurable via environment variable.

**Agreement version history.** `GET /counterparties/{id}/agreements/{ns}/history`
returning a timeline of CSA term changes across COB dates. The data is already
in the snapshots — this is a pure read-path addition.

**Anomaly detection.** At COB ingestion: compute a 30-day rolling mean and
standard deviation per counterparty. Flag counterparties deviating more than
2σ. Emit a structured alert (log, webhook, or email). Requires 30+ COB dates.

**UI investigation dashboard.** Browser-based spike investigation view.
The `diff` and `portfolio` endpoints are designed to feed this directly.
