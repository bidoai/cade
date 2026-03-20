# cade — Counterparty Agreement Data Engine

cade is a data platform for bilateral counterparty risk. It stores and serves
the complete set of data needed to compute bilateral margined exposure: ISDA CSA
agreement terms, trade positions, FX rates, inflation rates, and price matrix
references — all keyed by counterparty, netting set, and Close of Business (COB)
date.

Every snapshot is immutable and cryptographically hashed. Querying a historical
COB date today returns the same result as querying it in three years.

---

## Contents

- [Concepts](#concepts)
- [Installation](#installation)
- [Quick start](#quick-start)
- [CLI reference](#cli-reference)
- [HTTP API reference](#http-api-reference)
- [Ingestion](#ingestion)
- [Data model](#data-model)
- [Configuration](#configuration)
- [Running tests](#running-tests)

---

## Concepts

### Counterparty and netting set

A **counterparty** is identified by a string ID (e.g. `ACME-CORP`). A
counterparty may have multiple **netting sets** — one per ISDA Master Agreement
and CSA — identified by a second string ID (e.g. `RATES-USD`, `FX-EUR`).

Most operations take both IDs: `ACME-CORP / RATES-USD`.

### COB snapshot

A **COB snapshot** is an immutable record of everything known about one netting
set at one Close of Business date:

- The ISDA CSA terms in effect on that date
- All active trade positions under that netting set
- Market data: FX rates, inflation rates, and price matrix references

Snapshots are write-once. There is no update or delete operation.

### data_hash

Every snapshot carries a `data_hash` field: a SHA-256 fingerprint of its
contents. cade recomputes and verifies this hash on every read. A mismatch
means the stored data has been tampered with or corrupted since ingestion.

This makes every historical query reproducible and auditable — the same
(counterparty, netting set, date) triple always returns identical data, and
any change is detectable.

### Portfolio index

The portfolio index is a lightweight summary of exposure totals per
counterparty/netting set for a given COB date. It is written at ingestion
time and used to power the `who-matters` query without scanning all snapshot
files.

---

## Installation

```bash
git clone <repo>
cd cade
pip install -e ".[dev]"
```

**Requirements:** Python 3.11+

**Runtime dependencies:** FastAPI, uvicorn, Pydantic v2, pyarrow, Typer, Rich

---

## Quick start

### 1. Start the API server

```bash
CADE_DATA_DIR=./data uvicorn cade.api:app --reload
```

The interactive API docs are available at http://localhost:8000/docs.

### 2. Ingest a snapshot

```bash
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "snapshot": {
      "counterparty_id": "ACME-CORP",
      "netting_set_id": "RATES-USD",
      "cob_date": "2024-03-15",
      "agreement": {
        "agreement_id": "ISDA-001",
        "threshold_amount": 5000000.0,
        "minimum_transfer_amount": 500000.0,
        "currency": "USD",
        "eligible_collateral": ["USD_CASH", "US_TREASURY"]
      },
      "trades": [
        {
          "trade_id": "T-001",
          "product_type": "IRS",
          "notional": 10000000.0,
          "currency": "USD",
          "direction": "PAY"
        }
      ],
      "market_data": {
        "fx_rates": {"USD/GBP": 0.79, "USD/EUR": 0.92},
        "inflation_rates": {"UK_RPI": 0.031},
        "price_matrices": {}
      }
    },
    "exposure_total": 1250000.0
  }'
```

The response includes the snapshot with `data_hash` populated.

### 3. Retrieve it

```bash
curl http://localhost:8000/counterparties/ACME-CORP/agreements/RATES-USD/2024-03-15
```

### 4. Use the CLI

```bash
# Set data directory
export CADE_DATA_DIR=./data

# Retrieve a snapshot
cade get ACME-CORP RATES-USD --date 2024-03-15

# See what changed between two dates
cade diff ACME-CORP RATES-USD --from 2024-03-14 --to 2024-03-15

# Portfolio ranking for today
cade who-matters --date 2024-03-15 --threshold 1000000 --top 10
```

---

## CLI reference

All commands read `CADE_DATA_DIR` from the environment (default: `./data`).

---

### `cade get`

Retrieve the full snapshot for a counterparty netting set on a COB date.

```
cade get COUNTERPARTY_ID NETTING_SET_ID --date YYYY-MM-DD
```

Output: JSON to stdout. Exits non-zero if the snapshot does not exist.

**Example:**
```bash
cade get ACME-CORP RATES-USD --date 2024-03-15
```

---

### `cade diff`

Show what changed between two COB dates for a netting set.

```
cade diff COUNTERPARTY_ID NETTING_SET_ID --from YYYY-MM-DD --to YYYY-MM-DD
```

Output: JSON `SnapshotDiff` object with typed fields for each category of
change. `--from` must precede `--to`.

**Example:**
```bash
cade diff ACME-CORP RATES-USD --from 2024-03-14 --to 2024-03-15
```

**Sample output:**
```json
{
  "counterparty_id": "ACME-CORP",
  "netting_set_id": "RATES-USD",
  "from_date": "2024-03-14",
  "to_date": "2024-03-15",
  "trades_added": [],
  "trades_removed": [{"trade_id": "T-099", ...}],
  "agreement_changes": {"threshold_amount": [5000000.0, 4000000.0]},
  "fx_rate_changes": {"USD/GBP": [0.78, 0.79]},
  "inflation_rate_changes": {},
  "matrix_changes": [],
  "exposure_delta": null
}
```

---

### `cade who-matters`

Show counterparties ranked by exposure on a COB date.

```
cade who-matters --date YYYY-MM-DD [--threshold N] [--top N]
```

Prints a formatted table. Warns if the portfolio index may be stale.

**Options:**
- `--threshold N` — only show counterparties with exposure >= N
- `--top N` — show at most N rows (default: 20)

**Example:**
```bash
cade who-matters --date 2024-03-15 --threshold 1000000 --top 10
```

---

### `cade list-dates`

List all COB dates for which a snapshot exists.

```
cade list-dates COUNTERPARTY_ID NETTING_SET_ID
```

Output: one ISO date per line, sorted ascending.

---

### `cade export`

Export a snapshot to stdout.

```
cade export COUNTERPARTY_ID NETTING_SET_ID --date YYYY-MM-DD [--format json]
```

---

## HTTP API reference

Base URL: `http://localhost:8000` (configurable)

Interactive docs: `GET /docs`

---

### `GET /health`

Returns server status and configured data directory.

**Response:**
```json
{"status": "ok", "data_dir": "./data"}
```

---

### `POST /ingest`

Ingest a COB snapshot. The `data_hash` is computed and set server-side.

**Request body:**
```json
{
  "snapshot": { ... COBSnapshot fields (without data_hash) ... },
  "exposure_total": 1250000.0
}
```

**Responses:**
- `201` — snapshot stored; body is the full `COBSnapshot` with `data_hash`
- `400` — validation error (NaN/Inf in float fields, invalid ID format, missing matrix file)
- `409` — snapshot already exists for this (counterparty, netting set, date)

---

### `GET /counterparties/{counterparty_id}/agreements/{netting_set_id}/{cob_date}`

Retrieve a snapshot and verify its hash.

**Path parameters:**
- `counterparty_id` — must match `[A-Za-z0-9_-]+`
- `netting_set_id` — must match `[A-Za-z0-9_-]+`
- `cob_date` — ISO date, e.g. `2024-03-15`

**Responses:**
- `200` — `COBSnapshot` JSON
- `404` — no snapshot for this triple
- `500` — hash verification failed (data may be corrupted)

---

### `GET /counterparties/{counterparty_id}/agreements/{netting_set_id}/diff`

Compare two snapshots for a netting set.

**Query parameters:**
- `from_date` — ISO date (earlier)
- `to_date` — ISO date (later)

**Responses:**
- `200` — `SnapshotDiff` JSON
- `400` — `from_date >= to_date`
- `404` — one or both snapshots missing

---

### `GET /counterparties/{counterparty_id}/agreements/{netting_set_id}`

List all COB dates with snapshots for a netting set.

**Response:** `["2024-03-14", "2024-03-15", ...]`

---

### `GET /counterparties/{counterparty_id}/agreements`

List all netting set IDs for a counterparty.

**Response:** `["FX-EUR", "RATES-USD", ...]`

---

### `GET /portfolio/exposure`

Portfolio exposure ranking for a COB date.

**Query parameters:**
- `cob_date` — required, ISO date
- `threshold` — optional, minimum exposure to include
- `top_n` — optional, maximum number of results (must be ≥ 1)

**Response headers:**
- `X-Index-Stale: true` — included when the portfolio index may not reflect
  all ingested snapshots (a snapshot file is newer than the index file)

**Response:** Array of `ExposureSummary` objects, sorted by `exposure_total`
descending.

---

## Ingestion

cade does not pull data from source systems. It accepts snapshots pushed to
it via `POST /ingest` or `AgreementRepository.store_snapshot()`.

**What to provide:**

| Field | Notes |
|---|---|
| `counterparty_id` | Alphanumeric + `_-` only |
| `netting_set_id` | Alphanumeric + `_-` only |
| `cob_date` | ISO date |
| `agreement` | ISDA CSA terms in effect on this date |
| `trades` | Active positions under this netting set on this date |
| `market_data.fx_rates` | Spot rates as of COB |
| `market_data.inflation_rates` | Curve values as of COB |
| `market_data.price_matrices` | References to matrix files (path + hash) |
| `exposure_total` | Pre-computed bilateral exposure (cade does not compute this) |

**Price matrix files:**

cade does not store matrix data inline. Instead, copy the matrix file to the
data directory first, then reference it:

```json
"price_matrices": {
  "IR_SWAP": {
    "path": "matrices/2024-03-15/ACME-CORP/IR_SWAP.npy",
    "hash": "sha256-v1:a3f8c2..."
  }
}
```

The path is relative to `CADE_DATA_DIR`. cade validates the file exists at
ingest time and verifies its hash at read time (when `?include_matrices=true`
is used).

**Computing the matrix hash:**

```python
import hashlib

def hash_matrix_file(path: str) -> str:
    data = open(path, "rb").read()
    return "sha256-v1:" + hashlib.sha256(data).hexdigest()
```

---

## Data model

### `COBSnapshot`

The atomic unit of storage.

| Field | Type | Description |
|---|---|---|
| `counterparty_id` | `str` | Counterparty identifier |
| `netting_set_id` | `str` | Netting set identifier |
| `cob_date` | `date` | Close of Business date |
| `agreement` | `ISDAgreement` | CSA terms on this date |
| `trades` | `list[TradePosition]` | Active positions |
| `market_data` | `MarketDataSet` | Rates and matrix refs |
| `data_hash` | `str \| None` | Set by cade on ingestion |

### `ISDAgreement`

| Field | Type | Description |
|---|---|---|
| `agreement_id` | `str` | Unique agreement identifier |
| `threshold_amount` | `float` | Counterparty threshold (in `currency`) |
| `minimum_transfer_amount` | `float` | MTA |
| `independent_amount` | `float` | IA (default 0) |
| `currency` | `str` | ISO 4217 currency code |
| `eligible_collateral` | `list[str]` | Accepted collateral types |
| `rounding_amount` | `float` | Rounding amount (default 0) |
| `valuation_agent` | `str \| None` | Valuation agent name |
| `extra` | `dict` | Non-standard terms |

### `TradePosition`

| Field | Type | Description |
|---|---|---|
| `trade_id` | `str` | Unique trade identifier |
| `product_type` | `str` | e.g. `"IRS"`, `"CDS"`, `"FX_FORWARD"` |
| `notional` | `float` | Trade notional |
| `currency` | `str` | ISO 4217 |
| `maturity_date` | `date \| None` | Trade maturity |
| `direction` | `str` | `"PAY"` or `"RECEIVE"` |
| `extra` | `dict` | Additional trade fields |

### `MarketDataSet`

| Field | Type | Description |
|---|---|---|
| `fx_rates` | `dict[str, float]` | e.g. `{"USD/GBP": 0.79}` |
| `inflation_rates` | `dict[str, float]` | e.g. `{"UK_RPI": 0.031}` |
| `price_matrices` | `dict[str, MatrixRef]` | Instrument → file reference |

### `MatrixRef`

| Field | Type | Description |
|---|---|---|
| `path` | `str` | Path to matrix file, relative to `CADE_DATA_DIR` |
| `hash` | `str` | `sha256-v1:<hex>` of file bytes |

### `SnapshotDiff`

| Field | Type | Description |
|---|---|---|
| `trades_added` | `list[TradePosition]` | Trades present in `to_date` but not `from_date` |
| `trades_removed` | `list[TradePosition]` | Trades present in `from_date` but not `to_date` |
| `agreement_changes` | `dict[str, [old, new]]` | CSA fields that changed |
| `fx_rate_changes` | `dict[str, [old, new]]` | Rate pairs that changed |
| `inflation_rate_changes` | `dict[str, [old, new]]` | Inflation indices that changed |
| `matrix_changes` | `list[str]` | Instruments whose matrix hash changed |
| `exposure_delta` | `float \| None` | Change in exposure (if available) |

---

## Configuration

| Variable | Default | Description |
|---|---|---|
| `CADE_DATA_DIR` | `./data` | Root directory for all stored data |

---

## Running tests

```bash
pytest tests/ -v
```

36 tests covering: hashing, models, repository contract (all backends),
API integration, and CLI behaviour.

**Adding a new storage backend:**

1. Implement `AgreementRepository` in `cade/backends/your_backend.py`
2. Add `"your_backend"` to the `params` list in `tests/conftest.py`
3. Run `pytest tests/test_repository.py` — all contract tests run automatically
   against your backend
