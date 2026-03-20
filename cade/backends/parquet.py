"""Parquet storage backend for cade.

Directory layout:
    {data_dir}/
      snapshots/
        {counterparty_id}/
          {netting_set_id}/
            {cob_date}.parquet      # one row, JSON-encoded nested fields
      index/
        {cob_date}.parquet          # portfolio index: one row per cp/ns
      matrices/
        {cob_date}/
          {counterparty_id}/
            {instrument}.*          # matrix files (external, any format)
"""
import json
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from cade.exceptions import (
    DuplicateSnapshotError,
    IntegrityError,
    MatrixReferenceError,
    SnapshotNotFound,
)
from cade.hashing import compute_hash
from cade.models import (
    COBSnapshot,
    ExposureSummary,
    ISDAgreement,
    MarketDataSet,
    MatrixRef,
    TradePosition,
)
from cade.repository import AgreementRepository

_SNAPSHOT_SCHEMA = pa.schema([
    pa.field("counterparty_id", pa.string()),
    pa.field("netting_set_id", pa.string()),
    pa.field("cob_date", pa.string()),
    pa.field("agreement_json", pa.string()),
    pa.field("trades_json", pa.string()),
    pa.field("market_data_json", pa.string()),
    pa.field("data_hash", pa.string()),
    pa.field("ingested_at", pa.string()),
])

_INDEX_SCHEMA = pa.schema([
    pa.field("counterparty_id", pa.string()),
    pa.field("netting_set_id", pa.string()),
    pa.field("cob_date", pa.string()),
    pa.field("exposure_total", pa.float64()),
    pa.field("snapshot_path", pa.string()),
    pa.field("ingested_at", pa.string()),
])

_TRADE_INDEX_SCHEMA = pa.schema([
    pa.field("trade_id", pa.string()),
    pa.field("counterparty_id", pa.string()),
    pa.field("netting_set_id", pa.string()),
    pa.field("cob_date", pa.string()),
])


class ParquetBackend(AgreementRepository):
    def __init__(self, data_dir: str | Path) -> None:
        self._root = Path(data_dir)
        self._root.mkdir(parents=True, exist_ok=True)

    # ── path helpers ──────────────────────────────────────────────────────────

    def _snapshot_path(self, counterparty_id: str, netting_set_id: str, cob_date: date) -> Path:
        return self._root / "snapshots" / counterparty_id / netting_set_id / f"{cob_date}.parquet"

    def _index_path(self, cob_date: date) -> Path:
        return self._root / "index" / f"{cob_date}.parquet"

    def _trade_index_path(self, cob_date: date) -> Path:
        return self._root / "trade_index" / f"{cob_date}.parquet"

    def _resolve_matrix_path(self, ref_path: str) -> Path:
        """Resolve a matrix reference path relative to the data root."""
        p = Path(ref_path)
        if not p.is_absolute():
            p = self._root / p
        return p

    # ── write ─────────────────────────────────────────────────────────────────

    def store_snapshot(self, snapshot: COBSnapshot, exposure_total: float) -> COBSnapshot:
        snap_path = self._snapshot_path(
            snapshot.counterparty_id, snapshot.netting_set_id, snapshot.cob_date
        )
        if snap_path.exists():
            raise DuplicateSnapshotError(
                snapshot.counterparty_id, snapshot.netting_set_id, snapshot.cob_date
            )

        # Validate matrix references exist on disk
        for inst, ref in snapshot.market_data.price_matrices.items():
            p = self._resolve_matrix_path(ref.path)
            if not p.exists():
                raise MatrixReferenceError(ref.path)

        # Compute and set hash
        snapshot_dict = snapshot.model_dump(mode="json")
        data_hash = compute_hash(snapshot_dict)  # raises ValueError on NaN/Inf
        snapshot = snapshot.model_copy(update={"data_hash": data_hash})

        # Serialise nested fields to JSON strings
        snap_path.parent.mkdir(parents=True, exist_ok=True)
        now = datetime.now(timezone.utc).isoformat()

        table = pa.table(
            {
                "counterparty_id": [snapshot.counterparty_id],
                "netting_set_id": [snapshot.netting_set_id],
                "cob_date": [str(snapshot.cob_date)],
                "agreement_json": [snapshot.agreement.model_dump_json()],
                "trades_json": [json.dumps([t.model_dump(mode="json") for t in snapshot.trades])],
                "market_data_json": [snapshot.market_data.model_dump_json()],
                "data_hash": [data_hash],
                "ingested_at": [now],
            },
            schema=_SNAPSHOT_SCHEMA,
        )
        pq.write_table(table, snap_path)

        # Update portfolio index
        self._upsert_index(snapshot, exposure_total, str(snap_path.relative_to(self._root)), now)
        self._upsert_trade_index(snapshot)

        return snapshot

    def _upsert_index(
        self,
        snapshot: COBSnapshot,
        exposure_total: float,
        snapshot_rel_path: str,
        now: str,
    ) -> None:
        index_path = self._index_path(snapshot.cob_date)
        index_path.parent.mkdir(parents=True, exist_ok=True)

        new_row = pa.table(
            {
                "counterparty_id": [snapshot.counterparty_id],
                "netting_set_id": [snapshot.netting_set_id],
                "cob_date": [str(snapshot.cob_date)],
                "exposure_total": [exposure_total],
                "snapshot_path": [snapshot_rel_path],
                "ingested_at": [now],
            },
            schema=_INDEX_SCHEMA,
        )

        if index_path.exists():
            existing = pq.read_table(index_path)
            combined = pa.concat_tables([existing, new_row])
        else:
            combined = new_row

        pq.write_table(combined, index_path)

    def _upsert_trade_index(self, snapshot: COBSnapshot) -> None:
        """Write trade_id -> (counterparty_id, netting_set_id) entries for this snapshot."""
        if not snapshot.trades:
            return

        index_path = self._trade_index_path(snapshot.cob_date)
        index_path.parent.mkdir(parents=True, exist_ok=True)

        new_rows = pa.table(
            {
                "trade_id": [t.trade_id for t in snapshot.trades],
                "counterparty_id": [snapshot.counterparty_id] * len(snapshot.trades),
                "netting_set_id": [snapshot.netting_set_id] * len(snapshot.trades),
                "cob_date": [str(snapshot.cob_date)] * len(snapshot.trades),
            },
            schema=_TRADE_INDEX_SCHEMA,
        )

        if index_path.exists():
            existing = pq.read_table(index_path)
            combined = pa.concat_tables([existing, new_rows])
        else:
            combined = new_rows

        pq.write_table(combined, index_path)

    # ── read ──────────────────────────────────────────────────────────────────

    def get_snapshot(
        self,
        counterparty_id: str,
        netting_set_id: str,
        cob_date: date,
    ) -> COBSnapshot:
        snap_path = self._snapshot_path(counterparty_id, netting_set_id, cob_date)
        if not snap_path.exists():
            raise SnapshotNotFound(counterparty_id, netting_set_id, cob_date)

        table = pq.read_table(snap_path)
        row = {col: table[col][0].as_py() for col in table.schema.names}

        snapshot = COBSnapshot(
            counterparty_id=row["counterparty_id"],
            netting_set_id=row["netting_set_id"],
            cob_date=date.fromisoformat(row["cob_date"]),
            agreement=ISDAgreement.model_validate_json(row["agreement_json"]),
            trades=[TradePosition.model_validate(t) for t in json.loads(row["trades_json"])],
            market_data=MarketDataSet.model_validate_json(row["market_data_json"]),
            data_hash=row["data_hash"],
        )

        # Verify hash
        snapshot_dict = snapshot.model_dump(mode="json")
        recomputed = compute_hash(snapshot_dict)
        if recomputed != row["data_hash"]:
            raise IntegrityError(stored_hash=row["data_hash"], computed_hash=recomputed)

        return snapshot

    def list_cob_dates(self, counterparty_id: str, netting_set_id: str) -> list[date]:
        ns_dir = self._root / "snapshots" / counterparty_id / netting_set_id
        if not ns_dir.exists():
            return []
        dates = []
        for p in ns_dir.glob("*.parquet"):
            try:
                dates.append(date.fromisoformat(p.stem))
            except ValueError:
                continue
        return sorted(dates)

    def list_netting_sets(self, counterparty_id: str) -> list[str]:
        cp_dir = self._root / "snapshots" / counterparty_id
        if not cp_dir.exists():
            return []
        return sorted(p.name for p in cp_dir.iterdir() if p.is_dir())

    def get_portfolio(
        self,
        cob_date: date,
        threshold: float | None = None,
        top_n: int | None = None,
    ) -> tuple[list[ExposureSummary], bool]:
        index_path = self._index_path(cob_date)
        if not index_path.exists():
            return [], False

        # Check staleness: is any snapshot newer than the index?
        index_mtime = index_path.stat().st_mtime
        stale = False
        snap_dir = self._root / "snapshots"
        if snap_dir.exists():
            for p in snap_dir.rglob("*.parquet"):
                if p.stat().st_mtime > index_mtime:
                    stale = True
                    break

        table = pq.read_table(index_path)
        summaries = []
        for i in range(len(table)):
            row = {col: table[col][i].as_py() for col in table.schema.names}
            exp = row["exposure_total"]
            if threshold is not None and exp < threshold:
                continue
            summaries.append(
                ExposureSummary(
                    counterparty_id=row["counterparty_id"],
                    netting_set_id=row["netting_set_id"],
                    cob_date=date.fromisoformat(row["cob_date"]),
                    exposure_total=exp,
                    snapshot_path=row["snapshot_path"],
                )
            )

        summaries.sort(key=lambda s: s.exposure_total, reverse=True)
        if top_n is not None:
            summaries = summaries[:top_n]

        return summaries, stale

    def list_counterparties(self) -> list[str]:
        snap_dir = self._root / "snapshots"
        if not snap_dir.exists():
            return []
        return sorted(p.name for p in snap_dir.iterdir() if p.is_dir())

    def find_by_trade(
        self,
        trade_id: str,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> list[tuple[str, str, date, "TradePosition"]]:
        """Index-based trade lookup. Reads trade_index/{date}.parquet files only."""
        from cade.models import TradePosition

        # Find all relevant trade index files
        index_dir = self._root / "trade_index"
        if not index_dir.exists():
            return []

        index_files = sorted(index_dir.glob("*.parquet"))
        results = []

        for idx_file in index_files:
            try:
                file_date = date.fromisoformat(idx_file.stem)
            except ValueError:
                continue
            if from_date is not None and file_date < from_date:
                continue
            if to_date is not None and file_date > to_date:
                continue

            table = pq.read_table(idx_file)
            for i in range(len(table)):
                if table["trade_id"][i].as_py() == trade_id:
                    cp_id = table["counterparty_id"][i].as_py()
                    ns_id = table["netting_set_id"][i].as_py()
                    # Load the full snapshot to get the TradePosition
                    snap = self.get_snapshot(cp_id, ns_id, file_date)
                    for t in snap.trades:
                        if t.trade_id == trade_id:
                            results.append((cp_id, ns_id, file_date, t))
                            break

        return results
