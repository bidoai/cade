"""Python query API for cade.

Notebook-friendly functions for accessing agreement data. All functions
accept an optional `repo` parameter — if omitted, a module-level
ParquetBackend instance is used, configured via CADE_DATA_DIR.

Return types:
  - Structured data (single snapshot, dict of snapshots): Pydantic models
  - Tabular data (trades, portfolio, history): pandas DataFrames
"""
from __future__ import annotations

import os
from datetime import date
from typing import TYPE_CHECKING

import pandas as pd

from cade.models import COBSnapshot
from cade.repository import AgreementRepository

if TYPE_CHECKING:
    pass


def _default_repo() -> AgreementRepository:
    from cade.backends.parquet import ParquetBackend
    data_dir = os.environ.get("CADE_DATA_DIR", "./data")
    return ParquetBackend(data_dir)


def snapshot(
    counterparty_id: str,
    netting_set_id: str,
    cob_date: date,
    *,
    repo: AgreementRepository | None = None,
) -> COBSnapshot:
    """Retrieve a single COB snapshot.

    Args:
        counterparty_id: Counterparty identifier.
        netting_set_id: Netting set identifier.
        cob_date: Close of Business date.
        repo: Storage backend. Uses CADE_DATA_DIR if not provided.

    Returns:
        COBSnapshot with data_hash verified.

    Raises:
        SnapshotNotFound: if no snapshot exists for this triple.
        IntegrityError: if the stored hash does not match.

    Example:
        >>> snap = cade.query.snapshot("ACME-CORP", "RATES-USD", date(2024, 3, 15))
        >>> snap.agreement.threshold_amount
        5000000.0
    """
    r = repo or _default_repo()
    return r.get_snapshot(counterparty_id, netting_set_id, cob_date)


def by_counterparty(
    counterparty_id: str,
    cob_date: date,
    *,
    repo: AgreementRepository | None = None,
) -> dict[str, COBSnapshot]:
    """All netting sets for a counterparty on a COB date.

    Args:
        counterparty_id: Counterparty identifier.
        cob_date: Close of Business date.
        repo: Storage backend. Uses CADE_DATA_DIR if not provided.

    Returns:
        Dict mapping netting_set_id -> COBSnapshot.
        Empty dict if the counterparty has no snapshots on this date.

    Example:
        >>> snaps = cade.query.by_counterparty("ACME-CORP", date(2024, 3, 15))
        >>> for ns_id, snap in snaps.items():
        ...     print(ns_id, snap.agreement.threshold_amount)
    """
    from cade.exceptions import SnapshotNotFound

    r = repo or _default_repo()
    netting_sets = r.list_netting_sets(counterparty_id)
    result: dict[str, COBSnapshot] = {}
    for ns_id in netting_sets:
        try:
            result[ns_id] = r.get_snapshot(counterparty_id, ns_id, cob_date)
        except SnapshotNotFound:
            continue
    return result


def trades(
    counterparty_id: str,
    netting_set_id: str,
    cob_date: date,
    *,
    repo: AgreementRepository | None = None,
) -> pd.DataFrame:
    """All trades for a netting set on a COB date as a DataFrame.

    Args:
        counterparty_id: Counterparty identifier.
        netting_set_id: Netting set identifier.
        cob_date: Close of Business date.
        repo: Storage backend. Uses CADE_DATA_DIR if not provided.

    Returns:
        DataFrame with columns: trade_id, product_type, notional, currency,
        direction, maturity_date. Empty DataFrame if no trades.

    Example:
        >>> df = cade.query.trades("ACME-CORP", "RATES-USD", date(2024, 3, 15))
        >>> df[df.product_type == "IRS"]["notional"].sum()
    """
    r = repo or _default_repo()
    snap = r.get_snapshot(counterparty_id, netting_set_id, cob_date)
    if not snap.trades:
        return pd.DataFrame(columns=["trade_id", "product_type", "notional",
                                      "currency", "direction", "maturity_date"])
    rows = [
        {
            "trade_id": t.trade_id,
            "product_type": t.product_type,
            "notional": t.notional,
            "currency": t.currency,
            "direction": t.direction,
            "maturity_date": t.maturity_date,
        }
        for t in snap.trades
    ]
    return pd.DataFrame(rows)


def by_trade(
    trade_id: str,
    *,
    from_date: date | None = None,
    to_date: date | None = None,
    repo: AgreementRepository | None = None,
) -> pd.DataFrame:
    """Find every snapshot containing a trade ID.

    Uses the trade index for fast lookup when backed by ParquetBackend.
    Falls back to scanning all snapshots for other backends.

    Args:
        trade_id: Trade identifier to search for.
        from_date: Only search COB dates on or after this date.
        to_date: Only search COB dates on or before this date.
        repo: Storage backend. Uses CADE_DATA_DIR if not provided.

    Returns:
        DataFrame with columns: cob_date, counterparty_id, netting_set_id,
        trade_id, product_type, notional, currency, direction, maturity_date.
        Empty DataFrame if no matches.

    Example:
        >>> df = cade.query.by_trade("T-001")
        >>> df.groupby("cob_date")["notional"].first()  # notional over time
    """
    r = repo or _default_repo()
    matches = r.find_by_trade(trade_id, from_date=from_date, to_date=to_date)
    if not matches:
        return pd.DataFrame(columns=["cob_date", "counterparty_id", "netting_set_id",
                                      "trade_id", "product_type", "notional",
                                      "currency", "direction", "maturity_date"])
    rows = [
        {
            "cob_date": d,
            "counterparty_id": cp_id,
            "netting_set_id": ns_id,
            "trade_id": t.trade_id,
            "product_type": t.product_type,
            "notional": t.notional,
            "currency": t.currency,
            "direction": t.direction,
            "maturity_date": t.maturity_date,
        }
        for cp_id, ns_id, d, t in matches
    ]
    return pd.DataFrame(rows)


def fx_rates(
    cob_date: date,
    *,
    pair: str | None = None,
    repo: AgreementRepository | None = None,
) -> dict[str, float] | float:
    """FX rates for a COB date.

    Reads market data from the first available snapshot on this date.
    FX rates are assumed to be consistent across all snapshots for a
    given COB date (they come from the same market data source).

    Args:
        cob_date: Close of Business date.
        pair: If provided, return only this pair (e.g. "USD/GBP").
              Returns a float. Raises KeyError if pair not found.
        repo: Storage backend. Uses CADE_DATA_DIR if not provided.

    Returns:
        dict[pair, rate] if pair is None, else float.

    Raises:
        ValueError: if no snapshots exist for this COB date.
        KeyError: if pair is specified but not found.

    Example:
        >>> cade.query.fx_rates(date(2024, 3, 15))
        {"USD/GBP": 0.79, "USD/EUR": 0.92}
        >>> cade.query.fx_rates(date(2024, 3, 15), pair="USD/GBP")
        0.79
    """
    snap = _first_snapshot_for_date(cob_date, repo)
    rates = snap.market_data.fx_rates
    if pair is not None:
        return rates[pair]
    return dict(rates)


def inflation_rates(
    cob_date: date,
    *,
    index: str | None = None,
    repo: AgreementRepository | None = None,
) -> dict[str, float] | float:
    """Inflation rates for a COB date.

    Reads market data from the first available snapshot on this date.

    Args:
        cob_date: Close of Business date.
        index: If provided, return only this index (e.g. "UK_RPI").
               Returns a float. Raises KeyError if index not found.
        repo: Storage backend. Uses CADE_DATA_DIR if not provided.

    Returns:
        dict[index_name, rate] if index is None, else float.

    Raises:
        ValueError: if no snapshots exist for this COB date.
        KeyError: if index is specified but not found.

    Example:
        >>> cade.query.inflation_rates(date(2024, 3, 15))
        {"UK_RPI": 0.031}
    """
    snap = _first_snapshot_for_date(cob_date, repo)
    rates = snap.market_data.inflation_rates
    if index is not None:
        return rates[index]
    return dict(rates)


def exposure_history(
    counterparty_id: str,
    *,
    netting_set_id: str | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
    repo: AgreementRepository | None = None,
) -> pd.DataFrame:
    """Exposure time series for a counterparty, read from the portfolio index.

    This reads the lightweight portfolio index files (not full snapshots),
    so it is fast even over long date ranges.

    Args:
        counterparty_id: Counterparty identifier.
        netting_set_id: If provided, filter to this netting set only.
        from_date: Start of date range (inclusive).
        to_date: End of date range (inclusive).
        repo: Storage backend. Uses CADE_DATA_DIR if not provided.

    Returns:
        DataFrame with columns: cob_date, netting_set_id, exposure_total.
        Sorted by cob_date ascending.

    Example:
        >>> df = cade.query.exposure_history("ACME-CORP")
        >>> df.plot(x="cob_date", y="exposure_total")  # exposure over time
    """
    from cade.backends.parquet import ParquetBackend

    r = repo or _default_repo()

    # Use direct index access if available (ParquetBackend)
    if isinstance(r, ParquetBackend):
        rows = _parquet_exposure_history(r, counterparty_id, netting_set_id,
                                          from_date, to_date)
    else:
        # Fallback: use portfolio endpoint for each available date
        rows = _scan_exposure_history(r, counterparty_id, netting_set_id,
                                       from_date, to_date)

    if not rows:
        return pd.DataFrame(columns=["cob_date", "netting_set_id", "exposure_total"])

    df = pd.DataFrame(rows).sort_values("cob_date").reset_index(drop=True)
    return df


def portfolio(
    cob_date: date,
    *,
    threshold: float | None = None,
    top_n: int | None = None,
    repo: AgreementRepository | None = None,
) -> pd.DataFrame:
    """Portfolio exposure ranking for a COB date as a DataFrame.

    Args:
        cob_date: Close of Business date.
        threshold: Only include counterparties with exposure >= threshold.
        top_n: Return at most this many rows.
        repo: Storage backend. Uses CADE_DATA_DIR if not provided.

    Returns:
        DataFrame with columns: counterparty_id, netting_set_id, exposure_total.
        Sorted by exposure_total descending.

    Example:
        >>> df = cade.query.portfolio(date(2024, 3, 15), threshold=1_000_000)
        >>> df.head(5)
    """
    r = repo or _default_repo()
    summaries, _ = r.get_portfolio(cob_date, threshold=threshold, top_n=top_n)
    if not summaries:
        return pd.DataFrame(columns=["counterparty_id", "netting_set_id", "exposure_total"])
    rows = [
        {
            "counterparty_id": s.counterparty_id,
            "netting_set_id": s.netting_set_id,
            "exposure_total": s.exposure_total,
        }
        for s in summaries
    ]
    return pd.DataFrame(rows)


# ── internal helpers ──────────────────────────────────────────────────────────

def _first_snapshot_for_date(
    cob_date: date,
    repo: AgreementRepository | None,
) -> COBSnapshot:
    """Return any snapshot available for this COB date. Used for market data queries."""
    from cade.exceptions import SnapshotNotFound

    r = repo or _default_repo()
    for cp_id in r.list_counterparties():
        for ns_id in r.list_netting_sets(cp_id):
            dates = r.list_cob_dates(cp_id, ns_id)
            if cob_date in dates:
                return r.get_snapshot(cp_id, ns_id, cob_date)
    raise ValueError(f"No snapshots found for COB date {cob_date}")


def _parquet_exposure_history(
    repo,
    counterparty_id: str,
    netting_set_id: str | None,
    from_date: date | None,
    to_date: date | None,
) -> list[dict]:
    """Read exposure history directly from portfolio index Parquet files."""
    import pyarrow.parquet as pq

    index_dir = repo._root / "index"
    if not index_dir.exists():
        return []

    rows = []
    for idx_file in sorted(index_dir.glob("*.parquet")):
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
            row_cp = table["counterparty_id"][i].as_py()
            row_ns = table["netting_set_id"][i].as_py()
            if row_cp != counterparty_id:
                continue
            if netting_set_id is not None and row_ns != netting_set_id:
                continue
            rows.append({
                "cob_date": file_date,
                "netting_set_id": row_ns,
                "exposure_total": table["exposure_total"][i].as_py(),
            })
    return rows


def _scan_exposure_history(
    repo: AgreementRepository,
    counterparty_id: str,
    netting_set_id: str | None,
    from_date: date | None,
    to_date: date | None,
) -> list[dict]:
    """Fallback: scan portfolio endpoint across known dates."""
    # Collect all unique COB dates for this counterparty
    all_dates: set[date] = set()
    netting_sets = repo.list_netting_sets(counterparty_id)
    for ns in netting_sets:
        for d in repo.list_cob_dates(counterparty_id, ns):
            if from_date is not None and d < from_date:
                continue
            if to_date is not None and d > to_date:
                continue
            all_dates.add(d)

    rows = []
    for d in sorted(all_dates):
        summaries, _ = repo.get_portfolio(d)
        for s in summaries:
            if s.counterparty_id != counterparty_id:
                continue
            if netting_set_id is not None and s.netting_set_id != netting_set_id:
                continue
            rows.append({
                "cob_date": d,
                "netting_set_id": s.netting_set_id,
                "exposure_total": s.exposure_total,
            })
    return rows
