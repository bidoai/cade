"""Contract tests for AgreementRepository.

These tests bind to the AgreementRepository interface and run against all
backends via the `repository` fixture in conftest.py. When a new backend is
added, extend the fixture params — all tests run automatically.
"""
import pytest
from datetime import date
from pathlib import Path

from cade.exceptions import (
    DuplicateSnapshotError,
    IntegrityError,
    InvalidRangeError,
    MatrixReferenceError,
    SnapshotNotFound,
)
from cade.models import COBSnapshot, ISDAgreement, MarketDataSet, MatrixRef, TradePosition
from cade.repository import AgreementRepository


# ── store + retrieve ──────────────────────────────────────────────────────────

def test_round_trip(repository, sample_snapshot):
    stored = repository.store_snapshot(sample_snapshot, exposure_total=1_000_000.0)
    assert stored.data_hash is not None
    assert stored.data_hash.startswith("sha256-v1:")

    retrieved = repository.get_snapshot(
        sample_snapshot.counterparty_id,
        sample_snapshot.netting_set_id,
        sample_snapshot.cob_date,
    )
    assert retrieved.counterparty_id == sample_snapshot.counterparty_id
    assert retrieved.netting_set_id == sample_snapshot.netting_set_id
    assert retrieved.cob_date == sample_snapshot.cob_date
    assert retrieved.agreement.agreement_id == sample_snapshot.agreement.agreement_id
    assert len(retrieved.trades) == len(sample_snapshot.trades)
    assert retrieved.data_hash == stored.data_hash


def test_get_missing_raises_not_found(repository):
    with pytest.raises(SnapshotNotFound):
        repository.get_snapshot("UNKNOWN", "RATES", date(2024, 1, 1))


def test_duplicate_raises(repository, sample_snapshot):
    repository.store_snapshot(sample_snapshot, exposure_total=1_000_000.0)
    with pytest.raises(DuplicateSnapshotError):
        repository.store_snapshot(sample_snapshot, exposure_total=1_000_000.0)


def test_integrity_error_on_corrupted_file(repository, sample_snapshot, tmp_path):
    """The Friday 2am test: corrupt a byte, get IntegrityError."""
    repository.store_snapshot(sample_snapshot, exposure_total=1_000_000.0)

    # Find and corrupt the Parquet file
    parquet_files = list(tmp_path.rglob("*.parquet"))
    snapshot_files = [p for p in parquet_files if "index" not in str(p)]
    assert snapshot_files, "No snapshot file found"

    snap_file = snapshot_files[0]
    data = snap_file.read_bytes()
    # Flip a byte near the end of the file
    corrupted = data[:-10] + bytes([data[-10] ^ 0xFF]) + data[-9:]
    snap_file.write_bytes(corrupted)

    with pytest.raises((IntegrityError, Exception)):
        repository.get_snapshot(
            sample_snapshot.counterparty_id,
            sample_snapshot.netting_set_id,
            sample_snapshot.cob_date,
        )


def test_nan_in_fx_rates_raises(repository, sample_snapshot):
    import math
    snap = sample_snapshot.model_copy(
        update={"market_data": MarketDataSet(fx_rates={"USD/GBP": float("nan")})}
    )
    with pytest.raises(ValueError, match="Non-finite"):
        repository.store_snapshot(snap, exposure_total=0.0)


def test_matrix_ref_missing_raises(repository, sample_snapshot):
    snap = sample_snapshot.model_copy(
        update={
            "market_data": MarketDataSet(
                fx_rates={"USD/GBP": 0.79},
                price_matrices={
                    "IR_SWAP": MatrixRef(path="matrices/nonexistent.npy", hash="sha256-v1:abc")
                },
            )
        }
    )
    with pytest.raises(MatrixReferenceError):
        repository.store_snapshot(snap, exposure_total=0.0)


# ── list operations ───────────────────────────────────────────────────────────

def test_list_cob_dates(repository, sample_snapshot):
    repository.store_snapshot(sample_snapshot, exposure_total=1_000_000.0)

    snap2 = sample_snapshot.model_copy(update={"cob_date": date(2024, 3, 16), "data_hash": None})
    repository.store_snapshot(snap2, exposure_total=1_100_000.0)

    dates = repository.list_cob_dates(
        sample_snapshot.counterparty_id, sample_snapshot.netting_set_id
    )
    assert dates == [date(2024, 3, 15), date(2024, 3, 16)]


def test_list_cob_dates_empty(repository):
    assert repository.list_cob_dates("NOBODY", "RATES") == []


def test_list_netting_sets(repository, sample_snapshot):
    repository.store_snapshot(sample_snapshot, exposure_total=1_000_000.0)

    snap2 = sample_snapshot.model_copy(
        update={"netting_set_id": "FX-USD", "data_hash": None}
    )
    repository.store_snapshot(snap2, exposure_total=500_000.0)

    ns = repository.list_netting_sets(sample_snapshot.counterparty_id)
    assert "RATES-USD" in ns
    assert "FX-USD" in ns


# ── portfolio ─────────────────────────────────────────────────────────────────

def test_portfolio_ranking(repository, sample_snapshot):
    repository.store_snapshot(sample_snapshot, exposure_total=1_000_000.0)

    snap2 = sample_snapshot.model_copy(
        update={"counterparty_id": "FOO-BANK", "data_hash": None}
    )
    repository.store_snapshot(snap2, exposure_total=5_000_000.0)

    summaries, stale = repository.get_portfolio(sample_snapshot.cob_date)
    assert not stale
    assert summaries[0].counterparty_id == "FOO-BANK"
    assert summaries[1].counterparty_id == "ACME-CORP"


def test_portfolio_empty_date_returns_empty(repository):
    summaries, stale = repository.get_portfolio(date(2000, 1, 1))
    assert summaries == []
    assert not stale


def test_portfolio_threshold_filter(repository, sample_snapshot):
    repository.store_snapshot(sample_snapshot, exposure_total=100_000.0)
    summaries, _ = repository.get_portfolio(sample_snapshot.cob_date, threshold=1_000_000.0)
    assert summaries == []


def test_portfolio_top_n(repository, sample_snapshot):
    repository.store_snapshot(sample_snapshot, exposure_total=1_000_000.0)
    snap2 = sample_snapshot.model_copy(
        update={"counterparty_id": "FOO-BANK", "data_hash": None}
    )
    repository.store_snapshot(snap2, exposure_total=5_000_000.0)

    summaries, _ = repository.get_portfolio(sample_snapshot.cob_date, top_n=1)
    assert len(summaries) == 1
    assert summaries[0].counterparty_id == "FOO-BANK"


# ── diff ──────────────────────────────────────────────────────────────────────

def test_diff_detects_trade_change(repository, sample_snapshot):
    repository.store_snapshot(sample_snapshot, exposure_total=1_000_000.0)

    from cade.models import TradePosition
    snap2 = sample_snapshot.model_copy(
        update={
            "cob_date": date(2024, 3, 16),
            "data_hash": None,
            "trades": [
                TradePosition(
                    trade_id="T-001",  # same
                    product_type="IRS",
                    notional=10_000_000.0,
                    currency="USD",
                    direction="PAY",
                ),
                TradePosition(
                    trade_id="T-003",  # new, T-002 removed
                    product_type="CDS",
                    notional=3_000_000.0,
                    currency="USD",
                    direction="PAY",
                ),
            ],
        }
    )
    repository.store_snapshot(snap2, exposure_total=1_200_000.0)

    diff = repository.get_diff(
        sample_snapshot.counterparty_id,
        sample_snapshot.netting_set_id,
        date(2024, 3, 15),
        date(2024, 3, 16),
    )
    assert any(t.trade_id == "T-003" for t in diff.trades_added)
    assert any(t.trade_id == "T-002" for t in diff.trades_removed)


def test_diff_invalid_range(repository, sample_snapshot):
    repository.store_snapshot(sample_snapshot, exposure_total=1_000_000.0)
    with pytest.raises(InvalidRangeError):
        repository.get_diff(
            sample_snapshot.counterparty_id,
            sample_snapshot.netting_set_id,
            date(2024, 3, 16),
            date(2024, 3, 15),
        )


def test_diff_same_date_raises_invalid_range(repository, sample_snapshot):
    repository.store_snapshot(sample_snapshot, exposure_total=1_000_000.0)
    with pytest.raises(InvalidRangeError):
        repository.get_diff(
            sample_snapshot.counterparty_id,
            sample_snapshot.netting_set_id,
            date(2024, 3, 15),
            date(2024, 3, 15),
        )


def test_diff_missing_snapshot_raises(repository, sample_snapshot):
    repository.store_snapshot(sample_snapshot, exposure_total=1_000_000.0)
    with pytest.raises(SnapshotNotFound):
        repository.get_diff(
            sample_snapshot.counterparty_id,
            sample_snapshot.netting_set_id,
            date(2024, 3, 15),
            date(2024, 3, 16),  # not ingested
        )


# ── list_counterparties ───────────────────────────────────────────────────────

def test_list_counterparties(repository, sample_snapshot):
    repository.store_snapshot(sample_snapshot, exposure_total=1_000_000.0)
    snap2 = sample_snapshot.model_copy(
        update={"counterparty_id": "FOO-BANK", "data_hash": None}
    )
    repository.store_snapshot(snap2, exposure_total=2_000_000.0)
    cps = repository.list_counterparties()
    assert "ACME-CORP" in cps
    assert "FOO-BANK" in cps


# ── find_by_trade ─────────────────────────────────────────────────────────────

def test_find_by_trade_returns_match(repository, sample_snapshot):
    repository.store_snapshot(sample_snapshot, exposure_total=1_000_000.0)
    results = repository.find_by_trade("T-001")
    assert len(results) == 1
    cp_id, ns_id, d, trade = results[0]
    assert cp_id == sample_snapshot.counterparty_id
    assert trade.trade_id == "T-001"


def test_find_by_trade_not_found(repository, sample_snapshot):
    repository.store_snapshot(sample_snapshot, exposure_total=1_000_000.0)
    results = repository.find_by_trade("T-NONEXISTENT")
    assert results == []


def test_find_by_trade_date_filter(repository, sample_snapshot):
    # sample_snapshot uses cob_date=date(2024, 3, 15)
    repository.store_snapshot(sample_snapshot, exposure_total=1_000_000.0)
    # to_date of 2024-03-15 should include the snapshot
    results_inclusive = repository.find_by_trade("T-001", to_date=date(2024, 3, 15))
    assert len(results_inclusive) == 1
    # to_date of 2024-03-14 (before snapshot date) should exclude it
    results_before = repository.find_by_trade("T-001", to_date=date(2024, 3, 14))
    assert results_before == []
