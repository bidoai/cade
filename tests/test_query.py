"""Tests for cade.query — the Python query API."""
import pytest
from datetime import date

import pandas as pd

import cade.query as q
from cade.backends.parquet import ParquetBackend
from cade.exceptions import SnapshotNotFound
from cade.models import COBSnapshot, ISDAgreement, MarketDataSet, TradePosition


@pytest.fixture
def repo(tmp_path):
    return ParquetBackend(data_dir=tmp_path)


@pytest.fixture
def populated_repo(repo, sample_snapshot):
    """Repo with two counterparties, two netting sets, two COB dates.

    sample_snapshot uses cob_date=date(2024, 3, 15), so:
      d1 = 2024-03-15
      d2 = 2024-03-16
    """
    # ACME-CORP / RATES-USD / 2024-03-15  (T-001, T-002)
    snap_d1 = sample_snapshot.model_copy(update={"data_hash": None})
    repo.store_snapshot(snap_d1, exposure_total=1_000_000.0)

    # ACME-CORP / RATES-USD / 2024-03-16  (T-002 removed, T-003 added)
    snap_d2 = sample_snapshot.model_copy(
        update={
            "cob_date": date(2024, 3, 16),
            "data_hash": None,
            "trades": [
                TradePosition(trade_id="T-001", product_type="IRS",
                              notional=10_000_000.0, currency="USD", direction="PAY"),
                TradePosition(trade_id="T-003", product_type="CDS",
                              notional=3_000_000.0, currency="USD", direction="RECEIVE"),
            ],
        }
    )
    repo.store_snapshot(snap_d2, exposure_total=1_200_000.0)

    # ACME-CORP / FX-EUR / 2024-03-15 — no trades so they don't pollute the trade index
    snap_fx = sample_snapshot.model_copy(
        update={
            "netting_set_id": "FX-EUR",
            "data_hash": None,
            "trades": [],
            "market_data": MarketDataSet(
                fx_rates={"USD/EUR": 0.92},
                inflation_rates={},
            ),
        }
    )
    repo.store_snapshot(snap_fx, exposure_total=500_000.0)

    # FOO-BANK / RATES-USD / 2024-03-15  (copy of sample_snapshot)
    snap_foo = sample_snapshot.model_copy(
        update={"counterparty_id": "FOO-BANK", "data_hash": None}
    )
    repo.store_snapshot(snap_foo, exposure_total=5_000_000.0)

    return repo


# ── snapshot ──────────────────────────────────────────────────────────────────

def test_snapshot_returns_correct(populated_repo, sample_snapshot):
    snap = q.snapshot("ACME-CORP", "RATES-USD", date(2024, 3, 15), repo=populated_repo)
    assert snap.counterparty_id == "ACME-CORP"
    assert snap.netting_set_id == "RATES-USD"
    assert snap.cob_date == date(2024, 3, 15)


def test_snapshot_missing_raises(populated_repo):
    with pytest.raises(SnapshotNotFound):
        q.snapshot("NOBODY", "RATES", date(2024, 1, 1), repo=populated_repo)


# ── by_counterparty ───────────────────────────────────────────────────────────

def test_by_counterparty_returns_all_netting_sets(populated_repo):
    result = q.by_counterparty("ACME-CORP", date(2024, 3, 15), repo=populated_repo)
    assert set(result.keys()) == {"RATES-USD", "FX-EUR"}
    assert isinstance(result["RATES-USD"], COBSnapshot)


def test_by_counterparty_skips_missing_dates(populated_repo):
    # FX-EUR only exists on 2024-03-15, RATES-USD exists on both dates
    result = q.by_counterparty("ACME-CORP", date(2024, 3, 16), repo=populated_repo)
    assert "RATES-USD" in result
    assert "FX-EUR" not in result  # not ingested for this date


def test_by_counterparty_unknown_returns_empty(populated_repo):
    result = q.by_counterparty("NOBODY", date(2024, 3, 15), repo=populated_repo)
    assert result == {}


# ── trades ────────────────────────────────────────────────────────────────────

def test_trades_returns_dataframe(populated_repo):
    df = q.trades("ACME-CORP", "RATES-USD", date(2024, 3, 15), repo=populated_repo)
    assert isinstance(df, pd.DataFrame)
    assert set(df.columns) >= {"trade_id", "product_type", "notional", "currency", "direction"}
    assert len(df) == 2
    assert "T-001" in df["trade_id"].values


def test_trades_empty_snapshot_returns_empty_df(populated_repo):
    # Store a snapshot with no trades
    snap = COBSnapshot(
        counterparty_id="EMPTY-CORP",
        netting_set_id="RATES-USD",
        cob_date=date(2024, 3, 15),
        agreement=ISDAgreement(
            agreement_id="A1", threshold_amount=1.0,
            minimum_transfer_amount=0.1, currency="USD",
        ),
        trades=[],
    )
    populated_repo.store_snapshot(snap, exposure_total=0.0)
    df = q.trades("EMPTY-CORP", "RATES-USD", date(2024, 3, 15), repo=populated_repo)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0


# ── by_trade ──────────────────────────────────────────────────────────────────

def test_by_trade_finds_trade(populated_repo):
    # T-001 appears in:
    #   ACME-CORP / RATES-USD / 2024-03-15
    #   ACME-CORP / RATES-USD / 2024-03-16
    #   FOO-BANK  / RATES-USD / 2024-03-15
    # FX-EUR has trades=[], so it does not contribute
    df = q.by_trade("T-001", repo=populated_repo)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert all(df["trade_id"] == "T-001")


def test_by_trade_date_filter(populated_repo):
    # to_date=2024-03-15 excludes d2 (2024-03-16)
    # Remaining: ACME-CORP/RATES-USD/d1 and FOO-BANK/RATES-USD/d1
    df = q.by_trade("T-001", to_date=date(2024, 3, 15), repo=populated_repo)
    assert len(df) == 2
    assert all(df["cob_date"] == date(2024, 3, 15))


def test_by_trade_not_found_returns_empty(populated_repo):
    df = q.by_trade("T-NONEXISTENT", repo=populated_repo)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0


def test_by_trade_removed_trade_not_found_after_removal(populated_repo):
    # T-002 was removed on 2024-03-16; querying from 2024-03-16 onwards should find nothing
    df = q.by_trade("T-002", from_date=date(2024, 3, 16), repo=populated_repo)
    assert len(df) == 0


# ── fx_rates ──────────────────────────────────────────────────────────────────

def test_fx_rates_returns_dict(populated_repo):
    # Use 2024-03-16 where only RATES-USD snapshots exist (full market data)
    rates = q.fx_rates(date(2024, 3, 16), repo=populated_repo)
    assert isinstance(rates, dict)
    assert "USD/GBP" in rates
    assert abs(rates["USD/GBP"] - 0.79) < 1e-9


def test_fx_rates_single_pair(populated_repo):
    # Use 2024-03-16 where only RATES-USD snapshots exist (full market data)
    rate = q.fx_rates(date(2024, 3, 16), pair="USD/GBP", repo=populated_repo)
    assert isinstance(rate, float)
    assert abs(rate - 0.79) < 1e-9


def test_fx_rates_no_snapshots_raises(populated_repo):
    with pytest.raises(ValueError, match="No snapshots"):
        q.fx_rates(date(2000, 1, 1), repo=populated_repo)


# ── inflation_rates ───────────────────────────────────────────────────────────

def test_inflation_rates_returns_dict(populated_repo):
    # Use 2024-03-16 where only RATES-USD snapshots exist (has UK_RPI)
    rates = q.inflation_rates(date(2024, 3, 16), repo=populated_repo)
    assert isinstance(rates, dict)


def test_inflation_rates_single_index(populated_repo):
    # Use 2024-03-16 where only RATES-USD snapshots exist (has UK_RPI = 0.031)
    rate = q.inflation_rates(date(2024, 3, 16), index="UK_RPI", repo=populated_repo)
    assert isinstance(rate, float)
    assert abs(rate - 0.031) < 1e-9


# ── exposure_history ──────────────────────────────────────────────────────────

def test_exposure_history_returns_dataframe(populated_repo):
    df = q.exposure_history("ACME-CORP", repo=populated_repo)
    assert isinstance(df, pd.DataFrame)
    assert set(df.columns) >= {"cob_date", "netting_set_id", "exposure_total"}
    # ACME-CORP has RATES-USD on 2 dates + FX-EUR on 1 date = 3 rows
    assert len(df) == 3


def test_exposure_history_netting_set_filter(populated_repo):
    df = q.exposure_history("ACME-CORP", netting_set_id="RATES-USD", repo=populated_repo)
    assert all(df["netting_set_id"] == "RATES-USD")
    assert len(df) == 2


def test_exposure_history_date_filter(populated_repo):
    df = q.exposure_history(
        "ACME-CORP",
        from_date=date(2024, 3, 16),
        to_date=date(2024, 3, 16),
        repo=populated_repo,
    )
    assert all(df["cob_date"] == date(2024, 3, 16))


def test_exposure_history_sorted_ascending(populated_repo):
    df = q.exposure_history("ACME-CORP", netting_set_id="RATES-USD", repo=populated_repo)
    assert list(df["cob_date"]) == sorted(df["cob_date"])


def test_exposure_history_unknown_counterparty_empty(populated_repo):
    df = q.exposure_history("NOBODY", repo=populated_repo)
    assert len(df) == 0


# ── portfolio ─────────────────────────────────────────────────────────────────

def test_portfolio_returns_dataframe(populated_repo):
    df = q.portfolio(date(2024, 3, 15), repo=populated_repo)
    assert isinstance(df, pd.DataFrame)
    assert set(df.columns) >= {"counterparty_id", "netting_set_id", "exposure_total"}
    assert df.iloc[0]["counterparty_id"] == "FOO-BANK"  # highest exposure (5M)


def test_portfolio_threshold_filter(populated_repo):
    df = q.portfolio(date(2024, 3, 15), threshold=2_000_000.0, repo=populated_repo)
    assert all(df["exposure_total"] >= 2_000_000.0)


def test_portfolio_empty_date_returns_empty_df(populated_repo):
    df = q.portfolio(date(2000, 1, 1), repo=populated_repo)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0
