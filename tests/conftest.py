"""Shared test fixtures.

Contract tests use the `repository` fixture, which is parameterized over all
available backends. Adding a new backend: add it to the params list here and
implement the fixture case. All tests that use `repository` automatically run
against every backend.
"""
import pytest
from pathlib import Path

from cade.backends.parquet import ParquetBackend
from cade.models import (
    COBSnapshot,
    ISDAgreement,
    MarketDataSet,
    MatrixRef,
    TradePosition,
)


@pytest.fixture(params=["parquet"])
def repository(request, tmp_path):
    if request.param == "parquet":
        return ParquetBackend(data_dir=tmp_path)
    raise ValueError(f"Unknown backend: {request.param}")


@pytest.fixture
def sample_agreement() -> ISDAgreement:
    return ISDAgreement(
        agreement_id="ISDA-001",
        threshold_amount=5_000_000.0,
        minimum_transfer_amount=500_000.0,
        currency="USD",
        eligible_collateral=["USD_CASH", "US_TREASURY"],
    )


@pytest.fixture
def sample_trades() -> list[TradePosition]:
    return [
        TradePosition(
            trade_id="T-001",
            product_type="IRS",
            notional=10_000_000.0,
            currency="USD",
            direction="PAY",
        ),
        TradePosition(
            trade_id="T-002",
            product_type="FX_FORWARD",
            notional=2_000_000.0,
            currency="GBP",
            direction="RECEIVE",
        ),
    ]


@pytest.fixture
def sample_market_data() -> MarketDataSet:
    return MarketDataSet(
        fx_rates={"USD/GBP": 0.79, "USD/EUR": 0.92},
        inflation_rates={"UK_RPI": 0.031},
    )


@pytest.fixture
def sample_snapshot(sample_agreement, sample_trades, sample_market_data) -> COBSnapshot:
    from datetime import date
    return COBSnapshot(
        counterparty_id="ACME-CORP",
        netting_set_id="RATES-USD",
        cob_date=date(2024, 3, 15),
        agreement=sample_agreement,
        trades=sample_trades,
        market_data=sample_market_data,
    )
