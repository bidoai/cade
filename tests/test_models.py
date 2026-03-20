import pytest
from datetime import date
from pydantic import ValidationError
from cade.models import COBSnapshot, ISDAgreement, TradePosition, MarketDataSet


def test_invalid_counterparty_id():
    with pytest.raises(ValidationError):
        COBSnapshot(
            counterparty_id="../etc/passwd",
            netting_set_id="RATES",
            cob_date=date(2024, 3, 15),
            agreement=ISDAgreement(
                agreement_id="A1",
                threshold_amount=1.0,
                minimum_transfer_amount=0.1,
                currency="USD",
            ),
        )


def test_invalid_netting_set_id():
    with pytest.raises(ValidationError):
        COBSnapshot(
            counterparty_id="ACME",
            netting_set_id="RATES/USD",  # slash not allowed
            cob_date=date(2024, 3, 15),
            agreement=ISDAgreement(
                agreement_id="A1",
                threshold_amount=1.0,
                minimum_transfer_amount=0.1,
                currency="USD",
            ),
        )


def test_snapshot_diff_has_changes_false_when_empty():
    from cade.models import SnapshotDiff
    diff = SnapshotDiff(
        counterparty_id="ACME",
        netting_set_id="RATES",
        from_date=date(2024, 3, 14),
        to_date=date(2024, 3, 15),
    )
    assert not diff.has_changes


def test_snapshot_diff_has_changes_true():
    from cade.models import SnapshotDiff, TradePosition
    diff = SnapshotDiff(
        counterparty_id="ACME",
        netting_set_id="RATES",
        from_date=date(2024, 3, 14),
        to_date=date(2024, 3, 15),
        trades_added=[
            TradePosition(trade_id="T-NEW", product_type="IRS", notional=1e6, currency="USD", direction="PAY")
        ],
    )
    assert diff.has_changes
