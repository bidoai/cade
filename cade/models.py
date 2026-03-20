from __future__ import annotations

import re
from datetime import date
from typing import Any

from pydantic import BaseModel, Field, field_validator

_SAFE_ID_RE = re.compile(r'^[A-Za-z0-9_-]+$')


def _validate_safe_id(value: str) -> str:
    if not _SAFE_ID_RE.match(value):
        raise ValueError(f"ID must match [A-Za-z0-9_-]+, got {value!r}")
    return value


class MatrixRef(BaseModel):
    """Reference to an externally stored price matrix file."""
    path: str
    hash: str  # sha256 of file bytes, prefixed 'sha256-v1:'


class ISDAgreement(BaseModel):
    """ISDA CSA terms governing a netting set."""
    agreement_id: str
    threshold_amount: float
    minimum_transfer_amount: float
    independent_amount: float = 0.0
    currency: str
    eligible_collateral: list[str] = Field(default_factory=list)
    rounding_amount: float = 0.0
    valuation_agent: str | None = None
    extra: dict[str, Any] = Field(default_factory=dict)


class TradePosition(BaseModel):
    """A single trade position under a netting set."""
    trade_id: str
    product_type: str
    notional: float
    currency: str
    maturity_date: date | None = None
    direction: str  # "PAY" or "RECEIVE"
    extra: dict[str, Any] = Field(default_factory=dict)


class MarketDataSet(BaseModel):
    """Market data inputs for exposure computation."""
    fx_rates: dict[str, float] = Field(default_factory=dict)
    inflation_rates: dict[str, float] = Field(default_factory=dict)
    price_matrices: dict[str, MatrixRef] = Field(default_factory=dict)


class COBSnapshot(BaseModel):
    """Complete data snapshot for one netting set on one COB date.

    Immutable once stored. data_hash authenticates all fields:

        data_hash = sha256(canonical_json(all_fields_except_data_hash))

    Verifying the hash on read proves the stored data has not been
    modified since ingestion.
    """
    counterparty_id: str
    netting_set_id: str
    cob_date: date
    agreement: ISDAgreement
    trades: list[TradePosition] = Field(default_factory=list)
    market_data: MarketDataSet = Field(default_factory=MarketDataSet)
    data_hash: str | None = None  # set by store_snapshot(), not by caller

    @field_validator("counterparty_id", "netting_set_id")
    @classmethod
    def validate_id(cls, v: str) -> str:
        return _validate_safe_id(v)


class ExposureSummary(BaseModel):
    """Lightweight portfolio index entry."""
    counterparty_id: str
    netting_set_id: str
    cob_date: date
    exposure_total: float
    snapshot_path: str


class SnapshotDiff(BaseModel):
    """Structured diff between two COBSnapshots.

    Float comparisons use epsilon=1e-9 to suppress floating-point noise.
    """
    counterparty_id: str
    netting_set_id: str
    from_date: date
    to_date: date
    trades_added: list[TradePosition] = Field(default_factory=list)
    trades_removed: list[TradePosition] = Field(default_factory=list)
    agreement_changes: dict[str, tuple[Any, Any]] = Field(default_factory=dict)
    fx_rate_changes: dict[str, tuple[Any, Any]] = Field(default_factory=dict)
    inflation_rate_changes: dict[str, tuple[Any, Any]] = Field(default_factory=dict)
    matrix_changes: list[str] = Field(default_factory=list)
    exposure_delta: float | None = None

    @property
    def has_changes(self) -> bool:
        return bool(
            self.trades_added or self.trades_removed
            or self.agreement_changes or self.fx_rate_changes
            or self.inflation_rate_changes or self.matrix_changes
        )
