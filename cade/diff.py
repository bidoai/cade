"""Snapshot diff computation."""
from typing import Any

from cade.models import COBSnapshot, SnapshotDiff

_FLOAT_EPSILON = 1e-9


def _floats_equal(a: float, b: float) -> bool:
    return abs(a - b) < _FLOAT_EPSILON


def _diff_dict(a: dict, b: dict) -> dict[str, tuple[Any, Any]]:
    changes: dict[str, tuple[Any, Any]] = {}
    for k in set(a) | set(b):
        va, vb = a.get(k), b.get(k)
        if isinstance(va, float) and isinstance(vb, float):
            if not _floats_equal(va, vb):
                changes[k] = (va, vb)
        elif va != vb:
            changes[k] = (va, vb)
    return changes


def compute_diff(snap_from: COBSnapshot, snap_to: COBSnapshot) -> SnapshotDiff:
    from_trades = {t.trade_id: t for t in snap_from.trades}
    to_trades = {t.trade_id: t for t in snap_to.trades}

    added = [to_trades[tid] for tid in to_trades if tid not in from_trades]
    removed = [from_trades[tid] for tid in from_trades if tid not in to_trades]

    agreement_changes = _diff_dict(
        {k: v for k, v in snap_from.agreement.model_dump().items() if k != "agreement_id"},
        {k: v for k, v in snap_to.agreement.model_dump().items() if k != "agreement_id"},
    )

    all_fx = set(snap_from.market_data.fx_rates) | set(snap_to.market_data.fx_rates)
    fx_changes: dict[str, tuple] = {}
    for ccy in all_fx:
        va = snap_from.market_data.fx_rates.get(ccy)
        vb = snap_to.market_data.fx_rates.get(ccy)
        if va is None or vb is None or not _floats_equal(va, vb):
            fx_changes[ccy] = (va, vb)

    all_inf = set(snap_from.market_data.inflation_rates) | set(snap_to.market_data.inflation_rates)
    inf_changes: dict[str, tuple] = {}
    for idx in all_inf:
        va = snap_from.market_data.inflation_rates.get(idx)
        vb = snap_to.market_data.inflation_rates.get(idx)
        if va is None or vb is None or not _floats_equal(va, vb):
            inf_changes[idx] = (va, vb)

    all_instr = set(snap_from.market_data.price_matrices) | set(snap_to.market_data.price_matrices)
    matrix_changes = []
    for inst in all_instr:
        ref_a = snap_from.market_data.price_matrices.get(inst)
        ref_b = snap_to.market_data.price_matrices.get(inst)
        if ref_a is None or ref_b is None or ref_a.hash != ref_b.hash:
            matrix_changes.append(inst)

    return SnapshotDiff(
        counterparty_id=snap_from.counterparty_id,
        netting_set_id=snap_from.netting_set_id,
        from_date=snap_from.cob_date,
        to_date=snap_to.cob_date,
        trades_added=added,
        trades_removed=removed,
        agreement_changes=agreement_changes,
        fx_rate_changes=fx_changes,
        inflation_rate_changes=inf_changes,
        matrix_changes=sorted(matrix_changes),
    )
