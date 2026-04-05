"""Cade UI — Streamlit dashboard for browsing and exporting agreement data."""
from __future__ import annotations

import io
import json
import os
import zipfile
from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st

from cade.backends.parquet import ParquetBackend
from cade.exceptions import SnapshotNotFound

st.set_page_config(
    page_title="Cade",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Backend ───────────────────────────────────────────────────────────────────

@st.cache_resource
def get_repo() -> ParquetBackend:
    data_dir = os.environ.get("CADE_DATA_DIR", "./data")
    return ParquetBackend(data_dir)


def _fmt(amount: float, currency: str) -> str:
    return f"{currency} {amount:,.0f}"


def _business_days_since(d: date) -> int:
    """Count business days (Mon–Fri) between d and today, exclusive of d."""
    today = date.today()
    count = 0
    current = d + timedelta(days=1)
    while current <= today:
        if current.weekday() < 5:
            count += 1
        current += timedelta(days=1)
    return count


@st.cache_data(show_spinner=False)
def _get_exposure(cp_id: str, ns_id: str, cob_date: date) -> float | None:
    """Look up exposure_total from the portfolio index for this snapshot."""
    repo = get_repo()
    summaries, _ = repo.get_portfolio(cob_date)
    for s in summaries:
        if s.counterparty_id == cp_id and s.netting_set_id == ns_id:
            return s.exposure_total
    return None


@st.cache_data(show_spinner=False)
def _build_zip(cp_id: str, ns_id: str, cob_date: date) -> bytes:
    """Build an in-memory ZIP with all snapshot data. Cached by (cp, ns, date)."""
    repo = get_repo()
    snap = repo.get_snapshot(cp_id, ns_id, cob_date)
    data_dir = os.environ.get("CADE_DATA_DIR", "./data")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        # Full snapshot
        zf.writestr(
            "snapshot.json",
            json.dumps(snap.model_dump(mode="json"), indent=2, default=str),
        )
        # Agreement only
        zf.writestr(
            "agreement.json",
            json.dumps(snap.agreement.model_dump(mode="json"), indent=2, default=str),
        )
        # Trades as CSV
        if snap.trades:
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
            zf.writestr("trades.csv", pd.DataFrame(rows).to_csv(index=False))
        # Market data
        md = {
            "fx_rates": snap.market_data.fx_rates,
            "inflation_rates": snap.market_data.inflation_rates,
            "price_matrices": {
                k: {"path": v.path, "hash": v.hash}
                for k, v in snap.market_data.price_matrices.items()
            },
        }
        zf.writestr("market_data.json", json.dumps(md, indent=2))
        # Price matrix files
        for instrument, ref in snap.market_data.price_matrices.items():
            matrix_path = os.path.join(data_dir, ref.path)
            if os.path.exists(matrix_path):
                ext = os.path.splitext(ref.path)[1]
                with open(matrix_path, "rb") as f:
                    zf.writestr(f"matrices/{instrument}{ext}", f.read())
        # README
        zf.writestr(
            "README.txt",
            (
                f"Cade Export\n===========\n"
                f"Counterparty : {snap.counterparty_id}\n"
                f"Netting Set  : {snap.netting_set_id}\n"
                f"COB Date     : {snap.cob_date}\n"
                f"Exported     : {datetime.utcnow().isoformat()}Z\n\n"
                f"Data Hash: {snap.data_hash}\n\n"
                f"Files\n-----\n"
                f"snapshot.json    Full COBSnapshot (all fields)\n"
                f"agreement.json   ISDA CSA terms only\n"
                f"trades.csv       Active trade positions\n"
                f"market_data.json FX rates, inflation rates, matrix refs\n"
                f"matrices/        Price matrix files (if available)\n"
            ),
        )

    buf.seek(0)
    return buf.read()


# ── Sidebar ───────────────────────────────────────────────────────────────────

repo = get_repo()

st.sidebar.title("Cade")
st.sidebar.caption("Counterparty Agreement Data Engine")
st.sidebar.divider()

mode = st.sidebar.radio(
    "input_mode",
    ["Browse", "Direct entry"],
    horizontal=True,
    label_visibility="collapsed",
)

counterparties = sorted(repo.list_counterparties())
available_dates: list[date] = []

if mode == "Browse":
    cp_id: str | None = st.sidebar.selectbox(
        "Counterparty",
        counterparties,
        index=None,
        placeholder="Select counterparty…",
    )
    ns_options = sorted(repo.list_netting_sets(cp_id)) if cp_id else []
    ns_id: str | None = (
        st.sidebar.selectbox("Netting Set", ns_options, index=None, placeholder="Select netting set…")
        if cp_id
        else None
    )
    cob_date: date | None = None
    if cp_id and ns_id:
        available_dates = repo.list_cob_dates(cp_id, ns_id)
        if available_dates:
            cob_date = st.sidebar.selectbox(
                "COB Date",
                sorted(available_dates, reverse=True),
                format_func=lambda d: d.isoformat(),
                index=0,
            )
        else:
            st.sidebar.warning("No COB dates found for this netting set.")
else:
    cp_id = st.sidebar.text_input("Counterparty ID", placeholder="e.g. ACME-CORP") or None
    ns_id = st.sidebar.text_input("Netting Set ID", placeholder="e.g. RATES-USD") or None
    raw_date = st.sidebar.date_input("COB Date", value=date.today())
    cob_date = raw_date if (cp_id and ns_id) else None
    if cp_id and ns_id:
        available_dates = repo.list_cob_dates(cp_id, ns_id)

st.sidebar.divider()
st.sidebar.caption(f"Data: `{os.environ.get('CADE_DATA_DIR', './data')}`")

# ── Landing: no counterparty selected ────────────────────────────────────────

if not cp_id:
    st.title("Portfolio")
    if not counterparties:
        st.warning("No data found. Set the `CADE_DATA_DIR` environment variable.")
        st.stop()
    st.info(f"{len(counterparties)} counterparties in store. Select one in the sidebar.")
    rows = []
    for cp in counterparties:
        ns_list = repo.list_netting_sets(cp)
        all_dates = [d for ns in ns_list for d in repo.list_cob_dates(cp, ns)]
        rows.append({
            "Counterparty": cp,
            "Netting Sets": len(ns_list),
            "Snapshots": len(all_dates),
            "Last COB": max(all_dates).isoformat() if all_dates else "—",
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    st.stop()

# ── Counterparty selected, no netting set ────────────────────────────────────

if not ns_id:
    st.title(cp_id)
    ns_list = sorted(repo.list_netting_sets(cp_id))
    if not ns_list:
        st.warning("No netting sets found for this counterparty.")
        st.stop()
    rows = []
    for ns in ns_list:
        dates = repo.list_cob_dates(cp_id, ns)
        rows.append({
            "Netting Set": ns,
            "Snapshots": len(dates),
            "First COB": min(dates).isoformat() if dates else "—",
            "Last COB": max(dates).isoformat() if dates else "—",
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    st.stop()

# ── No date selected yet ──────────────────────────────────────────────────────

if not cob_date:
    st.title(f"{cp_id} / {ns_id}")
    st.info("Select a COB date in the sidebar.")
    st.stop()

# ── Load snapshot ─────────────────────────────────────────────────────────────

st.title(f"{cp_id} / {ns_id}")
st.caption(f"COB: {cob_date.isoformat()}")

try:
    snap = repo.get_snapshot(cp_id, ns_id, cob_date)
except SnapshotNotFound:
    st.error(f"No snapshot found for `{cp_id}` / `{ns_id}` on `{cob_date}`.")
    if available_dates:
        recent = sorted(available_dates, reverse=True)[:10]
        st.info("Available dates: " + "  ".join(f"`{d}`" for d in recent))
    st.stop()
except Exception as e:
    st.error(f"Error loading snapshot: {e}")
    st.stop()

# ── Stale data banner ─────────────────────────────────────────────────────────

if available_dates:
    last_cob = max(available_dates)
    stale_days = _business_days_since(last_cob)
    if stale_days > 1:
        st.warning(
            f"Data is **{stale_days} business day{'s' if stale_days != 1 else ''} stale** "
            f"— last COB is {last_cob.isoformat()}. Downstream risk metrics may be outdated."
        )

# ── Data availability summary ─────────────────────────────────────────────────

if available_dates:
    col1, col2, col3 = st.columns(3)
    col1.metric("Snapshots available", len(available_dates))
    col2.metric("First COB", min(available_dates).isoformat())
    col3.metric("Last COB", max(available_dates).isoformat())

st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────

tab_agr, tab_trades, tab_market, tab_diff = st.tabs(
    ["Agreement", "Trades", "Market Data", "Diff"]
)
agr = snap.agreement

# Well-known keys that may appear in agreement.extra
_EXTRA_LABELS: dict[str, str] = {
    "csa_type": "CSA Type",
    "mpor": "MPOR (days)",
    "margin_period_of_risk": "Margin Period of Risk (days)",
    "netting_eligible": "Netting Eligible",
    "governing_law": "Governing Law",
    "dispute_resolution": "Dispute Resolution",
    "credit_support_amount": "Credit Support Amount",
    "delivery_amount": "Delivery Amount",
    "return_amount": "Return Amount",
}

with tab_agr:
    # ── Exposure vs. Threshold ────────────────────────────────────────────────
    exposure = _get_exposure(cp_id, ns_id, cob_date)
    if exposure is not None:
        delta = exposure - agr.threshold_amount
        ecol1, ecol2, ecol3, ecol4 = st.columns(4)
        ecol1.metric("Exposure", _fmt(exposure, agr.currency))
        ecol2.metric("Threshold", _fmt(agr.threshold_amount, agr.currency))

        if agr.threshold_amount == 0:
            ecol3.metric("Threshold Type", "Zero — Two-Way")
            ecol4.metric("MTA", _fmt(agr.minimum_transfer_amount, agr.currency))
        else:
            utilization = exposure / agr.threshold_amount * 100
            label = "Over Threshold" if delta > 0 else "Under Threshold"
            ecol3.metric(
                label,
                _fmt(abs(delta), agr.currency),
                delta=f"{delta:+,.0f}",
                delta_color="inverse",
            )
            ecol4.metric("Utilization", f"{utilization:.1f}%")

        if delta > 0:
            st.error(
                f"Exposure exceeds threshold by {_fmt(delta, agr.currency)} — "
                f"margin call may be eligible. Check MTA of {_fmt(agr.minimum_transfer_amount, agr.currency)}."
            )
        st.divider()

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Threshold Amount", _fmt(agr.threshold_amount, agr.currency))
        st.metric("Min Transfer Amount", _fmt(agr.minimum_transfer_amount, agr.currency))
        st.metric("Independent Amount", _fmt(agr.independent_amount, agr.currency))

    with col2:
        st.metric("Currency", agr.currency)
        if agr.rounding_amount:
            st.metric("Rounding Amount", _fmt(agr.rounding_amount, agr.currency))
        if agr.valuation_agent:
            st.metric("Valuation Agent", agr.valuation_agent)

    with col3:
        st.markdown("**Eligible Collateral**")
        if agr.eligible_collateral:
            for c in agr.eligible_collateral:
                st.markdown(f"- {c}")
        else:
            st.caption("None specified")
        st.markdown(f"**Agreement ID**  \n`{agr.agreement_id}`")

    # Promote well-known extra fields; show remainder in expander
    if agr.extra:
        st.divider()
        st.subheader("Additional Terms")
        promoted = {k: v for k, v in agr.extra.items() if k in _EXTRA_LABELS}
        remainder = {k: v for k, v in agr.extra.items() if k not in _EXTRA_LABELS}

        if promoted:
            cols = st.columns(min(len(promoted), 3))
            for i, (key, label) in enumerate(
                (k, _EXTRA_LABELS[k]) for k in _EXTRA_LABELS if k in promoted
            ):
                cols[i % 3].metric(label, str(promoted[key]))

        if remainder:
            with st.expander(f"Other terms ({len(remainder)})"):
                st.json(remainder)

    st.divider()
    # Hash integrity — verified on load (IntegrityError would have been raised otherwise)
    st.caption(f"[hash verified] Data hash: `{snap.data_hash}`")

with tab_trades:
    if not snap.trades:
        st.info("No trades in this snapshot.")
    else:
        rows = [
            {
                "Trade ID": t.trade_id,
                "Product": t.product_type,
                "Direction": t.direction,
                "Notional": t.notional,
                "Currency": t.currency,
                "Maturity": t.maturity_date,
            }
            for t in snap.trades
        ]
        df = pd.DataFrame(rows)

        col1, col2, col3 = st.columns(3)
        col1.metric("Trades", len(df))
        col2.metric("Product Types", df["Product"].nunique())
        col3.metric("Total Notional", f"{df['Notional'].sum():,.0f}")

        by_prod = (
            df.groupby("Product")
            .agg(Count=("Trade ID", "count"), Total_Notional=("Notional", "sum"))
            .reset_index()
        )
        st.dataframe(by_prod, use_container_width=True, hide_index=True)

        st.subheader("All Trades")
        st.dataframe(df, use_container_width=True, hide_index=True)

with tab_market:
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**FX Rates**")
        if snap.market_data.fx_rates:
            st.dataframe(
                pd.DataFrame([{"Pair": k, "Rate": v} for k, v in snap.market_data.fx_rates.items()]),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.caption("None")

    with col2:
        st.markdown("**Inflation Rates**")
        if snap.market_data.inflation_rates:
            st.dataframe(
                pd.DataFrame(
                    [{"Index": k, "Rate": f"{v:.4%}"} for k, v in snap.market_data.inflation_rates.items()]
                ),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.caption("None")

    st.markdown("**Price Matrices**")
    if snap.market_data.price_matrices:
        st.dataframe(
            pd.DataFrame(
                [{"Instrument": k, "Path": v.path, "Hash": v.hash}
                 for k, v in snap.market_data.price_matrices.items()]
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.caption("None")

with tab_diff:
    other_dates = sorted([d for d in available_dates if d != cob_date], reverse=True)
    if not other_dates:
        st.info("Only one COB date available — nothing to compare against.")
    else:
        compare_date = st.selectbox(
            "Compare against",
            other_dates,
            format_func=lambda d: d.isoformat(),
            index=0,  # auto-select the most recent previous date
        )
        if compare_date:
            from_d = min(compare_date, cob_date)
            to_d = max(compare_date, cob_date)
            try:
                diff = repo.get_diff(cp_id, ns_id, from_d, to_d)
            except Exception as e:
                st.error(f"Could not compute diff: {e}")
            else:
                if not diff.has_changes:
                    st.success(f"No changes between {from_d} and {to_d}.")
                else:
                    if diff.agreement_changes:
                        st.markdown("**Agreement Changes**")
                        st.dataframe(
                            pd.DataFrame(
                                [{"Field": k, str(from_d): str(v[0]), str(to_d): str(v[1])}
                                 for k, v in diff.agreement_changes.items()]
                            ),
                            use_container_width=True,
                            hide_index=True,
                        )
                    if diff.trades_added:
                        st.markdown(f"**Trades Added ({len(diff.trades_added)})**")
                        st.dataframe(
                            pd.DataFrame(
                                [{"Trade ID": t.trade_id, "Product": t.product_type, "Notional": t.notional}
                                 for t in diff.trades_added]
                            ),
                            use_container_width=True,
                            hide_index=True,
                        )
                    if diff.trades_removed:
                        st.markdown(f"**Trades Removed ({len(diff.trades_removed)})**")
                        st.dataframe(
                            pd.DataFrame(
                                [{"Trade ID": t.trade_id, "Product": t.product_type, "Notional": t.notional}
                                 for t in diff.trades_removed]
                            ),
                            use_container_width=True,
                            hide_index=True,
                        )
                    if diff.fx_rate_changes:
                        st.markdown("**FX Rate Changes**")
                        st.dataframe(
                            pd.DataFrame(
                                [{"Pair": k, str(from_d): v[0], str(to_d): v[1]}
                                 for k, v in diff.fx_rate_changes.items()]
                            ),
                            use_container_width=True,
                            hide_index=True,
                        )
                    if diff.inflation_rate_changes:
                        st.markdown("**Inflation Rate Changes**")
                        st.dataframe(
                            pd.DataFrame(
                                [{"Index": k, str(from_d): f"{v[0]:.4%}", str(to_d): f"{v[1]:.4%}"}
                                 for k, v in diff.inflation_rate_changes.items()]
                            ),
                            use_container_width=True,
                            hide_index=True,
                        )
                    if diff.matrix_changes:
                        st.markdown("**Matrix Changes**")
                        for m in diff.matrix_changes:
                            st.markdown(f"- {m}")
                    if diff.exposure_delta is not None:
                        st.metric("Exposure Delta", f"{diff.exposure_delta:+,.2f}")

# ── Download ──────────────────────────────────────────────────────────────────

st.divider()
zip_bytes = _build_zip(cp_id, ns_id, cob_date)
st.download_button(
    label="Download ZIP",
    data=zip_bytes,
    file_name=f"cade_{cp_id}_{ns_id}_{cob_date.isoformat()}.zip",
    mime="application/zip",
    type="primary",
)
