"""RideFlow dashboard — analytics on the pipeline's warehouse output.

Run:  python -m streamlit run rideflow/dashboard.py

Two views:
  * Warehouse — the gold star schema (business analytics) + DQ monitoring
  * Simulator — the raw OLTP source (for debugging the generator)

Live mode (sidebar toggle) auto-refreshes the current view every 15s —
pair it with `python -m rideflow.demo` to watch data flow end to end.
"""
from __future__ import annotations

import streamlit as st

from rideflow.config import OLTP_URL, STATE_URL, WAREHOUSE_URL

st.set_page_config(page_title="RideFlow", page_icon="🚕", layout="wide")
st.title("🚕 RideFlow")

view = st.sidebar.radio(
    "Data source",
    ["Warehouse (gold)", "Simulator (OLTP)"],
    help="Warehouse = star schema built by the pipeline (analytics-grade). "
         "Simulator = the operational database the pipeline extracts from.")
live = st.sidebar.toggle(
    "Live mode", value=False,
    help="Auto-refresh this view every 15 seconds. "
         "Run `python -m rideflow.demo` to generate live traffic.")
st.sidebar.caption(
    "Pipeline: OLTP → bronze (JSONL) → silver (Parquet) → gold (star schema), "
    "gated by 13 data quality checks.")


def _render() -> None:
    if view == "Warehouse (gold)":
        from rideflow.dashboards.warehouse import render
        render(WAREHOUSE_URL, STATE_URL)
    else:
        from rideflow.dashboards.simulator import render
        render(OLTP_URL)


if live:
    @st.fragment(run_every="15s")
    def _live_view() -> None:
        _render()

    _live_view()
else:
    _render()
