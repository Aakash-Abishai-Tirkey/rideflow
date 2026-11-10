"""Warehouse view — analytics on the gold star schema + DQ monitoring.

Everything here queries warehouse.db (gold) and state.db (run/DQ history),
never the OLTP source: the dashboard is a consumer of the pipeline's
output, which is the whole point of having a warehouse.
"""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine

STATUS_COLOR = {"pass": "#2e7d32", "warn": "#f9a825", "fail": "#c62828"}
FUNNEL_STAGES = ["requested", "matched", "started", "completed"]


@st.cache_data(ttl=15)
def load_gold(url: str) -> dict[str, pd.DataFrame]:
    eng = create_engine(url)
    out: dict[str, pd.DataFrame] = {}
    for t in ("fact_rides", "fact_payments", "fact_ride_events",
              "dim_zone", "dim_driver", "dim_user"):
        try:
            out[t] = pd.read_sql_table(t, eng)
        except Exception:
            out[t] = pd.DataFrame()
    return out


@st.cache_data(ttl=15)
def load_state(state_url: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    eng = create_engine(state_url)
    try:
        dq = pd.read_sql_table("dq_results", eng)
        runs = pd.read_sql_table("runs", eng)
    except Exception:
        dq, runs = pd.DataFrame(), pd.DataFrame()
    return dq, runs


def render(warehouse_url: str, state_url: str) -> None:
    gold = load_gold(warehouse_url)
    rides = gold["fact_rides"]
    if rides.empty:
        st.warning("Warehouse is empty — build it first:  python -m rideflow.orchestrator run")
        return

    rides["request_ts"] = pd.to_datetime(rides["request_ts"])
    rides["hour"] = rides["request_ts"].dt.hour
    rides["date"] = rides["request_ts"].dt.date
    payments = gold["fact_payments"]
    events = gold["fact_ride_events"]
    zone = gold["dim_zone"]
    drivers = gold["dim_driver"]

    zone_name = dict(zip(zone["zone_id"], zone["zone_name"]))
    completed = rides[rides["status"] == "completed"]
    paid = payments[payments["status"] == "completed"] if not payments.empty else payments
    revenue = paid["amount"].sum() if not paid.empty else 0.0

    biz, ops, dq_tab = st.tabs(["Business", "Operations", "Data Quality"])

    # ------------------------------------------------------------ Business
    with biz:
        k1, k2, k3, k4, k5, k6 = st.columns(6)
        k1.metric("Rides", f"{len(rides):,}")
        k2.metric("Completion", f"{len(completed) / len(rides):.0%}")
        k3.metric("Revenue", f"${revenue:,.0f}")
        k4.metric("Avg fare", f"${completed['fare'].mean():.2f}")
        k5.metric("Avg surge", f"{rides['surge_multiplier'].mean():.2f}x")
        k6.metric("Unmet demand", f"{rides['driver_id'].isna().sum():,}")
        st.caption("All metrics computed from the gold star schema (pipeline output).")

        c1, c2 = st.columns(2)
        with c1:
            rev_day = paid.copy() if not paid.empty else pd.DataFrame()
            if not rev_day.empty:
                rev_day["day"] = pd.to_datetime(rev_day["paid_at"]).dt.date
                g = rev_day.groupby("day")["amount"].sum().reset_index()
                g["avg"] = g["amount"].mean()
                st.plotly_chart(
                    px.bar(g, x="day", y="amount", title="Revenue by day (completed payments)",
                           labels={"amount": "revenue ($)"})
                    .update_traces(marker_color="#2e7d32"),
                    width="stretch")
        with c2:
            zr = (completed
                  .assign(pickup=completed["pickup_zone"].map(zone_name))
                  .groupby("pickup")["fare"].sum().sort_values()
                  .tail(12))
            st.plotly_chart(
                px.bar(x=zr.values, y=zr.index, orientation="h",
                       title="Revenue by pickup zone (top 12)",
                       labels={"x": "revenue ($)", "y": ""}),
                width="stretch")

        st.subheader("Driver leaderboard")
        top = drivers.sort_values("revenue_generated_usd", ascending=False).head(15)
        if not top.empty:
            g = top.sort_values("revenue_generated_usd")
            st.plotly_chart(
                px.bar(g, x="revenue_generated_usd", y="driver_id", orientation="h",
                       hover_data=["vehicle", "rating", "rides_completed"],
                       labels={"revenue_generated_usd": "revenue ($)"})
                .update_yaxes(dtick=1),
                width="stretch")

        st.subheader("Payment method mix")
        if not payments.empty:
            g = (payments.groupby(["method", "status"]).size()
                 .reset_index(name="n"))
            st.plotly_chart(
                px.bar(g, x="method", y="n", color="status",
                       color_discrete_map={"completed": "#2e7d32", "failed": "#c62828"},
                       title="Payments by method and status"),
                width="stretch")

    # ---------------------------------------------------------- Operations
    with ops:
        st.subheader("Ride funnel — where demand drops off")
        if not events.empty:
            counts = (events[events["event_type"].isin(FUNNEL_STAGES)]
                      .groupby("event_type")["ride_id"].nunique()
                      .reindex(FUNNEL_STAGES).fillna(0))
            st.plotly_chart(
                px.funnel(x=counts.values, y=FUNNEL_STAGES,
                          title="Rides reaching each lifecycle stage"),
                width="stretch")
            req, mat = counts["requested"], counts["matched"]
            com = counts["completed"]
            st.caption(
                f"matched {mat / req:.0%} of requests · completed {com / req:.0%} "
                f"of requests · lost after match: {(mat - com):,} "
                f"({(mat - com) / mat:.0%} of matched rides cancelled mid-trip)")

        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Avg surge — zone × hour")
            piv = completed.pivot_table(index="pickup_zone", columns="hour",
                                        values="surge_multiplier", aggfunc="mean")
            piv.index = [zone_name.get(z, z) for z in piv.index]
            st.plotly_chart(px.imshow(piv, aspect="auto",
                                      color_continuous_scale="OrRd"),
                            width="stretch")
        with c2:
            st.subheader("Demand flow — pickup × dropoff")
            od = (completed
                  .assign(pk=completed["pickup_zone"].map(zone_name),
                          dp=completed["dropoff_zone"].map(zone_name))
                  .pivot_table(index="pk", columns="dp",
                               values="ride_id", aggfunc="count"))
            st.plotly_chart(px.imshow(od.fillna(0), aspect="auto",
                                      color_continuous_scale="Blues"),
                            width="stretch")

        with st.expander("Star schema — tables in the warehouse"):
            for t, df in gold.items():
                st.write(f"**{t}** — {len(df):,} rows · columns: "
                         + ", ".join(df.columns))

    # -------------------------------------------------------- Data Quality
    with dq_tab:
        dq, runs = load_state(state_url)
        if dq.empty:
            st.info("No DQ results yet — run:  python -m rideflow.pipeline.dq run")
            return

        dq = dq.merge(
            runs[["run_id", "mode", "status", "started_at"]]
            .rename(columns={"status": "run_status"}),
            on="run_id", how="left")
        dq["started_at"] = pd.to_datetime(dq["started_at"])
        dq = dq.sort_values("started_at")

        latest = dq["run_id"].iloc[-1]
        agg = (dq.groupby(["run_id", "mode", "started_at", "status"], as_index=False)
               .size().rename(columns={"size": "n"}))
        agg["label"] = agg["started_at"].dt.strftime("%m-%d %H:%M")

        st.plotly_chart(
            px.bar(agg, x="label", y="n", color="status", barmode="stack",
                   color_discrete_map=STATUS_COLOR,
                   category_orders={"status": ["pass", "warn", "fail"]},
                   hover_data=["run_id", "mode"],
                   title="DQ checks per run — pass / warn / fail over time",
                   labels={"label": "run started", "n": "checks"}),
            width="stretch")

        bad = dq[dq["status"] != "pass"].sort_values("started_at")
        if bad.empty:
            st.success(f"All checks passing across all {dq['run_id'].nunique()} runs.")
        else:
            st.subheader("Incident history — every warn/fail ever recorded")
            st.dataframe(
                bad[["started_at", "run_id", "mode", "run_status", "status",
                     "check", "layer", "detail"]],
                width="stretch", height=320)

        st.caption(f"Latest run: {latest} · full history in state.db (dq_results).")
