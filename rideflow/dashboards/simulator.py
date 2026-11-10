"""Simulator view — v1 explorer over the raw OLTP source.

Useful for debugging the simulator itself; business analytics belongs to
the warehouse view.
"""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine


@st.cache_data(ttl=15)
def load(url: str) -> dict[str, pd.DataFrame]:
    eng = create_engine(url)
    return {
        "rides": pd.read_sql_table("rides", eng, parse_dates=["request_ts", "completed_ts"]),
        "payments": pd.read_sql_table("payments", eng, parse_dates=["paid_at"]),
        "drivers": pd.read_sql_table("drivers", eng),
    }


def render(oltp_url: str) -> None:
    st.header("Simulator — raw OLTP source")
    st.caption("The operational database the pipeline extracts from. "
               "For analytics, switch to the Warehouse view.")

    data = load(oltp_url)
    rides: pd.DataFrame = data["rides"]
    if rides.empty:
        st.warning("No data yet. Generate some first:  "
                   "python -m rideflow.simulator.engine --days 7 --reset")
        return

    from rideflow.simulator.city import ZONE_BY_ID, ZONES, zone_center

    payments = data["payments"]
    completed = rides[rides["status"] == "completed"]
    rides["hour"] = rides["request_ts"].dt.hour
    rides["pickup_kind"] = rides["pickup_zone"].map(lambda z: ZONE_BY_ID[z].kind)
    revenue = payments.loc[payments["status"] == "completed", "amount"].sum()

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Rides", f"{len(rides):,}")
    k2.metric("Completion", f"{len(completed) / len(rides):.0%}")
    k3.metric("Cancelled", f"{(rides['status'] == 'cancelled').sum():,}")
    k4.metric("Unmet demand", f"{rides['driver_id'].isna().sum():,}")
    k5.metric("Revenue", f"${revenue:,.0f}")
    k6.metric("Avg fare", f"${completed['fare'].mean():.2f}")

    st.divider()

    left, right = st.columns(2)
    with left:
        hourly = rides.groupby(["hour", "status"]).size().reset_index(name="n")
        st.plotly_chart(
            px.bar(hourly, x="hour", y="n", color="status",
                   title="Demand by hour of day (stacked by outcome)",
                   color_discrete_map={"completed": "#2e7d32", "cancelled": "#c62828"}),
            width="stretch")
    with right:
        st.plotly_chart(
            px.scatter(completed.sample(min(len(completed), 3000)),
                       x="distance_km", y="fare", color="surge_multiplier",
                       title="Fare vs distance (color = surge multiplier)",
                       labels={"distance_km": "distance (km)"}),
            width="stretch")

    st.subheader("Zone map — pickup locations by zone type")
    sample = rides.sample(min(len(rides), 6000))
    fig = px.scatter(
        sample, x="pickup_lon", y="pickup_lat", color="pickup_kind",
        opacity=0.25, hover_data=["fare", "surge_multiplier"],
        labels={"pickup_lon": "lon", "pickup_lat": "lat", "pickup_kind": "zone type"})
    fig.add_trace(go.Scatter(
        x=[zone_center(z.zone_id)[1] for z in ZONES],
        y=[zone_center(z.zone_id)[0] for z in ZONES],
        text=[z.name for z in ZONES], mode="text",
        textfont={"size": 9, "color": "#333"}, showlegend=False))
    fig.update_yaxes(scaleanchor="x", scaleratio=1.0)
    st.plotly_chart(fig, width="stretch")

    st.subheader("Ride log (latest 500)")
    statuses = st.multiselect("filter by status", sorted(rides["status"].unique().tolist()),
                              default=sorted(rides["status"].unique().tolist()))
    cols = ["ride_id", "status", "request_ts", "pickup_zone", "dropoff_zone",
            "distance_km", "duration_min", "fare", "surge_multiplier"]
    st.dataframe(
        rides[rides["status"].isin(statuses)][cols]
        .sort_values("request_ts", ascending=False).head(500),
        width="stretch", height=360)
