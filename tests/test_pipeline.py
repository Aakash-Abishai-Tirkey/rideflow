"""End-to-end: the orchestrator's whole DAG on a tiny platform."""
from __future__ import annotations

import pytest
from sqlalchemy import create_engine, func, select, text

from rideflow import config
from rideflow.oltp import Payment, Ride, RideEvent, get_engine
from rideflow.orchestrator import execute
from rideflow.pipeline.dq import DQFailure
from rideflow.pipeline.watermarks import StateStore


def oltp_count(table) -> int:
    with get_engine().connect() as c:
        return c.scalar(select(func.count()).select_from(table))


def gold_count(table: str) -> int:
    with create_engine(config.WAREHOUSE_URL).connect() as c:
        return c.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()


def test_pipeline_lifecycle(small_sim):
    store = StateStore()

    # cold start: gold mirrors the source exactly
    r1 = execute(store)
    assert r1["gold"]["fact_rides"] == oltp_count(Ride)
    assert r1["gold"]["fact_payments"] == oltp_count(Payment)
    assert r1["gold"]["fact_ride_events"] == oltp_count(RideEvent)
    assert store.last_runs(1)[0].status == "success"

    # incremental: no new data -> gold unchanged
    r2 = execute(store)
    assert r2["gold"] == r1["gold"]

    # backfill: full reload -> still identical (idempotent)
    r3 = execute(store, backfill=True)
    assert r3["gold"] == r1["gold"]


def test_dq_gate_blocks_and_heals_corruption(sim_factory):
    sim_factory(days=2)  # older rides live in older silver partitions
    store = StateStore()
    execute(store)  # healthy baseline

    # delete the OLDEST ride: it sits in a silver partition that incremental
    # staging (latest date only) never re-loads, so only a backfill can heal
    with create_engine(config.WAREHOUSE_URL).begin() as c:
        ride = c.execute(text(
            "SELECT ride_id FROM fact_rides ORDER BY request_ts ASC LIMIT 1")).scalar()
        c.execute(text("DELETE FROM fact_rides WHERE ride_id = :r"), {"r": ride})

    # the next run must fail loudly at the gate (after retries)
    with pytest.raises(DQFailure) as exc:
        execute(store)
    assert "reconcile_rides" in str(exc.value)
    assert store.last_runs(1)[0].status == "failed"

    # incremental cannot heal — this is exactly when a backfill is required
    with pytest.raises(DQFailure):
        execute(store)

    # backfill restores the row and the gate goes green
    r = execute(store, backfill=True)
    assert r["gold"]["fact_rides"] == oltp_count(Ride)
    assert store.last_runs(1)[0].status == "success"
    assert gold_count("fact_rides") == oltp_count(Ride)
