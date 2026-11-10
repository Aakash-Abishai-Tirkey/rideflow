"""Shared fixtures: every test gets fully isolated data paths.

Pipeline modules read paths from rideflow.config at call time, so patching
the config attributes redirects the entire system — OLTP, warehouse, state
store, bronze and silver — into pytest's per-test tmp_path. No test ever
touches the real ./data directory.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from sqlalchemy import insert

from rideflow import config
from rideflow.oltp import Ride, get_engine, init_db

SMALL_SIM = {"users": 60, "drivers": 20, "base_rides_per_hour": 30}


def ride_dict(ride_id: str, ts: datetime, **over) -> dict:
    """A contract-valid ride row, overridable field by field."""
    row = dict(
        ride_id=ride_id, user_id="u_test", driver_id="d_test", status="completed",
        request_ts=ts, pickup_zone=3, dropoff_zone=11,
        pickup_lat=40.70, pickup_lon=-74.00, dropoff_lat=40.72, dropoff_lon=-74.02,
        distance_km=5.2, duration_min=14, fare=13.75, surge_multiplier=1.2,
        completed_ts=ts + timedelta(minutes=14),
    )
    row.update(over)
    return row


def insert_rides(rows: list[dict]) -> None:
    """Insert ride rows directly into the (patched) OLTP, as if by the app."""
    engine = get_engine()
    init_db(engine)
    with engine.begin() as c:
        c.execute(insert(Ride), rows)


@pytest.fixture
def data_env(tmp_path, monkeypatch):
    """Isolated pipeline environment: all paths/URLs under tmp_path."""
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "OLTP_URL", f"sqlite:///{tmp_path / 'oltp.db'}")
    monkeypatch.setattr(config, "WAREHOUSE_URL", f"sqlite:///{tmp_path / 'warehouse.db'}")
    monkeypatch.setattr(config, "STATE_URL", f"sqlite:///{tmp_path / 'state.db'}")
    monkeypatch.setattr(config, "BRONZE_DIR", tmp_path / "bronze")
    monkeypatch.setattr(config, "SILVER_DIR", tmp_path / "silver")
    return tmp_path


@pytest.fixture
def sim_factory(data_env, monkeypatch):
    """Build tiny deterministic platforms: sim_factory(days=N)."""
    def _make(days: int = 1):
        for key, value in SMALL_SIM.items():
            monkeypatch.setitem(config.SIM, key, value)
        from rideflow.simulator.engine import Simulator

        sim = Simulator()
        sim.reset()
        sim.populate()
        stats = sim.backfill(days=days)
        return sim, stats
    return _make


@pytest.fixture
def small_sim(sim_factory):
    """A tiny deterministic platform: 1 simulated day, small fleet (~2s)."""
    return sim_factory(days=1)


@pytest.fixture
def small_world(small_sim):
    """Seeded OLTP plus one successful pipeline run through the whole DAG."""
    from rideflow.orchestrator import execute
    from rideflow.pipeline.watermarks import StateStore

    store = StateStore()
    result = execute(store)
    return result
