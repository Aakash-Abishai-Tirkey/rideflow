"""Bronze extraction: watermark + lookback-window semantics."""
from __future__ import annotations

import json
from datetime import datetime, timedelta

from sqlalchemy import func, select

from conftest import insert_rides, ride_dict
from rideflow.oltp import Ride, get_engine
from rideflow.pipeline.extract import extract_table
from rideflow.pipeline.watermarks import StateStore


def ids_of(file: str | None) -> set[str]:
    if not file:
        return set()
    with open(file, encoding="utf-8") as f:
        return {json.loads(line)["ride_id"] for line in f if line.strip()}


def test_first_extract_pulls_everything(small_sim):
    store = StateStore()
    with get_engine().connect() as c:
        total = c.scalar(select(func.count()).select_from(Ride))
        mx = c.scalar(select(func.max(Ride.request_ts)))

    result = extract_table(store, store.start_run("t1"), "rides")

    assert result["rows"] == total == small_sim[1]["rides"]
    assert store.get("bronze.rides") == str(mx)


def test_second_extract_adds_nothing_new(small_sim):
    store = StateStore()
    first = extract_table(store, store.start_run("t1"), "rides")
    wm = store.get("bronze.rides")

    second = extract_table(store, store.start_run("t2"), "rides")

    # lookback re-reads overlapping rows — but nothing NEW may appear
    assert ids_of(second["file"]) <= ids_of(first["file"])
    assert store.get("bronze.rides") == wm  # watermark never regresses


def test_lookback_and_watermark_boundaries(small_sim):
    store = StateStore()
    extract_table(store, store.start_run("t1"), "rides")
    wm = datetime.fromisoformat(store.get("bronze.rides"))

    insert_rides([
        ride_dict("r_late", wm - timedelta(minutes=5)),    # late arrival, inside lookback
        ride_dict("r_new", wm + timedelta(hours=1)),       # genuinely new
        ride_dict("r_stale", wm - timedelta(hours=3)),    # outside lookback — missed
    ])
    result = extract_table(store, store.start_run("t2"), "rides")
    ids = ids_of(result["file"])

    assert "r_late" in ids        # caught by the 90-minute lookback window
    assert "r_new" in ids         # caught by the watermark
    assert "r_stale" not in ids   # too old — this is why backfills exist
    assert store.get("bronze.rides") == str(wm + timedelta(hours=1))
