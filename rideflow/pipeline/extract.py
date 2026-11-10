"""Bronze layer: watermarked incremental extraction from the OLTP source.

- Event tables (rides, events, gps, payments) use a timestamp watermark
  with a lookback window so late-landing rows are never missed.
- Small reference tables (users, drivers) are full snapshots every run
  (cheap, and the silver snapshots become free SCD history).
- Bronze is append-only and immutable: raw rows exactly as extracted,
  partitioned by ingestion date.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import select

from rideflow.config import BRONZE_DIR, SIM
from rideflow.oltp import Driver, GpsPing, Payment, Ride, RideEvent, User, get_engine
from rideflow.contracts import (DriverRow, GpsPingRow, PaymentRow, RideEventRow,
                                RideRow, UserRow)
from rideflow.pipeline.watermarks import StateStore

TABLES: dict[str, dict] = {
    "rides":       dict(orm=Ride,      ts="request_ts", pk="ride_id",   model=RideRow),
    "ride_events": dict(orm=RideEvent, ts="event_ts",   pk="event_id",  model=RideEventRow),
    "gps_pings":   dict(orm=GpsPing,   ts="ts",         pk="ping_id",   model=GpsPingRow),
    "payments":    dict(orm=Payment,   ts="paid_at",    pk="payment_id", model=PaymentRow),
    "users":       dict(orm=User,      ts=None,         pk="user_id",   model=UserRow,   full=True),
    "drivers":     dict(orm=Driver,    ts=None,         pk="driver_id", model=DriverRow, full=True),
}

EPOCH = "1970-01-01 00:00:00"


def extract_table(store: StateStore, run_id: str, table: str,
                  run_date: str | None = None) -> dict:
    cfg = TABLES[table]
    engine = get_engine()
    stmt = select(cfg["orm"])
    wm_before = None

    if not cfg.get("full"):
        wm_before = store.get(f"bronze.{table}", EPOCH)
        cutoff = (datetime.fromisoformat(wm_before)
                  - timedelta(minutes=SIM["lookback_minutes"]))
        stmt = stmt.where(getattr(cfg["orm"], cfg["ts"]) > cutoff)

    with engine.connect() as c:
        df = pd.read_sql(stmt, c)

    rows = len(df)
    if rows:
        # Overlapping lookback windows can re-read the same PK: keep one copy.
        df = df.drop_duplicates(subset=[cfg["pk"]], keep="last")

    wm_after = wm_before
    if rows and not cfg.get("full"):
        wm_after = str(df[cfg["ts"]].max())

    file = None
    if rows:
        dt = run_date or run_id[:8]
        path = BRONZE_DIR / table / f"dt={dt}" / f"batch-{run_id}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        # pandas renders NULL datetimes as NaT: sanitize to None -> JSON null
        clean = df.astype(object).where(pd.notna(df), None)
        with open(path, "w", encoding="utf-8") as f:
            for row in clean.to_dict("records"):
                f.write(json.dumps(row, default=str) + "\n")
        file = str(path)

    if wm_after is not None:
        store.set(f"bronze.{table}", wm_after)
    store.record_extraction(run_id, table, file, rows, wm_before, wm_after)
    return {"rows": rows, "file": file, "wm_before": wm_before, "wm_after": wm_after}
