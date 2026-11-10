"""Gold layer: silver -> star schema in the warehouse.

A miniature dbt: models are .sql files in pipeline/models/, ordered by
filename prefix (10_ facts, 20_ dims). Facts upsert by primary key
(idempotent re-runs); small dims are fully rebuilt each run.
dim_zone and dim_date are generated (static / calendar) and loaded directly.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

from rideflow import config
from rideflow.pipeline.extract import TABLES
from rideflow.pipeline.transform import read_partition, silver_partitions
from rideflow.pipeline.watermarks import StateStore
from rideflow.simulator.city import ZONES, zone_center

MODELS_DIR = Path(__file__).parent / "models"
STAGED = ["rides", "payments", "ride_events", "users", "drivers"]


def _run_sql(engine, sql: str) -> None:
    for stmt in (s for s in sql.split(";") if s.strip()):
        with engine.begin() as c:
            c.exec_driver_sql(stmt)


def _stage(store: StateStore, engine, table: str) -> int:
    cfg = TABLES[table]
    if cfg.get("full"):
        parts = silver_partitions(table)
        if not parts:
            n = 0
            df = pd.DataFrame()
        else:  # reference table: only the latest snapshot partition
            df = read_partition(parts[-1])
            n = len(df)
    else:
        since = store.get(f"silver.{table}", "0000-00-00")
        parts = [p for p in silver_partitions(table) if p.name >= f"dt={since}"]
        frames = [read_partition(p) for p in parts]
        df = pd.concat(frames) if frames else pd.DataFrame()
        df = df.drop_duplicates(subset=[cfg["pk"]], keep="last")
        n = len(df)
        if parts:
            store.set(f"silver.{table}", parts[-1].name[3:])

    df.to_sql(f"stg_{table}", engine, if_exists="replace", index=False)
    return n


def _load_zone_dim(engine) -> None:
    df = pd.DataFrame([{
        "zone_id": z.zone_id, "zone_name": z.name, "kind": z.kind,
        "grid_x": z.x, "grid_y": z.y,
        "center_lat": zone_center(z.zone_id)[0],
        "center_lon": zone_center(z.zone_id)[1],
    } for z in ZONES])
    df.to_sql("dim_zone", engine, if_exists="replace", index=False)


def _load_date_dim(engine) -> None:
    with engine.connect() as c:
        span = pd.read_sql(
            """SELECT MIN(request_ts) AS a, MAX(request_ts) AS b FROM fact_rides
               UNION ALL SELECT MIN(paid_at), MAX(paid_at) FROM fact_payments""",
            c)
    dates = pd.to_datetime(span.stack()).dropna()
    if dates.empty:
        return
    rng = pd.date_range(dates.min().normalize(), dates.max().normalize(), freq="D")
    df = pd.DataFrame({
        "date_key": [int(d.strftime("%Y%m%d")) for d in rng],
        "full_date": rng.date,
        "year": rng.year, "quarter": rng.quarter,
        "month": rng.month, "month_name": rng.strftime("%B"),
        "day": rng.day, "day_of_week": rng.dayofweek,
        "day_name": rng.strftime("%A"),
        "is_weekend": (rng.dayofweek >= 5).astype(int),
    })
    df.to_sql("dim_date", engine, if_exists="replace", index=False)


def load_gold(store: StateStore, run_id: str) -> dict:
    config.DATA_DIR.mkdir(exist_ok=True)
    engine = create_engine(config.WAREHOUSE_URL)

    _run_sql(engine, (MODELS_DIR / "schema.sql").read_text(encoding="utf-8"))
    staged = {t: _stage(store, engine, t) for t in STAGED}
    _load_zone_dim(engine)

    models = [p for p in sorted(MODELS_DIR.glob("*.sql")) if p.name != "schema.sql"]
    for path in models:
        _run_sql(engine, path.read_text(encoding="utf-8"))
    _load_date_dim(engine)  # spans depend on materialized facts

    with engine.connect() as c:
        gold = {t: c.exec_driver_sql(f"SELECT COUNT(*) FROM {t}").scalar()
                for t in ("fact_rides", "fact_payments", "fact_ride_events",
                          "dim_user", "dim_driver")}
    return {"staged": staged, "gold": gold}
