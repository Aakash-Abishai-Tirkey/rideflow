"""Central configuration: paths, database URLs, simulation parameters."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"

OLTP_URL = f"sqlite:///{DATA_DIR / 'oltp.db'}"        # source system ("production app")
WAREHOUSE_URL = f"sqlite:///{DATA_DIR / 'warehouse.db'}"  # analytics star schema
STATE_URL = f"sqlite:///{DATA_DIR / 'state.db'}"      # orchestrator + watermarks + DQ results

BRONZE_DIR = DATA_DIR / "bronze"   # raw, immutable, partitioned by ingestion date
SILVER_DIR = DATA_DIR / "silver"   # validated, deduped, typed

SIM = {
    "seed": 42,                 # reproducible datasets (good for tests + demos)
    "drivers": 120,
    "users": 800,
    "base_rides_per_hour": 60,  # off-peak baseline; rush hour multiplies this
    "peak_multiplier": 3.2,
    "cancel_prob": 0.08,        # rides cancelled mid-trip
    "payment_fail_prob": 0.03,  # payments that stay failed (DQ reconciliation fodder)
    "avg_speed_kmh": 28,
    "fare_base": 2.50,
    "fare_per_km": 1.10,
    "fare_per_min": 0.35,
    "tip_prob": 0.15,
    "lookback_minutes": 90,     # pipeline re-reads a window to catch late-arriving rows
}
