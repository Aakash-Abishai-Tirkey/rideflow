-- Physical DDL for gold facts. Primary keys make incremental upserts
-- (INSERT OR REPLACE) idempotent: re-running a day never duplicates rows.
CREATE TABLE IF NOT EXISTS fact_rides (
  ride_id TEXT PRIMARY KEY,
  user_id TEXT,
  driver_id TEXT,
  pickup_zone INTEGER,
  dropoff_zone INTEGER,
  request_date_key INTEGER,
  request_ts TEXT,
  completed_ts TEXT,
  status TEXT,
  distance_km REAL,
  duration_min INTEGER,
  fare REAL,
  surge_multiplier REAL,
  fare_per_km REAL,
  is_airport INTEGER
);
CREATE TABLE IF NOT EXISTS fact_payments (
  payment_id TEXT PRIMARY KEY,
  ride_id TEXT,
  amount REAL,
  method TEXT,
  status TEXT,
  paid_at TEXT,
  paid_date_key INTEGER
);
CREATE TABLE IF NOT EXISTS fact_ride_events (
  event_id TEXT PRIMARY KEY,
  ride_id TEXT,
  event_type TEXT,
  event_ts TEXT
);
