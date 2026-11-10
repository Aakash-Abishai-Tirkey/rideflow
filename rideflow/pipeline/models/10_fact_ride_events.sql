-- materialized: incremental (upsert on event_id)
INSERT OR REPLACE INTO fact_ride_events
SELECT
  e.event_id,
  e.ride_id,
  e.event_type,
  e.event_ts
FROM stg_ride_events e;
