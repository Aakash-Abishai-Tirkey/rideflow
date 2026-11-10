-- materialized: incremental (upsert on ride_id)
INSERT OR REPLACE INTO fact_rides
SELECT
  r.ride_id,
  r.user_id,
  r.driver_id,
  r.pickup_zone,
  r.dropoff_zone,
  CAST(strftime('%Y%m%d', r.request_ts) AS INTEGER) AS request_date_key,
  r.request_ts,
  r.completed_ts,
  r.status,
  r.distance_km,
  r.duration_min,
  r.fare,
  r.surge_multiplier,
  ROUND(r.fare / NULLIF(r.distance_km, 0), 2) AS fare_per_km,
  CASE WHEN r.pickup_zone = 16 OR r.dropoff_zone = 16 THEN 1 ELSE 0 END AS is_airport
FROM stg_rides r;
