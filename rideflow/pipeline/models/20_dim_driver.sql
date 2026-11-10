-- materialized: table (full rebuild — small dimension, joined to full history)
-- depends_on: fact_rides, stg_drivers, dim_zone
DROP TABLE IF EXISTS dim_driver;
CREATE TABLE dim_driver AS
SELECT
  d.driver_id AS driver_key,
  d.driver_id,
  d.signup_ts,
  d.home_zone,
  z.zone_name AS home_zone_name,
  d.vehicle,
  d.rating,
  COUNT(fr.ride_id) AS rides_completed,
  ROUND(COALESCE(SUM(fr.fare), 0), 2) AS revenue_generated_usd
FROM stg_drivers d
LEFT JOIN dim_zone z ON z.zone_id = d.home_zone
LEFT JOIN fact_rides fr ON fr.driver_id = d.driver_id AND fr.status = 'completed'
GROUP BY d.driver_id;
