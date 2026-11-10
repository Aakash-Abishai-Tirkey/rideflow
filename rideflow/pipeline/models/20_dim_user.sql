-- materialized: table (full rebuild — small dimension, joined to full history)
-- depends_on: fact_rides, stg_users, dim_zone
DROP TABLE IF EXISTS dim_user;
CREATE TABLE dim_user AS
SELECT
  u.user_id AS user_key,
  u.user_id,
  u.signup_ts,
  u.home_zone,
  z.zone_name AS home_zone_name,
  u.rating AS user_rating,
  COUNT(fr.ride_id) AS rides_completed,
  ROUND(COALESCE(SUM(fr.fare), 0), 2) AS lifetime_spend_usd
FROM stg_users u
LEFT JOIN dim_zone z ON z.zone_id = u.home_zone
LEFT JOIN fact_rides fr ON fr.user_id = u.user_id AND fr.status = 'completed'
GROUP BY u.user_id;
