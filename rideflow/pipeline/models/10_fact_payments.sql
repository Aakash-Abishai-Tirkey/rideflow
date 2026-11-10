-- materialized: incremental (upsert on payment_id)
INSERT OR REPLACE INTO fact_payments
SELECT
  p.payment_id,
  p.ride_id,
  p.amount,
  p.method,
  p.status,
  p.paid_at,
  CAST(strftime('%Y%m%d', p.paid_at) AS INTEGER) AS paid_date_key
FROM stg_payments p;
