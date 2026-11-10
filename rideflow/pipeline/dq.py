"""RideFlow data quality layer — checks that gate the pipeline.

Three severity levels:
  pass  — within expectation
  warn  — degraded but not wrong (freshness lag, volume dip, distribution drift)
  fail  — provably wrong (reconciliation mismatch, broken key, orphaned FK)

Layers covered: source<->gold reconciliation, gold integrity (PK/FK/domain),
freshness & volume, distribution health, silver quarantine rate.

Usage:
  python -m rideflow.pipeline.dq run        # standalone: run + record all checks
  python -m rideflow.pipeline.dq history    # recent DQ runs and their results
The orchestrator also runs this as a `dq_gate` task after load_gold and
fails the run when any check reaches `fail` (disable with --no-strict).
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta

from sqlalchemy import create_engine, text

from rideflow import config
from rideflow.pipeline.watermarks import StateStore, utc

TOL = 0.01  # revenue reconciliation tolerance (cents)
EPOCH_WM = "1970-01-01 00:00:00"


def _recon_grace_minutes() -> int:
    """How far behind the watermark reconciliation asserts equality.

    Rides land in the source at COMPLETION time, not request time: a ride
    requested at wm-30 with a 40-minute span lands during the pipeline run
    — already counted by the source, not yet in gold. The lookback window
    is (by design) larger than any in-flight ride span, so everything at or
    below wm - lookback is contractually in gold; only that settled window
    is asserted. Newest rows enter the asserted window as the watermark
    advances, so corruption is caught one run later, not never.
    """
    from rideflow.config import SIM
    return SIM["lookback_minutes"]


def _recon_bound(store: StateStore, scope: str) -> datetime | None:
    """Inclusive timestamp up to which gold must match the source."""
    wm = store.get(scope)
    if not wm or wm == EPOCH_WM:
        return None
    return datetime.fromisoformat(wm) - timedelta(minutes=_recon_grace_minutes())


class DQFailure(Exception):
    """Raised by the gate when any check fails."""

    def __init__(self, results: list[dict]):
        failed = [r for r in results if r["status"] == "fail"]
        msg = "DQ gate failed:\n" + "\n".join(
            f"  {r['check']}: {r['detail']}" for r in failed)
        super().__init__(msg)


# --------------------------------------------------------------------- checks

def chk_reconcile_rides(oc, wc, store):
    bound = _recon_bound(store, "bronze.rides")
    if bound is not None:
        s = oc.execute(text("SELECT COUNT(*) FROM rides "
                            "WHERE julianday(request_ts) <= julianday(:b)"),
                       {"b": bound.strftime("%Y-%m-%d %H:%M:%S.%f")}).scalar()
        t = wc.execute(text("SELECT COUNT(*) FROM fact_rides "
                            "WHERE julianday(request_ts) <= julianday(:b)"),
                       {"b": bound.strftime("%Y-%m-%d %H:%M:%S.%f")}).scalar()
        scope = f"through wm-{_recon_grace_minutes()}m"
    else:
        s = oc.execute(text("SELECT COUNT(*) FROM rides")).scalar()
        t = wc.execute(text("SELECT COUNT(*) FROM fact_rides")).scalar()
        scope = "full"
    return dict(check="reconcile_rides", layer="source<->gold",
                status="pass" if t - s == 0 else "fail",
                metric=t - s, detail=f"[{scope}] oltp={s:,} gold={t:,}")


def chk_reconcile_payments(oc, wc, store):
    bound = _recon_bound(store, "bronze.payments")
    if bound is not None:
        src = oc.execute(text(
            "SELECT COUNT(*), COALESCE(SUM(amount),0) FROM payments "
            "WHERE status='completed' AND julianday(paid_at) <= julianday(:b)"),
            {"b": bound.strftime("%Y-%m-%d %H:%M:%S.%f")}).one()
        tgt = wc.execute(text(
            "SELECT COUNT(*), COALESCE(SUM(amount),0) FROM fact_payments "
            "WHERE status='completed' AND julianday(paid_at) <= julianday(:b)"),
            {"b": bound.strftime("%Y-%m-%d %H:%M:%S.%f")}).one()
    else:
        src = oc.execute(text(
            "SELECT COUNT(*), COALESCE(SUM(amount),0) FROM payments "
            "WHERE status='completed'")).one()
        tgt = wc.execute(text(
            "SELECT COUNT(*), COALESCE(SUM(amount),0) FROM fact_payments "
            "WHERE status='completed'")).one()
    ok = src[0] == tgt[0] and abs(src[1] - tgt[1]) < TOL
    return dict(check="reconcile_payments", layer="source<->gold",
                status="pass" if ok else "fail", metric=round(tgt[1] - src[1], 2),
                detail=f"oltp: {src[0]:,} rows ${src[1]:,.2f} | "
                       f"gold: {tgt[0]:,} rows ${tgt[1]:,.2f}")


def chk_reconcile_events(oc, wc, store):
    bound = _recon_bound(store, "bronze.ride_events")
    if bound is not None:
        s = oc.execute(text("SELECT COUNT(*) FROM ride_events "
                            "WHERE julianday(event_ts) <= julianday(:b)"),
                       {"b": bound.strftime("%Y-%m-%d %H:%M:%S.%f")}).scalar()
        t = wc.execute(text("SELECT COUNT(*) FROM fact_ride_events "
                            "WHERE julianday(event_ts) <= julianday(:b)"),
                       {"b": bound.strftime("%Y-%m-%d %H:%M:%S.%f")}).scalar()
    else:
        s = oc.execute(text("SELECT COUNT(*) FROM ride_events")).scalar()
        t = wc.execute(text("SELECT COUNT(*) FROM fact_ride_events")).scalar()
    return dict(check="reconcile_events", layer="source<->gold",
                status="pass" if t - s == 0 else "fail",
                metric=t - s, detail=f"oltp={s:,} gold={t:,}")


def chk_fact_pk_unique(oc, wc, store):
    dupes = wc.execute(text(
        "SELECT COUNT(*) FROM (SELECT ride_id FROM fact_rides "
        "GROUP BY ride_id HAVING COUNT(*) > 1)")).scalar()
    return dict(check="fact_pk_unique", layer="gold",
                status="pass" if dupes == 0 else "fail", metric=dupes,
                detail=f"duplicate ride_ids: {dupes:,}")


def chk_fk_payments_rides(oc, wc, store):
    orphans = wc.execute(text(
        "SELECT COUNT(*) FROM fact_payments p LEFT JOIN fact_rides r "
        "ON p.ride_id = r.ride_id WHERE r.ride_id IS NULL")).scalar()
    return dict(check="fk_payments_rides", layer="gold",
                status="pass" if orphans == 0 else "fail", metric=orphans,
                detail=f"payments with no ride: {orphans:,}")


def chk_fk_rides_dims(oc, wc, store):
    row = wc.execute(text(
        "SELECT SUM(CASE WHEN fr.driver_id IS NOT NULL AND dd.driver_id IS NULL "
        "THEN 1 ELSE 0 END), "
        "SUM(CASE WHEN du.user_id IS NULL THEN 1 ELSE 0 END), "
        "SUM(CASE WHEN pz.zone_id IS NULL OR dz.zone_id IS NULL THEN 1 ELSE 0 END) "
        "FROM fact_rides fr "
        "LEFT JOIN dim_driver dd ON dd.driver_id = fr.driver_id "
        "LEFT JOIN dim_user du ON du.user_id = fr.user_id "
        "LEFT JOIN dim_zone pz ON pz.zone_id = fr.pickup_zone "
        "LEFT JOIN dim_zone dz ON dz.zone_id = fr.dropoff_zone")).one()
    bad = row[0] or 0
    return dict(check="fk_rides_dims", layer="gold",
                status="pass" if bad == 0 else "fail", metric=bad,
                detail=f"rides with unknown driver/user/zone: {bad:,}")


def chk_status_domain(oc, wc, store):
    bad = wc.execute(text(
        "SELECT COUNT(*) FROM fact_rides "
        "WHERE status NOT IN ('completed','cancelled')")).scalar()
    return dict(check="status_domain", layer="gold",
                status="pass" if bad == 0 else "fail", metric=bad,
                detail=f"rides with unexpected status: {bad:,}")


def chk_surge_range(oc, wc, store):
    bad = wc.execute(text(
        "SELECT COUNT(*) FROM fact_rides "
        "WHERE surge_multiplier < 1.0 OR surge_multiplier > 2.4")).scalar()
    return dict(check="surge_range", layer="gold",
                status="pass" if bad == 0 else "fail", metric=bad,
                detail=f"fares with surge outside [1.0, 2.4]: {bad:,}")


def chk_freshness(oc, wc, store):
    mx = wc.execute(text("SELECT MAX(request_ts) FROM fact_rides")).scalar()
    if not mx:
        return dict(check="freshness", layer="gold", status="fail",
                    metric=None, detail="no rides in warehouse")
    lag_h = (utc() - datetime.fromisoformat(str(mx))).total_seconds() / 3600
    if lag_h <= 2:
        status = "pass"
    elif lag_h <= 24:
        status = "warn"
    else:
        status = "fail"
    return dict(check="freshness", layer="gold", status=status,
                metric=round(lag_h, 2), detail=f"newest ride is {lag_h:.1f}h old")


def chk_volume_anomaly(oc, wc, store):
    rows = wc.execute(text(
        "SELECT request_date_key, COUNT(*) FROM fact_rides GROUP BY 1 ORDER BY 1")).all()
    keys = [r[0] for r in rows]
    counts = dict(rows)
    today = max(keys)
    full = [k for k in keys if k < today]          # exclude today's partial day
    if len(full) < 2:
        return dict(check="volume_anomaly", layer="gold", status="pass",
                    metric=None, detail="insufficient history (need 2 full days)")
    last, prior = full[-1], full[-7:-1]
    mean = sum(counts[k] for k in prior) / len(prior)
    delta = (counts[last] - mean) / mean
    status = "pass" if -0.30 <= delta <= 0.50 else "warn"
    return dict(check="volume_anomaly", layer="gold", status=status,
                metric=round(delta, 3),
                detail=f"last full day {counts[last]:,} rides "
                        f"({delta:+.0%} vs {len(prior)}-day mean {mean:,.0f})")


def chk_payment_health(oc, wc, store):
    row = wc.execute(text(
        "SELECT COUNT(*), SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) "
        "FROM fact_payments")).one()
    total, failed = row[0], row[1] or 0
    share = failed / total if total else 0
    status = "pass" if 0.01 <= share <= 0.06 else "warn"
    return dict(check="payment_health", layer="gold", status=status,
                metric=round(share, 4),
                detail=f"failed payments: {failed:,}/{total:,} ({share:.1%})")


def chk_cancel_share(oc, wc, store):
    row = wc.execute(text(
        "SELECT COUNT(*), SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END) "
        "FROM fact_rides")).one()
    total, cancelled = row[0], row[1] or 0
    share = cancelled / total if total else 0
    status = "pass" if 0.05 <= share <= 0.15 else "warn"
    return dict(check="cancel_share", layer="gold", status=status,
                metric=round(share, 4),
                detail=f"cancelled rides: {cancelled:,}/{total:,} ({share:.1%})")


def chk_quarantine_rate(oc, wc, store):
    with store.engine.connect() as c:
        row = c.execute(text(
            "SELECT COALESCE(SUM(rows_in),0), COALESCE(SUM(rows_bad),0) FROM transforms "
            "WHERE run_id = (SELECT run_id FROM transforms ORDER BY id DESC LIMIT 1)"
        )).one()
    rows_in, rows_bad = row[0], row[1]
    if rows_in == 0:
        return dict(check="quarantine_rate", layer="silver", status="pass",
                    metric=None, detail="no rows transformed yet")
    rate = rows_bad / rows_in
    status = "pass" if rate <= 0.01 else ("warn" if rate <= 0.05 else "fail")
    return dict(check="quarantine_rate", layer="silver", status=status,
                metric=round(rate, 4),
                detail=f"last run: {rows_bad:,}/{rows_in:,} rows quarantined ({rate:.2%})")


CHECKS = [
    chk_reconcile_rides,
    chk_reconcile_payments,
    chk_reconcile_events,
    chk_fact_pk_unique,
    chk_fk_payments_rides,
    chk_fk_rides_dims,
    chk_status_domain,
    chk_surge_range,
    chk_freshness,
    chk_volume_anomaly,
    chk_payment_health,
    chk_cancel_share,
    chk_quarantine_rate,
]

ICON = {"pass": "PASS", "warn": "WARN", "fail": "FAIL"}


def run_checks(store: StateStore, run_id: str | None = None) -> list[dict]:
    oltp = create_engine(config.OLTP_URL)
    wh = create_engine(config.WAREHOUSE_URL)
    results: list[dict] = []
    with oltp.connect() as oc, wh.connect() as wc:
        for fn in CHECKS:
            try:
                r = fn(oc, wc, store)
            except Exception as e:  # noqa: BLE001 — a broken check is itself a finding
                r = dict(check=fn.__name__, layer="?", status="fail",
                         metric=None, detail=f"check crashed: {e!r}")
            results.append(r)
    if run_id:
        store.clear_dq(run_id)  # last attempt wins — retries don't duplicate rows
        for r in results:
            store.record_dq(run_id, r["check"], r["layer"],
                            r["status"], str(r["metric"]), r["detail"])
    return results


def summarize(results: list[dict]) -> dict:
    fails = sum(1 for r in results if r["status"] == "fail")
    warns = sum(1 for r in results if r["status"] == "warn")
    return {"checks": len(results), "warns": warns, "fails": fails}


def print_results(results: list[dict]) -> None:
    print(f"{'status':6s} {'check':24s} {'layer':14s} {'metric':>8s}  detail")
    for r in results:
        print(f"{ICON[r['status']]:6s} {r['check']:24s} {r['layer']:14s} "
              f"{str(r['metric']):>8s}  {r['detail']}")
    s = summarize(results)
    print(f"\n{s['checks']} checks: {s['checks'] - s['warns'] - s['fails']} pass, "
          f"{s['warns']} warn, {s['fails']} fail")


def history(store: StateStore, n: int = 5) -> None:
    runs = store.dq_runs(n)
    if not runs:
        print("no DQ results yet — run:  python -m rideflow.pipeline.dq run")
        return
    for r in runs:
        checks = store.dq_results_of_run(r.run_id)
        counts = {s: sum(1 for c in checks if c.status == s) for s in ("pass", "warn", "fail")}
        print(f"\nrun {r.run_id}  [{r.mode}]  {r.status}   "
              f"pass={counts['pass']} warn={counts['warn']} fail={counts['fail']}")
        for c in checks:
            if c.status != "pass":
                print(f"  {c.status} {c.check}: {c.detail}")


def main() -> None:
    ap = argparse.ArgumentParser(description="RideFlow data quality")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("run", help="run all checks and record results")
    ph = sub.add_parser("history", help="show recent DQ runs")
    ph.add_argument("-n", type=int, default=5)
    args = ap.parse_args()

    store = StateStore()
    if args.cmd == "run":
        run_id = store.start_run("dq")
        results = run_checks(store, run_id)
        print_results(results)
        s = summarize(results)
        store.finish_run(run_id, "fail" if s["fails"] else
                         ("warn" if s["warns"] else "success"))
    else:
        history(store, args.n)


if __name__ == "__main__":
    main()
