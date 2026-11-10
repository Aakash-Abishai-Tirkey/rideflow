"""One-command live demo: simulator + continuous incremental pipeline.

Usage:
  python -m rideflow.demo                     # sim speed 1x, pipeline every 45s
  python -m rideflow.demo --speed 2 --interval 30

Pair it with the dashboard (enable "Live mode" in its sidebar):
  streamlit run rideflow/dashboard.py

Ctrl+C stops cleanly: the simulator thread flushes rides still in flight,
so the OLTP is never left mid-lifecycle.
"""
from __future__ import annotations

import argparse
import threading
import time
from datetime import timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from rideflow.orchestrator import execute
from rideflow.pipeline.watermarks import StateStore
from rideflow.simulator.engine import Simulator, utcnow


def _sim_loop(sim: Simulator, stop: threading.Event, sec_per_min: float,
              start: datetime) -> None:
    """Advance simulated time in a background thread (own engine/connection).

    `start` continues from the newest data (max request_ts + 1m): a fresh
    process would otherwise restart at real-utcnow, which can sit BEHIND a
    watermark left by an earlier session whose sim clock ran ahead — every
    new ride would land below the extraction cutoff and gold would freeze.
    """
    now = start
    while not stop.is_set():
        sim._process_minute(now)          # simulator's internal tick API
        now += timedelta(minutes=1)
        stop.wait(sec_per_min)
    # flush rides still in flight so the source DB isn't left mid-lifecycle
    sim._drain(now + timedelta(hours=2))


def main() -> None:
    ap = argparse.ArgumentParser(description="RideFlow live demo")
    ap.add_argument("--speed", type=float, default=1.0,
                    help="simulated minutes per real second (default 1)")
    ap.add_argument("--interval", type=float, default=45,
                    help="seconds between incremental pipeline runs (default 45)")
    args = ap.parse_args()

    from rideflow.oltp import Ride, User

    sim = Simulator()
    with Session(sim.engine) as s:
        if s.scalar(select(func.count()).select_from(User)) == 0:
            sim.populate()
        else:
            sim.resume()
        # continue the sim clock from the newest data, not real-utcnow
        start = s.scalar(select(func.max(Ride.request_ts)))
    if start is None:
        start = utcnow()
    start = (start + timedelta(minutes=1)).replace(second=0, microsecond=0)

    stop = threading.Event()
    thread = threading.Thread(
        target=_sim_loop, args=(sim, stop, args.speed, start), daemon=True)
    thread.start()
    print(f"simulator: continuing from {start:%Y-%m-%d %H:%M} "
          f"(1 simulated minute per {args.speed}s; Ctrl+C to stop)")
    print(f"pipeline : incremental run every {args.interval}s\n")

    passes = 0
    try:
        while True:
            time.sleep(args.interval)
            passes += 1
            try:
                result = execute(StateStore())
                gold = result.get("gold", {})
                print(f"[pass {passes}] gold: "
                      + ", ".join(f"{k}={v:,}" for k, v in gold.items()) + "\n")
            except Exception as e:  # noqa: BLE001 — a bad cycle shouldn't kill the demo
                print(f"[pass {passes}] pipeline error (retrying next cycle): {e!r}\n")
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        thread.join(timeout=10)
        print("\ndemo stopped — final source stats:")
        for k, v in sim.stats().items():
            print(f"  {k:>16}: {v:,}" if isinstance(v, int) else f"  {k:>16}: {v}")


if __name__ == "__main__":
    main()
