"""RideFlow orchestrator — a miniature Airflow.

DAG:      extract_<table> -> transform_<table> for each of 6 source tables,
          all feeding load_gold (medallion: bronze -> silver -> gold star schema).
Retries:  tenacity, 3 attempts with exponential backoff per task.
State:    every run and task attempt recorded in state.db (runs / task_runs).
Backfill: watermarks reset -> full re-extract; every layer is idempotent,
          so the result is byte-identical to the original load.

Usage:
  python -m rideflow.orchestrator run             # incremental
  python -m rideflow.orchestrator run --backfill # full reload
  python -m rideflow.orchestrator status          # run history
"""
from __future__ import annotations

import argparse
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from tenacity import Retrying, stop_after_attempt, wait_exponential

from rideflow.pipeline.extract import TABLES, extract_table
from rideflow.pipeline.load import STAGED, load_gold
from rideflow.pipeline.transform import transform_table
from rideflow.pipeline.watermarks import StateStore, utc


@dataclass
class Task:
    name: str
    fn: object
    depends: list[str] = field(default_factory=list)


def build_tasks(store: StateStore, run_id: str) -> list[Task]:
    tasks: list[Task] = []

    for table in TABLES:
        def make_extract(table=table):
            def fn(ctx: dict) -> dict:
                return extract_table(store, run_id, table)
            return fn

        extract_name = f"extract_{table}"
        tasks.append(Task(extract_name, make_extract()))

        def make_transform(table=table):
            def fn(ctx: dict) -> dict:
                return transform_table(store, run_id, table)
            return fn

        tasks.append(Task(f"transform_{table}", make_transform(), [extract_name]))

    def load_fn(ctx: dict) -> dict:
        return load_gold(store, run_id)

    tasks.append(Task("load_gold", load_fn, [f"transform_{t}" for t in STAGED]))
    return tasks


def execute(store: StateStore, backfill: bool = False) -> dict:
    if backfill:
        store.clear_prefix("bronze.")
        store.clear_prefix("silver.")

    run_id = store.start_run("backfill" if backfill else "incremental")
    tasks = build_tasks(store, run_id)
    ctx: dict = {}
    pending = list(tasks)
    done: set[str] = set()

    while pending:
        runnable = [t for t in pending if all(d in done for d in t.depends)]
        if not runnable:
            store.finish_run(run_id, "failed")
            raise RuntimeError(f"dependency cycle among {[t.name for t in pending]}")
        for task in runnable:
            started = time.perf_counter()
            retrying = Retrying(stop=stop_after_attempt(3),
                                wait=wait_exponential(multiplier=0.4, max=5),
                                reraise=True)
            try:
                result = retrying(task.fn, ctx)
                ctx[task.name] = result
                detail = ", ".join(f"{k}={v}" for k, v in result.items()) \
                    if isinstance(result, dict) else ""
                store.record_task(run_id, task.name, "success",
                                  retrying.statistics.get("attempt_number", 1),
                                  utc(), detail=detail[:500])
                print(f"  ok    {task.name:24s} ({time.perf_counter() - started:.1f}s) {detail[:80]}")
            except Exception as e:  # noqa: BLE001 — record then abort downstream
                store.record_task(run_id, task.name, "failed",
                                  retrying.statistics.get("attempt_number", 1),
                                  utc(), error=repr(e))
                store.finish_run(run_id, "failed")
                print(f"  FAIL  {task.name}: {e!r}")
                raise
            done.add(task.name)
            pending.remove(task)

    store.finish_run(run_id, "success")
    print(f"\nrun {run_id} complete")
    gold = ctx.get("load_gold", {}).get("gold", {})
    if gold:
        print("  gold: " + ", ".join(f"{k}={v:,}" for k, v in gold.items()))
    return {"run_id": run_id, **ctx.get("load_gold", {})}


def status(store: StateStore, n: int = 5) -> None:
    runs = store.last_runs(n)
    if not runs:
        print("no runs yet — start one:  python -m rideflow.orchestrator run")
        return
    for r in reversed(runs):
        dur = ""
        if r.finished_at and r.started_at:
            dur = f" {(r.finished_at - r.started_at).total_seconds():.1f}s"
        print(f"\nrun {r.run_id}  [{r.mode}]  {r.status}{dur}")
        for t in store.tasks_of_run(r.run_id):
            line = f"  {t.status:8s} {t.task:24s} attempts={t.attempts}"
            if t.detail:
                line += f"  {t.detail[:90]}"
            if t.error:
                line += f"\n          error: {t.error[:160]}"
            print(line)


def main() -> None:
    ap = argparse.ArgumentParser(description="RideFlow orchestrator")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p_run = sub.add_parser("run", help="execute the pipeline DAG")
    p_run.add_argument("--backfill", action="store_true",
                       help="reset watermarks and reload everything")
    p_status = sub.add_parser("status", help="show run history")
    p_status.add_argument("-n", type=int, default=5)
    args = ap.parse_args()

    store = StateStore()
    if args.cmd == "status":
        status(store, args.n)
    else:
        execute(store, backfill=args.backfill)


if __name__ == "__main__":
    main()
