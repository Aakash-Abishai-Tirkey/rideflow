"""Pipeline state store: watermarks + run/task history (SQLite).

Watermarks make extraction incremental: each run asks for rows newer than
the last run's high-water mark (minus a lookback window for late arrivals).
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import (Column, DateTime, Integer, MetaData, String, Table,
                        Text, create_engine, delete, insert, select)

from rideflow.config import DATA_DIR, STATE_URL

METADATA = MetaData()

WATERMARKS = Table(
    "watermarks", METADATA,
    Column("scope", String, primary_key=True),
    Column("value", Text),
    Column("updated_at", DateTime),
)
RUNS = Table(
    "runs", METADATA,
    Column("run_id", String, primary_key=True),
    Column("started_at", DateTime),
    Column("finished_at", DateTime),
    Column("status", String),
    Column("mode", String),
)
TASK_RUNS = Table(
    "task_runs", METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String, index=True),
    Column("task", String),
    Column("status", String),
    Column("attempts", Integer),
    Column("started_at", DateTime),
    Column("finished_at", DateTime),
    Column("detail", Text),
    Column("error", Text),
)
EXTRACTS = Table(
    "extractions", METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String, index=True),
    Column("table", String),
    Column("file", Text),
    Column("rows", Integer),
    Column("wm_before", Text),
    Column("wm_after", Text),
)
TRANSFORMS = Table(
    "transforms", METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String, index=True),
    Column("table", String),
    Column("rows_in", Integer),
    Column("rows_good", Integer),
    Column("rows_bad", Integer),
    Column("partitions", Integer),
)
PROCESSED = Table(
    "processed_files", METADATA,
    Column("file", String, primary_key=True),
    Column("table", String),
    Column("run_id", String),
    Column("rows_good", Integer),
    Column("rows_bad", Integer),
    Column("processed_at", DateTime),
)


def utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class StateStore:
    def __init__(self, url: str = STATE_URL):
        DATA_DIR.mkdir(exist_ok=True)
        self.engine = create_engine(url)
        METADATA.create_all(self.engine)

    # ------------------------------------------------------------- watermarks
    def get(self, scope: str, default: str | None = None) -> str | None:
        with self.engine.connect() as c:
            row = c.execute(
                WATERMARKS.select().where(WATERMARKS.c.scope == scope)
            ).first()
        return row.value if row else default

    def set(self, scope: str, value: str) -> None:
        with self.engine.begin() as c:
            c.execute(delete(WATERMARKS).where(WATERMARKS.c.scope == scope))
            c.execute(insert(WATERMARKS).values(
                scope=scope, value=value, updated_at=utc()))

    def clear_prefix(self, prefix: str) -> None:
        with self.engine.begin() as c:
            c.execute(delete(WATERMARKS).where(WATERMARKS.c.scope.like(f"{prefix}%")))

    # ------------------------------------------------------------------- runs
    def start_run(self, mode: str) -> str:
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
        with self.engine.begin() as c:
            c.execute(insert(RUNS).values(
                run_id=run_id, started_at=utc(), status="running", mode=mode))
        return run_id

    def finish_run(self, run_id: str, status: str) -> None:
        with self.engine.begin() as c:
            c.execute(RUNS.update().where(RUNS.c.run_id == run_id)
                      .values(finished_at=utc(), status=status))

    def record_task(self, run_id: str, task: str, status: str, attempts: int,
                    started: datetime, detail: str = "", error: str = "") -> None:
        with self.engine.begin() as c:
            c.execute(insert(TASK_RUNS).values(
                run_id=run_id, task=task, status=status, attempts=attempts,
                started_at=started, finished_at=utc(),
                detail=detail, error=error[:2000]))

    def record_extraction(self, run_id: str, table: str, file: str | None,
                          rows: int, wm_before: str | None, wm_after: str | None) -> None:
        with self.engine.begin() as c:
            c.execute(insert(EXTRACTS).values(
                run_id=run_id, table=table, file=file, rows=rows,
                wm_before=wm_before, wm_after=wm_after))

    def record_transform(self, run_id: str, table: str, rows_in: int,
                         rows_good: int, rows_bad: int, partitions: int) -> None:
        with self.engine.begin() as c:
            c.execute(insert(TRANSFORMS).values(
                run_id=run_id, table=table, rows_in=rows_in, rows_good=rows_good,
                rows_bad=rows_bad, partitions=partitions))

    def extraction_files(self, run_id: str, table: str) -> str | None:
        """File path of this run's bronze batch for a table (latest if several)."""
        with self.engine.connect() as c:
            rows = c.execute(
                select(EXTRACTS.c.file).where(EXTRACTS.c.run_id == run_id,
                                              EXTRACTS.c.table == table)
            ).scalars().all()
        return rows[-1] if rows else None

    def last_runs(self, n: int = 5) -> list:
        with self.engine.connect() as c:
            return c.execute(select(RUNS).order_by(RUNS.c.started_at.desc())
                              .limit(n)).all()

    def tasks_of_run(self, run_id: str) -> list:
        with self.engine.connect() as c:
            return c.execute(select(TASK_RUNS).where(TASK_RUNS.c.run_id == run_id)
                             .order_by(TASK_RUNS.c.id)).all()

    # ------------------------------------------------- bronze file ledger
    def unprocessed_files(self, table: str, files: list[str]) -> list[str]:
        """Bronze files never successfully transformed (crash-safe replay)."""
        with self.engine.connect() as c:
            done = set(c.execute(select(PROCESSED.c.file)
                                 .where(PROCESSED.c.table == table)).scalars())
        return [f for f in files if f not in done]

    def mark_processed(self, file: str, table: str, run_id: str,
                      rows_good: int, rows_bad: int) -> None:
        with self.engine.begin() as c:
            c.execute(insert(PROCESSED).values(
                file=file, table=table, run_id=run_id,
                rows_good=rows_good, rows_bad=rows_bad, processed_at=utc()))
