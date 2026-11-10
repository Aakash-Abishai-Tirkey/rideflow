"""Silver layer: contract validation + dedup + partitioned Parquet.

- Every bronze row is re-validated against the SAME pydantic contract the
  simulator was built on: schema drift fails loudly, bad rows are
  quarantined (not silently dropped), and the rest flows on.
- Partitions are compacted on write: one deduped part per (table, date),
  so re-running a day is always idempotent.
- Event tables partition by their own event date; reference tables keep a
  daily full snapshot (dt = run date) — free SCD history later.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from pydantic import ValidationError

from rideflow import config
from rideflow.pipeline.extract import TABLES
from rideflow.pipeline.watermarks import StateStore


def transform_table(store: StateStore, run_id: str, table: str) -> dict:
    """Process every bronze file not yet in the ledger.

    Crash-safe: a batch extracted by a run that failed mid-pipeline stays
    unmarked and is replayed by the next run. Bronze is immutable and
    silver dedups by PK, so replay never duplicates rows.
    """
    cfg = TABLES[table]
    model = cfg["model"]

    root = config.BRONZE_DIR / table
    files = sorted(str(p) for p in root.glob("dt=*/batch-*.jsonl")) if root.exists() else []
    files = store.unprocessed_files(table, files)

    rows: list[dict] = []
    per_file: list[tuple[str, int]] = []
    for path in files:
        n = 0
        with open(path, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    rows.append(json.loads(line))
                    n += 1
        per_file.append((path, n))

    good: list[dict] = []
    bad: list[dict] = []
    for r in rows:
        try:
            good.append(model.model_validate(r).model_dump())
        except ValidationError as e:
            bad.append({"source_file": per_file[-1][0] if per_file else "?",
                        "row": r, "errors": [str(err) for err in e.errors()[:3]]})

    if bad:
        qpath = config.DATA_DIR / "quarantine" / table / f"batch-{run_id}.jsonl"
        qpath.parent.mkdir(parents=True, exist_ok=True)
        with open(qpath, "w", encoding="utf-8") as f:
            for b in bad:
                f.write(json.dumps(b, default=str) + "\n")

    partitions = 0
    if good:
        df = pd.DataFrame(good)
        ts = cfg["ts"]
        if ts:
            df[ts] = pd.to_datetime(df[ts])
            df["dt"] = df[ts].dt.strftime("%Y-%m-%d")
        else:  # reference table: snapshot semantics
            df["dt"] = run_id[:8]

        pk = cfg["pk"]
        for dt, grp in df.groupby("dt"):
            part_dir = config.SILVER_DIR / table / f"dt={dt}"
            part_dir.mkdir(parents=True, exist_ok=True)
            grp.to_parquet(part_dir / f"part-{run_id}.parquet", index=False)

            # Compact: merge new part with any existing parts, dedupe by PK,
            # newest version wins (parts sort oldest-first by run id).
            parts = sorted(part_dir.glob("part-*.parquet"))
            if len(parts) > 1 or parts[0].name != "part-0000.parquet":
                merged = pd.concat([pd.read_parquet(p) for p in parts])
                merged = merged.drop_duplicates(subset=[pk], keep="last")
                merged.to_parquet(part_dir / "part-0000.parquet", index=False)
                for p in parts:
                    if p.name != "part-0000.parquet":
                        p.unlink()
            partitions += 1

    # Mark each source batch processed only after silver succeeded.
    for path, _ in per_file:
        store.mark_processed(path, table, run_id, len(good), len(bad))

    store.record_transform(run_id, table, len(rows), len(good), len(bad), partitions)
    return {"files": len(files), "rows_in": len(rows), "rows_good": len(good),
            "rows_bad": len(bad), "partitions": partitions}


def silver_partitions(table: str) -> list[Path]:
    """All partition dirs of a silver table (event date order)."""
    root = config.SILVER_DIR / table
    if not root.exists():
        return []
    return sorted(root.glob("dt=*"), key=lambda p: p.name)


def read_partition(part_dir: Path) -> pd.DataFrame:
    parts = sorted(part_dir.glob("part-*.parquet"))
    if not parts:
        return pd.DataFrame()
    df = pd.concat([pd.read_parquet(p) for p in parts])
    return df
