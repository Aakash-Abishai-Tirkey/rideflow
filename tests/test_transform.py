"""Silver layer: validation, quarantine, dedup, partition compaction."""
from __future__ import annotations

import json
from datetime import datetime

from conftest import ride_dict
from rideflow import config
from rideflow.pipeline.transform import read_partition, silver_partitions, transform_table
from rideflow.pipeline.watermarks import StateStore


def write_bronze(table: str, run_id: str, rows: list[dict]) -> str:
    path = config.BRONZE_DIR / table / "dt=20260101" / f"batch-{run_id}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, default=str) + "\n")
    return str(path)


def test_good_rows_flow_to_silver(data_env):
    store = StateStore()
    write_bronze("rides", "20260101T000001-aaaa", [
        ride_dict("r1", datetime(2026, 1, 1, 8, 0)),
        ride_dict("r2", datetime(2026, 1, 1, 8, 30)),
    ])

    result = transform_table(store, "20260101T000001-aaaa", "rides")
    assert result["rows_good"] == 2
    assert result["rows_bad"] == 0
    assert len(read_partition(silver_partitions("rides")[0])) == 2

    # the processed-file ledger means the batch is never re-processed
    again = transform_table(store, "20260101T000009-zzzz", "rides")
    assert again["rows_in"] == 0


def test_contract_violations_quarantined(data_env):
    store = StateStore()
    bad = ride_dict("r2", datetime(2026, 1, 1, 8, 30))
    bad["bonus_field"] = "drift"  # schema drift
    write_bronze("rides", "20260101T000001-aaaa", [
        ride_dict("r1", datetime(2026, 1, 1, 8, 0)), bad,
    ])

    result = transform_table(store, "20260101T000001-aaaa", "rides")
    assert result["rows_good"] == 1
    assert result["rows_bad"] == 1

    qfile = config.DATA_DIR / "quarantine" / "rides" / "batch-20260101T000001-aaaa.jsonl"
    quarantined = json.loads(qfile.read_text(encoding="utf-8").splitlines()[0])
    assert quarantined["row"]["ride_id"] == "r2"
    assert quarantined["errors"]

    # good rows still flow
    assert len(read_partition(silver_partitions("rides")[0])) == 1


def test_duplicate_pks_collapse_to_newest(data_env):
    store = StateStore()
    write_bronze("rides", "20260101T000001-aaaa", [
        ride_dict("r1", datetime(2026, 1, 1, 8, 0), fare=10.0),
        ride_dict("r1", datetime(2026, 1, 1, 8, 0), fare=20.0),
    ])

    result = transform_table(store, "20260101T000001-aaaa", "rides")
    assert result["rows_good"] == 2  # both are valid rows...

    df = read_partition(silver_partitions("rides")[0])
    assert len(df) == 1             # ...but silver keeps one version
    assert df.iloc[0]["fare"] == 20.0  # newest wins


def test_partitions_compact_across_runs(data_env):
    store = StateStore()
    write_bronze("rides", "20260101T000001-aaaa",
                 [ride_dict("r1", datetime(2026, 1, 1, 8, 0), fare=10.0)])
    transform_table(store, "20260101T000001-aaaa", "rides")

    write_bronze("rides", "20260101T000002-bbbb", [
        ride_dict("r1", datetime(2026, 1, 1, 9, 0), fare=30.0),
        ride_dict("r2", datetime(2026, 1, 1, 9, 30)),
    ])
    transform_table(store, "20260101T000002-bbbb", "rides")

    part = silver_partitions("rides")[0]
    assert list(part.glob("part-*.parquet")) == [part / "part-0000.parquet"]
    df = read_partition(part)
    assert len(df) == 2
    assert df[df.ride_id == "r1"].iloc[0]["fare"] == 30.0  # later batch wins
