"""DQ suite behavior on a healthy pipeline."""
from __future__ import annotations

from datetime import datetime

from rideflow.pipeline.dq import run_checks, summarize
from rideflow.pipeline.watermarks import StateStore


def test_healthy_pipeline_passes_all_checks(small_world):
    results = run_checks(StateStore(), run_id=None)  # record nothing, just assert
    failed = [r for r in results if r["status"] == "fail"]
    assert not failed, failed
    assert summarize(results) == {"checks": 13, "warns": 0, "fails": 0}


def test_check_coverage(small_world):
    """Every layer is actually covered by the suite."""
    results = run_checks(StateStore(), run_id=None)
    layers = {r["layer"] for r in results}
    assert "source<->gold" in layers
    assert "gold" in layers
    assert "silver" in layers


def test_reconciliation_tolerates_growing_source(small_world):
    """Live-mode race: rows landing after extraction must not fail the gate.

    Reproduces the real bug: with a continuously-writing source, unbounded
    source-vs-gold counts fail every run. Reconciliation is bounded by the
    extraction watermark (minus a grace window), so in-flight rows are
    none of the pipeline's business yet.
    """
    from datetime import timedelta

    from conftest import insert_rides, ride_dict

    wm = datetime.fromisoformat(StateStore().get("bronze.rides"))
    insert_rides([  # these land AFTER extraction — like the live simulator
        ride_dict("r_inflight1", wm + timedelta(minutes=1)),
        ride_dict("r_inflight2", wm + timedelta(minutes=3)),
    ])

    results = run_checks(StateStore(), run_id=None)
    failed = [r for r in results if r["status"] == "fail"]
    assert not failed, failed
