"""DQ suite behavior on a healthy pipeline."""
from __future__ import annotations

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
