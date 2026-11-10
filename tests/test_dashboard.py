"""Dashboard renders both views headlessly (streamlit AppTest)."""
from __future__ import annotations

from pathlib import Path

import rideflow
from rideflow.dashboards import simulator as sim_view
from rideflow.dashboards import warehouse as wh_view

DASHBOARD = Path(rideflow.__file__).parent / "dashboard.py"


def _clear_caches() -> None:
    wh_view.load_gold.clear()
    wh_view.load_state.clear()
    sim_view.load.clear()


def test_dashboard_renders_both_views(small_world):
    from streamlit.testing.v1 import AppTest

    _clear_caches()
    at = AppTest.from_file(str(DASHBOARD), default_timeout=180)
    at.run()
    assert not at.exception, at.exception

    # switch to the raw-source view
    _clear_caches()
    at.sidebar.radio[0].set_value("Simulator (OLTP)")
    at.run()
    assert not at.exception, at.exception
