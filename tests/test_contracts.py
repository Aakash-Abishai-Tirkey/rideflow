"""Data contracts: the single source of truth for row shapes."""
from __future__ import annotations

from datetime import datetime

import pytest
from pydantic import ValidationError

from rideflow.contracts import RideRow


def ride(**over) -> dict:
    row = dict(
        ride_id="r1", user_id="u1", driver_id="d1", status="completed",
        request_ts=datetime(2026, 1, 1, 8, 0),
        pickup_zone=3, dropoff_zone=11,
        pickup_lat=40.70, pickup_lon=-74.00, dropoff_lat=40.72, dropoff_lon=-74.02,
        distance_km=5.2, duration_min=14, fare=13.75, surge_multiplier=1.2,
        completed_ts=datetime(2026, 1, 1, 8, 14),
    )
    row.update(over)
    return row


def test_valid_row_passes():
    assert RideRow.model_validate(ride()).ride_id == "r1"


def test_unknown_field_rejected():
    """Schema drift fails loudly instead of silently corrupting downstream."""
    with pytest.raises(ValidationError):
        RideRow.model_validate(ride(bonus_field="drift"))


def test_bad_status_rejected():
    with pytest.raises(ValidationError):
        RideRow.model_validate(ride(status="teleported"))


def test_null_completed_ts_allowed():
    """Regression: NaT must serialize to null and validate, not crash silver."""
    assert RideRow.model_validate(ride(completed_ts=None)).completed_ts is None


def test_null_driver_id_allowed():
    """Unmet-demand rides: requested but never matched a driver."""
    row = RideRow.model_validate(ride(driver_id=None, status="cancelled"))
    assert row.driver_id is None


def test_zone_bounds_enforced():
    with pytest.raises(ValidationError):
        RideRow.model_validate(ride(pickup_zone=17))


def test_negative_fare_rejected():
    with pytest.raises(ValidationError):
        RideRow.model_validate(ride(fare=-5.0))
