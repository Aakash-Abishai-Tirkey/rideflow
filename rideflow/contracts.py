"""Data contracts — single source of truth for row shapes.

The simulator produces rows matching these models, and the silver layer
re-validates them with the SAME models. Schema drift fails loudly here
instead of silently corrupting the warehouse.
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class RideStatus(str, Enum):
    requested = "requested"
    matched = "matched"
    in_progress = "in_progress"
    completed = "completed"
    cancelled = "cancelled"


class PaymentStatus(str, Enum):
    completed = "completed"
    failed = "failed"


class _Row(BaseModel):
    # unknown field = contract violation; enums stored as their string values
    model_config = ConfigDict(extra="forbid", use_enum_values=True)


class RideRow(_Row):
    ride_id: str
    user_id: str
    # None = request never matched a driver (unmet demand — cancelled)
    driver_id: str | None = None
    status: RideStatus
    request_ts: datetime
    pickup_zone: int = Field(ge=1, le=16)
    dropoff_zone: int = Field(ge=1, le=16)
    pickup_lat: float
    pickup_lon: float
    dropoff_lat: float
    dropoff_lon: float
    distance_km: float = Field(ge=0)
    duration_min: int = Field(ge=0)
    fare: float = Field(ge=0)
    surge_multiplier: float = Field(ge=1)
    completed_ts: datetime | None = None


class RideEventRow(_Row):
    event_id: str
    ride_id: str
    event_type: str          # requested / matched / started / completed / cancelled
    event_ts: datetime


class GpsPingRow(_Row):
    ping_id: str
    ride_id: str
    ts: datetime
    lat: float
    lon: float
    zone_id: int = Field(ge=1, le=16)
    speed_kmh: float = Field(ge=0)


class PaymentRow(_Row):
    payment_id: str
    ride_id: str
    amount: float = Field(ge=0)
    method: str              # card / cash / wallet
    status: PaymentStatus
    paid_at: datetime
