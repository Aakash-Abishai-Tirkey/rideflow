"""OLTP schema — the operational database of the ride-hailing app.

Indexes on timestamp columns matter: the pipeline does watermarked
incremental extraction (WHERE request_ts > :watermark), so these
indexes are what keep that query cheap at scale.
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, create_engine, event
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from rideflow import config


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    user_id: Mapped[str] = mapped_column(primary_key=True)
    signup_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    home_zone: Mapped[int]
    rating: Mapped[float]


class Driver(Base):
    __tablename__ = "drivers"

    driver_id: Mapped[str] = mapped_column(primary_key=True)
    signup_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    home_zone: Mapped[int]
    status: Mapped[str] = mapped_column(default="online")  # online / busy / offline
    rating: Mapped[float]
    vehicle: Mapped[str]


class Ride(Base):
    __tablename__ = "rides"

    ride_id: Mapped[str] = mapped_column(primary_key=True)
    user_id: Mapped[str] = mapped_column(index=True)
    driver_id: Mapped[str | None] = mapped_column(index=True)
    status: Mapped[str]
    request_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    pickup_zone: Mapped[int]
    dropoff_zone: Mapped[int]
    pickup_lat: Mapped[float]
    pickup_lon: Mapped[float]
    dropoff_lat: Mapped[float]
    dropoff_lon: Mapped[float]
    distance_km: Mapped[float]
    duration_min: Mapped[int]
    fare: Mapped[float]
    surge_multiplier: Mapped[float]
    completed_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)


class RideEvent(Base):
    __tablename__ = "ride_events"

    event_id: Mapped[str] = mapped_column(primary_key=True)
    ride_id: Mapped[str] = mapped_column(index=True)
    event_type: Mapped[str]
    event_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class GpsPing(Base):
    __tablename__ = "gps_pings"

    ping_id: Mapped[str] = mapped_column(primary_key=True)
    ride_id: Mapped[str] = mapped_column(index=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    lat: Mapped[float]
    lon: Mapped[float]
    zone_id: Mapped[int]
    speed_kmh: Mapped[float]


class Payment(Base):
    __tablename__ = "payments"

    payment_id: Mapped[str] = mapped_column(primary_key=True)
    ride_id: Mapped[str] = mapped_column(index=True)
    amount: Mapped[float]
    method: Mapped[str]
    status: Mapped[str]
    paid_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


def _set_pragmas(dbapi_conn, _record):
    cur = dbapi_conn.cursor()
    cur.execute("PRAGMA foreign_keys=ON")
    cur.execute("PRAGMA journal_mode=WAL")
    cur.close()


def get_engine(url: str | None = None):
    config.DATA_DIR.mkdir(exist_ok=True)
    engine = create_engine(url or config.OLTP_URL, echo=False)
    event.listen(engine, "connect", _set_pragmas)
    return engine


def init_db(engine) -> None:
    Base.metadata.create_all(engine)
