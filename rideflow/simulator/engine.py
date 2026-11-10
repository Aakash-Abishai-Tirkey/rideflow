"""RideFlow simulator engine — the "app" that generates business data.

Simulates the full ride lifecycle as a state machine driven by simulated time:

    requested -> matched -> in_progress -> completed | cancelled

Modes:
  backfill   replay N days of history in seconds (instant demo dataset)
  live       advance simulated time in real time (dashboard-friendly)

Design notes (interview material):
  * Every ride's full future is computed the instant it spawns — but its
    rows only land in the OLTP once simulated time passes their completion,
    mimicking a real event stream arriving in (roughly) event order.
  * All timestamps are naive UTC (SQLite project convention).
  * Seeded RNG => reproducible datasets for tests and demos.
"""
from __future__ import annotations

import argparse
import random
import time as time_mod
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, insert, select
from sqlalchemy.orm import Session

from rideflow.config import SIM
from rideflow.contracts import GpsPingRow, PaymentRow, RideEventRow, RideRow
from rideflow.oltp import (
    Base, Driver, GpsPing, Payment, Ride, RideEvent, User,
    get_engine, init_db,
)
from rideflow.simulator import city

VEHICLES = ("Sedan", "SUV", "Hatchback", "Minivan", "EV Compact")


def utcnow() -> datetime:
    """Naive UTC 'now'."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def uid(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


@dataclass
class SimDriver:
    driver_id: str
    home_zone: int
    zone: int          # current position; relocates as rides complete
    free_at: datetime  # busy until this simulated time
    rating: float
    vehicle: str


class Simulator:
    def __init__(self, engine=None, seed: int | None = None):
        self.engine = engine or get_engine()
        self.rng = random.Random(seed if seed is not None else SIM["seed"])
        self.user_ids: list[str] = []
        self.drivers: list[SimDriver] = []
        # (ready_ts, [(table, row_dict), ...]) — rows land when time passes ready_ts
        self._pending: list[tuple[datetime, list]] = []
        self._hour_rate = self._build_hour_rates()

    # ------------------------------------------------------------------ setup

    def reset(self) -> None:
        Base.metadata.drop_all(self.engine)
        init_db(self.engine)

    def populate(self) -> None:
        """Seed the platform population: users and drivers."""
        base = utcnow() - timedelta(days=365)
        users = [
            dict(
                user_id=uid("u"),
                signup_ts=base + timedelta(seconds=self.rng.randrange(3600 * 24 * 330)),
                home_zone=self.rng.choice(city.ZONES).zone_id,
                rating=round(self.rng.uniform(3.8, 5.0), 2),
            )
            for _ in range(SIM["users"])
        ]
        self.drivers = []
        driver_rows = []
        for _ in range(SIM["drivers"]):
            z = self.rng.choice(city.ZONES).zone_id
            d = SimDriver(
                driver_id=uid("d"), home_zone=z, zone=z,
                free_at=datetime(2000, 1, 1),
                rating=round(self.rng.uniform(4.0, 5.0), 2),
                vehicle=self.rng.choice(VEHICLES),
            )
            self.drivers.append(d)
            driver_rows.append(dict(
                driver_id=d.driver_id,
                signup_ts=base + timedelta(seconds=self.rng.randrange(3600 * 24 * 330)),
                home_zone=z, status="online", rating=d.rating, vehicle=d.vehicle,
            ))
        with Session(self.engine) as s:
            s.execute(insert(User), users)
            s.execute(insert(Driver), driver_rows)
            s.commit()
        self.user_ids = [u["user_id"] for u in users]

    def resume(self) -> None:
        """Continue simulating against an already-populated database.

        Loads users/drivers into memory; drivers start a fresh shift (free
        immediately, positioned at their home zones).
        """
        with Session(self.engine) as s:
            self.user_ids = s.scalars(select(User.user_id)).all()
            rows = s.execute(select(Driver.driver_id, Driver.home_zone,
                                    Driver.rating, Driver.vehicle)).all()
        self.drivers = [
            SimDriver(driver_id=d, home_zone=z, zone=z,
                      free_at=datetime(2000, 1, 1), rating=r, vehicle=v)
            for d, z, r, v in rows
        ]

    # --------------------------------------------------------- demand helpers

    def _build_hour_rates(self) -> list[float]:
        """City-wide rides/hour for each hour, normalized around the base rate."""
        overall = sum(
            city.demand_weight(z.zone_id, h) for z in city.ZONES for h in range(24)
        ) / (len(city.ZONES) * 24)
        return [
            SIM["base_rides_per_hour"]
            * sum(city.demand_weight(z.zone_id, h) for z in city.ZONES)
            / len(city.ZONES) / overall
            for h in range(24)
        ]

    def _pickup_zone(self, hour: int) -> int:
        ids = [z.zone_id for z in city.ZONES]
        weights = [city.demand_weight(z.zone_id, hour) for z in city.ZONES]
        return self.rng.choices(ids, weights=weights)[0]

    def _dropoff_zone(self, pickup: int, hour: int) -> int:
        """Commuting flow: to business in the morning, home at night."""
        if 6 <= hour < 10:
            target = ("business", "transit")
        elif 17 <= hour < 23:
            target = ("residential", "nightlife")
        else:
            target = ()
        ids, weights = [], []
        for z in city.ZONES:
            w = 2.5 if (target and z.kind in target) else 0.4 if target else 1.0
            if z.zone_id == pickup:
                w *= 0.15
            ids.append(z.zone_id)
            weights.append(w)
        return self.rng.choices(ids, weights=weights)[0]

    def _surge(self, zone: int, hour: int) -> float:
        """Surge follows zone demand: quiet 1.0, rush/nightlife up to ~2.4."""
        w = city.demand_weight(zone, hour)
        return round(min(1.0 + 0.35 * max(0.0, w - 1.6), 2.4), 1)

    def _jitter(self, amount: float = 0.004) -> float:
        return self.rng.uniform(-amount, amount)

    # ------------------------------------------------------------- lifecycle

    def _spawn_ride(self, now: datetime) -> None:
        request_ts = now + timedelta(seconds=self.rng.randrange(60))
        hour = request_ts.hour
        pickup = self._pickup_zone(hour)
        dropoff = self._dropoff_zone(pickup, hour)
        user_id = self.rng.choice(self.user_ids)
        surge = self._surge(pickup, hour)

        plat, plon = city.zone_center(pickup)
        plat += self._jitter(); plon += self._jitter()
        dlat, dlon = city.zone_center(dropoff)
        dlat += self._jitter(); dlon += self._jitter()

        # ---- try to match a driver (nearest available, slight randomness)
        avail = [d for d in self.drivers if d.free_at <= request_ts]
        avail.sort(key=lambda d: city.distance_km(d.zone, pickup) + self.rng.random() * 1.5)
        driver = avail[0] if avail else None
        rows: list[tuple] = []

        if driver is None:
            # Unmet demand: no free driver -> cancelled before matching.
            cancel_ts = request_ts + timedelta(minutes=self.rng.uniform(2, 6))
            ride = RideRow(
                ride_id=uid("r"), user_id=user_id, driver_id=None,
                status="cancelled", request_ts=request_ts,
                pickup_zone=pickup, dropoff_zone=dropoff,
                pickup_lat=plat, pickup_lon=plon, dropoff_lat=dlat, dropoff_lon=dlon,
                distance_km=0.0, duration_min=0, fare=0.0,
                surge_multiplier=surge, completed_ts=None,
            )
            rows.append((Ride, ride.model_dump()))
            for etype, ts in (("requested", request_ts), ("cancelled", cancel_ts)):
                ev = RideEventRow(event_id=uid("ev"), ride_id=ride.ride_id,
                                  event_type=etype, event_ts=ts)
                rows.append((RideEvent, ev.model_dump()))
            self._pending.append((cancel_ts, rows))
            return

        match_ts = request_ts + timedelta(seconds=self.rng.randrange(30, 180))
        deadhead_km = city.distance_km(driver.zone, pickup)
        deadhead_min = 2 + deadhead_km / SIM["avg_speed_kmh"] * 60 * self.rng.uniform(0.8, 1.2)
        start_ts = match_ts + timedelta(minutes=deadhead_min)

        distance = city.distance_km(pickup, dropoff) * self.rng.uniform(0.9, 1.3)
        traffic = 1.35 if (7 <= hour <= 9 or 16 <= hour <= 19) else 1.0
        duration = max(3, round(
            distance / SIM["avg_speed_kmh"] * 60 * traffic * self.rng.uniform(0.85, 1.15)
        ))
        fare = round(
            (SIM["fare_base"] + SIM["fare_per_km"] * distance
             + SIM["fare_per_min"] * duration) * surge, 2
        )

        cancelled_mid = self.rng.random() < SIM["cancel_prob"]
        if cancelled_mid:
            frac = self.rng.uniform(0.3, 0.7)
            end_ts = start_ts + timedelta(minutes=duration * frac)
            status, completed_ts = "cancelled", None
        else:
            end_ts = start_ts + timedelta(minutes=duration)
            status, completed_ts = "completed", end_ts

        ride = RideRow(
            ride_id=uid("r"), user_id=user_id, driver_id=driver.driver_id,
            status=status, request_ts=request_ts,
            pickup_zone=pickup, dropoff_zone=dropoff,
            pickup_lat=plat, pickup_lon=plon, dropoff_lat=dlat, dropoff_lon=dlon,
            distance_km=round(distance, 2), duration_min=duration, fare=fare,
            surge_multiplier=surge, completed_ts=completed_ts,
        )
        rows.append((Ride, ride.model_dump()))

        events = [("requested", request_ts), ("matched", match_ts), ("started", start_ts),
                  (status, end_ts)]
        for etype, ts in events:
            ev = RideEventRow(event_id=uid("ev"), ride_id=ride.ride_id,
                              event_type=etype, event_ts=ts)
            rows.append((RideEvent, ev.model_dump()))

        # ---- GPS pings along the route
        n_pings = max(2, int((end_ts - start_ts).total_seconds() // 60))
        for i in range(1, n_pings + 1):
            f = i / (n_pings + 1)
            ts = start_ts + (end_ts - start_ts) * f
            lat = plat + (dlat - plat) * f + self._jitter(0.003)
            lon = plon + (dlon - plon) * f + self._jitter(0.003)
            ping = GpsPingRow(
                ping_id=uid("gp"), ride_id=ride.ride_id, ts=ts, lat=lat, lon=lon,
                zone_id=city.zone_of(lat, lon),
                speed_kmh=round(distance / max(duration, 1) * 60 * self.rng.uniform(0.8, 1.2), 1),
            )
            rows.append((GpsPing, ping.model_dump()))

        # ---- payment on completion (3% stay failed -> reconciliation fodder)
        if status == "completed":
            amount = fare
            if self.rng.random() < SIM["tip_prob"]:
                amount = round(fare * (1 + self.rng.uniform(0.10, 0.25)), 2)
            ok = self.rng.random() >= SIM["payment_fail_prob"]
            pay = PaymentRow(
                payment_id=uid("p"), ride_id=ride.ride_id, amount=amount,
                method=self.rng.choices(("card", "wallet", "cash"), (0.7, 0.2, 0.1))[0],
                status="completed" if ok else "failed", paid_at=end_ts,
            )
            rows.append((Payment, pay.model_dump()))

        # ---- driver becomes free where the ride ends
        driver.free_at = end_ts
        driver.zone = dropoff

        self._pending.append((end_ts, rows))

    # ------------------------------------------------------------------ sink

    def _drain(self, now: datetime) -> None:
        """Land pending rows whose simulated time has arrived."""
        ready = [p for p in self._pending if p[0] <= now]
        if not ready:
            return
        self._pending = [p for p in self._pending if p[0] > now]
        with Session(self.engine) as s:
            for _, rows in sorted(ready, key=lambda p: p[0]):
                by_table: dict = {}
                for table, row in rows:
                    by_table.setdefault(table, []).append(row)
                for table, trs in by_table.items():
                    s.execute(insert(table), trs)
            s.commit()

    # ------------------------------------------------------------------ modes

    def _process_minute(self, now: datetime) -> None:
        rate = self._hour_rate[now.hour] / 60.0
        n = int(rate) + (1 if self.rng.random() < rate - int(rate) else 0)
        for _ in range(n):
            self._spawn_ride(now)
        self._drain(now)

    def backfill(self, days: int = 7) -> dict:
        end = utcnow().replace(second=0, microsecond=0)
        now = end - timedelta(days=days)
        while now < end:
            self._process_minute(now)
            now += timedelta(minutes=1)
        self._drain(end + timedelta(days=1))  # flush in-flight rides
        return self.stats()

    def live(self, sec_per_min: float = 1.0, ticks: int | None = None) -> None:
        """Advance simulated time in real time. Each tick = 1 simulated minute."""
        now = utcnow().replace(second=0, microsecond=0)
        i = 0
        try:
            while ticks is None or i < ticks:
                self._process_minute(now)
                now += timedelta(minutes=1)
                i += 1
                time_mod.sleep(sec_per_min)
        except KeyboardInterrupt:
            pass
        self._drain(now + timedelta(hours=2))  # flush rides still in flight

    # ----------------------------------------------------------------- stats

    def stats(self) -> dict:
        with Session(self.engine) as s:
            n = lambda q: s.scalar(q)
            rides = n(select(func.count()).select_from(Ride))
            completed = n(select(func.count()).select_from(Ride).where(Ride.status == "completed"))
            cancelled = n(select(func.count()).select_from(Ride).where(Ride.status == "cancelled"))
            unmet = n(select(func.count()).select_from(Ride).where(Ride.driver_id.is_(None)))
            revenue = n(select(func.coalesce(func.sum(Payment.amount), 0))
                        .where(Payment.status == "completed"))
            failed_pays = n(select(func.count()).select_from(Payment)
                            .where(Payment.status == "failed"))
            return {
                "rides": rides,
                "completed": completed,
                "cancelled": cancelled,
                "unmet_demand": unmet,
                "revenue_usd": round(revenue or 0, 2),
                "avg_fare": round(n(select(func.avg(Ride.fare)).where(Ride.status == "completed")) or 0, 2),
                "avg_surge": round(n(select(func.avg(Ride.surge_multiplier))) or 0, 2),
                "payments": n(select(func.count()).select_from(Payment)),
                "failed_payments": failed_pays,
                "events": n(select(func.count()).select_from(RideEvent)),
                "gps_pings": n(select(func.count()).select_from(GpsPing)),
                "drivers": n(select(func.count()).select_from(Driver)),
                "users": n(select(func.count()).select_from(User)),
            }

    def example_ride(self) -> str:
        """Print one completed ride's lifecycle — great for understanding the data."""
        with Session(self.engine) as s:
            ride_id = s.scalar(
                select(Ride.ride_id).where(Ride.status == "completed")
                .order_by(Ride.request_ts).limit(1)
            )
            if not ride_id:
                return "(no completed rides yet)"
            r = s.get(Ride, ride_id)
            evs = s.execute(
                select(RideEvent.event_type, RideEvent.event_ts)
                .where(RideEvent.ride_id == ride_id).order_by(RideEvent.event_ts)
            ).all()
            p = s.execute(
                select(Payment.amount, Payment.method, Payment.status)
                .where(Payment.ride_id == ride_id)
            ).first()
        lines = [
            f"ride {ride_id} | {city.ZONE_BY_ID[r.pickup_zone].name} -> "
            f"{city.ZONE_BY_ID[r.dropoff_zone].name}",
            f"  user {r.user_id} | driver {r.driver_id}",
        ]
        for etype, ts in evs:
            lines.append(f"  {ts:%Y-%m-%d %H:%M:%S}  {etype}")
        lines.append(
            f"  {r.distance_km} km, {r.duration_min} min, fare ${r.fare} "
            f"(surge x{r.surge_multiplier})"
        )
        if p:
            lines.append(f"  payment: ${p[0]} via {p[1]} [{p[2]}]")
        return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="RideFlow simulator")
    ap.add_argument("--days", type=int, default=7, help="backfill days of history")
    ap.add_argument("--reset", action="store_true", help="wipe databases first")
    ap.add_argument("--live", action="store_true", help="run in real time")
    ap.add_argument("--speed", type=float, default=1.0, help="live: real seconds per simulated minute")
    args = ap.parse_args()

    sim = Simulator()
    if args.reset:
        sim.reset()
    init_db(sim.engine)
    with Session(sim.engine) as s:
        if s.scalar(select(func.count()).select_from(User)) == 0:
            sim.populate()

    if args.live:
        print(f"live mode (1 sim-minute per {args.speed}s) — Ctrl+C to stop")
        sim.live(sec_per_min=args.speed)
        return

    stats = sim.backfill(days=args.days)
    print("=== RideFlow backfill complete ===")
    for k, v in stats.items():
        print(f"  {k:>16}: {v:,}" if isinstance(v, int) else f"  {k:>16}: {v}")
    print("\n=== example ride lifecycle ===")
    print(sim.example_ride())


if __name__ == "__main__":
    main()
