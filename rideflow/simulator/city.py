"""Meridian City: a 4x4 grid of zones with realistic diurnal demand patterns."""
from __future__ import annotations

import math
from dataclasses import dataclass

CENTER_LAT, CENTER_LON = 40.700, -74.000
ZONE_KM = 2.0  # grid cell edge length


@dataclass(frozen=True)
class Zone:
    zone_id: int
    name: str
    x: int          # grid column (east+)
    y: int          # grid row (north+)
    kind: str       # business / residential / nightlife / transit


ZONES: tuple[Zone, ...] = (
    Zone(1,  "Old Town",     0, 0, "nightlife"),
    Zone(2,  "Riverside",    1, 0, "residential"),
    Zone(3,  "Docklands",    2, 0, "business"),
    Zone(4,  "Eastport",     3, 0, "residential"),
    Zone(5,  "Market Row",   0, 1, "nightlife"),
    Zone(6,  "Civic Center", 1, 1, "business"),
    Zone(7,  "Union Square", 2, 1, "business"),
    Zone(8,  "Botanic Ward", 3, 1, "residential"),
    Zone(9,  "Westfield",    0, 2, "residential"),
    Zone(10, "Midtown",     1, 2, "business"),
    Zone(11, "Techpark",    2, 2, "business"),
    Zone(12, "Stadium",     3, 2, "nightlife"),
    Zone(13, "Southbank",   0, 3, "residential"),
    Zone(14, "Green Hills", 1, 3, "residential"),
    Zone(15, "Union Yard",  2, 3, "residential"),
    Zone(16, "Sky Harbor",  3, 3, "transit"),  # the airport
)
ZONE_BY_ID = {z.zone_id: z for z in ZONES}


def zone_center(zone_id: int) -> tuple[float, float]:
    """Approximate lat/lon of a zone's center."""
    z = ZONE_BY_ID[zone_id]
    return (
        CENTER_LAT + z.y * ZONE_KM * 0.009,
        CENTER_LON + z.x * ZONE_KM * 0.012,
    )


def zone_of(lat: float, lon: float) -> int:
    """Nearest zone for a lat/lon point (inverse of zone_center)."""
    dx = ZONE_KM * 0.012  # degrees per grid step (lon)
    dy = ZONE_KM * 0.009  # degrees per grid step (lat)
    x = min(max(round((lon - CENTER_LON) / dx), 0), 3)
    y = min(max(round((lat - CENTER_LAT) / dy), 0), 3)
    return y * 4 + x + 1


def distance_km(a: int, b: int) -> float:
    """Euclidean inter-zone distance plus intra-zone fudge."""
    za, zb = ZONE_BY_ID[a], ZONE_BY_ID[b]
    d = math.hypot(za.x - zb.x, za.y - zb.y) * ZONE_KM
    return d + 0.8 if d > 0 else 0.9


def _bell(hour: int, center: float, width: float) -> float:
    """Unnormalized gaussian bump over the 24h clock."""
    dh = min(abs(hour - center), 24 - abs(hour - center))
    return math.exp(-((dh / width) ** 2))


def demand_weight(zone_id: int, hour: int) -> float:
    """Relative pickup demand for a zone at a given hour (>= 0.1).

    business  -> morning + evening commute peaks
    nightlife -> late-evening peak
    transit   -> airport: steady all day, flight-time bumps
    residential -> evening returns home
    """
    z = ZONE_BY_ID[zone_id]
    if z.kind == "business":
        w = 0.6 + 2.0 * _bell(hour, 8, 1.6) + 2.2 * _bell(hour, 17.5, 1.8)
    elif z.kind == "nightlife":
        w = 0.5 + 2.5 * _bell(hour, 23, 2.6) + 0.8 * _bell(hour, 1, 1.5)
    elif z.kind == "transit":
        w = 1.2 + 0.9 * _bell(hour, 6, 1.6) + 1.1 * _bell(hour, 15, 2.0)
    else:  # residential
        w = 0.6 + 1.2 * _bell(hour, 7.5, 1.2) + 1.6 * _bell(hour, 19, 2.2)
    return max(w, 0.1)
