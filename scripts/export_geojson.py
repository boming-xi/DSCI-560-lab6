#!/usr/bin/env python3
"""Export wells + stimulations + web data to GeoJSON for mapping."""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
from typing import Any, Dict, Optional, Tuple


def get_columns(conn: sqlite3.Connection, table: str, exclude: Tuple[str, ...]) -> list[str]:
    cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    return [col for col in cols if col not in exclude]


def normalize_missing(value: Any) -> Any:
    if value is None:
        return "N/A"
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned == "":
            return "N/A"
        return cleaned
    return value


def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if text.lower() in {"n/a", "na", "none", ""}:
        return None
    cleaned = re.sub(r"[^0-9.\-]", "", text)
    if cleaned == "":
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def dms_to_decimal(dms: Optional[str]) -> Optional[float]:
    if not dms:
        return None
    text = str(dms)
    numbers = re.findall(r"\d+(?:\.\d+)?", text)
    if not numbers:
        return None
    deg = float(numbers[0])
    minutes = float(numbers[1]) if len(numbers) > 1 else 0.0
    seconds = float(numbers[2]) if len(numbers) > 2 else 0.0
    decimal = deg + minutes / 60.0 + seconds / 3600.0
    if re.search(r"\bS\b", text, re.IGNORECASE) or re.search(r"\bW\b", text, re.IGNORECASE):
        decimal *= -1
    return decimal


def pick_lat_lon(row: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    def is_valid_coord(lat_val: Optional[float], lon_val: Optional[float]) -> bool:
        if lat_val is None or lon_val is None:
            return False
        if not (-90.0 <= lat_val <= 90.0 and -180.0 <= lon_val <= 180.0):
            return False
        if lat_val == 0.0 and lon_val == 0.0:
            return False
        return True

    lat = to_float(row.get("latitude_decimal"))
    lon = to_float(row.get("longitude_decimal"))
    if is_valid_coord(lat, lon):
        return lat, lon

    lat = dms_to_decimal(row.get("latitude"))
    lon = dms_to_decimal(row.get("longitude"))
    if is_valid_coord(lat, lon):
        return lat, lon

    return None, None


def build_features(conn: sqlite3.Connection) -> list[dict]:
    well_cols = get_columns(conn, "wells", exclude=("raw_text",))
    stim_cols = get_columns(conn, "stimulations", exclude=("raw_text",))

    select_parts = [f"w.{col} AS w_{col}" for col in well_cols]
    select_parts += [f"s.{col} AS s_{col}" for col in stim_cols]
    query = (
        f"SELECT {', '.join(select_parts)} "
        "FROM wells w LEFT JOIN stimulations s ON s.well_id = w.id "
        "ORDER BY w.id"
    )

    conn.row_factory = sqlite3.Row
    rows = conn.execute(query).fetchall()

    by_well: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        row_dict = dict(row)
        well_id = row_dict.get("w_id")
        if well_id is None:
            continue
        if well_id not in by_well:
            entry: Dict[str, Any] = {}
            for col in well_cols:
                entry[col] = row_dict.get(f"w_{col}")
            for col in stim_cols:
                entry[col] = row_dict.get(f"s_{col}")
            by_well[well_id] = entry
        else:
            # If this well already exists but stimulation fields are empty, fill them.
            existing = by_well[well_id]
            for col in stim_cols:
                if existing.get(col) in (None, "", "N/A") and row_dict.get(f"s_{col}") not in (None, ""):
                    existing[col] = row_dict.get(f"s_{col}")

    features: list[dict] = []
    for _, data in by_well.items():
        lat, lon = pick_lat_lon(data)
        if lat is None or lon is None:
            continue
        props = {key: normalize_missing(value) for key, value in data.items()}
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": props,
            }
        )

    return features


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export wells to GeoJSON for web mapping.")
    parser.add_argument("--db-path", default="oil_wells.db", help="SQLite database path.")
    parser.add_argument(
        "--output",
        default=os.path.join("web", "data", "wells.geojson"),
        help="Output GeoJSON file path.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    conn = sqlite3.connect(args.db_path)
    features = build_features(conn)
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as handle:
        json.dump({"type": "FeatureCollection", "features": features}, handle, indent=2)
    print(f"Wrote {len(features)} features to {args.output}")


if __name__ == "__main__":
    main()
