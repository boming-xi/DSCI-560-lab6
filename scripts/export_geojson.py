from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
from typing import Any,Dict,Optional,Tuple


def get_columns(connection:sqlite3.Connection,
                table_name:str,
                exclude:Tuple[str,...])->list[str]:

    columns=[row[1] for row in
             connection.execute(f"PRAGMA table_info({table_name})").fetchall()]

    return [col for col in columns if col not in exclude]


def normalize_missing(value:Any)->Any:
    if value is None:
        return "N/A"
    if isinstance(value,str):
        cleaned=value.strip()
        if cleaned=="":
            return "N/A"
        return cleaned
    return value


def to_float(value:Any)->Optional[float]:
    if value is None:
        return None
    if isinstance(value,(int,float)):
        return float(value)

    text=str(value).strip()
    if text.lower() in {"n/a","na","none",""}:
        return None

    cleaned=re.sub(r"[^0-9.\-]","",text)
    if cleaned=="":
        return None

    try:
        return float(cleaned)
    except ValueError:
        return None


def dms_to_decimal(dms:Optional[str])->Optional[float]:
    if not dms:
        return None

    text=str(dms)
    numbers=re.findall(r"\d+(?:\.\d+)?",text)

    if not numbers:
        return None

    degrees=float(numbers[0])
    minutes=float(numbers[1]) if len(numbers)>1 else 0.0
    seconds=float(numbers[2]) if len(numbers)>2 else 0.0

    decimal=degrees+minutes/60.0+seconds/3600.0

    if re.search(r"\bS\b",text,re.IGNORECASE) or \
       re.search(r"\bW\b",text,re.IGNORECASE):
        decimal*=-1

    return decimal


def pick_lat_lon(row_data:Dict[str,Any])->Tuple[Optional[float],Optional[float]]:

    def is_valid(lat_val:Optional[float],
                 lon_val:Optional[float])->bool:
        if lat_val is None or lon_val is None:
            return False
        if not (-90.0<=lat_val<=90.0 and -180.0<=lon_val<=180.0):
            return False
        if lat_val==0.0 and lon_val==0.0:
            return False
        return True

    latitude=to_float(row_data.get("latitude_decimal"))
    longitude=to_float(row_data.get("longitude_decimal"))

    if is_valid(latitude,longitude):
        return latitude,longitude

    latitude=dms_to_decimal(row_data.get("latitude"))
    longitude=dms_to_decimal(row_data.get("longitude"))

    if is_valid(latitude,longitude):
        return latitude,longitude

    return None,None


def build_features(connection:sqlite3.Connection)->list[dict]:

    well_columns=get_columns(connection,"wells",exclude=("raw_text",))
    stimulation_columns=get_columns(connection,"stimulations",exclude=("raw_text",))

    select_parts=[f"w.{col} AS w_{col}" for col in well_columns]
    select_parts+=[f"s.{col} AS s_{col}" for col in stimulation_columns]

    query=(
        f"SELECT {', '.join(select_parts)} "
        "FROM wells w LEFT JOIN stimulations s ON s.well_id=w.id "
        "ORDER BY w.id"
    )

    connection.row_factory=sqlite3.Row
    rows=connection.execute(query).fetchall()

    wells_grouped:Dict[int,Dict[str,Any]]={}

    for row in rows:
        row_dict=dict(row)
        well_id=row_dict.get("w_id")
        if well_id is None:
            continue

        if well_id not in wells_grouped:
            entry={}
            for col in well_columns:
                entry[col]=row_dict.get(f"w_{col}")
            for col in stimulation_columns:
                entry[col]=row_dict.get(f"s_{col}")
            wells_grouped[well_id]=entry
        else:
            existing=wells_grouped[well_id]
            for col in stimulation_columns:
                if existing.get(col) in (None,"","N/A") and \
                   row_dict.get(f"s_{col}") not in (None,""):
                    existing[col]=row_dict.get(f"s_{col}")

    features=[]

    for _,data in wells_grouped.items():
        latitude,longitude=pick_lat_lon(data)
        if latitude is None or longitude is None:
            continue

        well_info={}
        stimulation_info={}
        web_info={}

        for key,value in data.items():
            clean_val=normalize_missing(value)

            if key.startswith("web_"):
                web_info[key]=clean_val
            elif key in stimulation_columns:
                stimulation_info[key]=clean_val
            else:
                well_info[key]=clean_val

        properties={
            "well":well_info,
            "stimulation":stimulation_info,
            "web":web_info,
        }

        features.append(
            {
                "type":"Feature",
                "geometry":{
                    "type":"Point",
                    "coordinates":[longitude,latitude]
                },
                "properties":properties,
            }
        )

    return features


def parse_arguments()->argparse.Namespace:
    parser=argparse.ArgumentParser(
        description="Export wells to structured GeoJSON for web mapping."
    )
    parser.add_argument("--db-path",default="oil_wells.db")
    parser.add_argument(
        "--output",
        default=os.path.join("web","data","wells.geojson")
    )
    return parser.parse_args()


def main()->None:
    arguments=parse_arguments()

    connection=sqlite3.connect(arguments.db_path)
    features=build_features(connection)

    os.makedirs(os.path.dirname(arguments.output),exist_ok=True)

    with open(arguments.output,"w",encoding="utf-8") as handle:
        json.dump(
            {"type":"FeatureCollection","features":features},
            handle,
            indent=2
        )

    print(f"Wrote {len(features)} features to {arguments.output}")


if __name__=="__main__":
    main()