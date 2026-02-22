import sqlite3
import csv
import os
from typing import Dict, Optional


def initialize_database(connection:sqlite3.Connection)->None:
    connection.executescript("""
        CREATE TABLE IF NOT EXISTS wells (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            api TEXT,
            well_name TEXT,
            operator TEXT,
            county TEXT,
            state TEXT,
            shl_location TEXT,
            latitude TEXT,
            longitude TEXT,
            latitude_decimal REAL,
            longitude_decimal REAL,
            datum TEXT,
            source_file TEXT UNIQUE,
            raw_text TEXT,
            web_well_status TEXT
        );

        CREATE TABLE IF NOT EXISTS stimulations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            well_id INTEGER,
            date_stimulated TEXT,
            stimulated_formation TEXT,
            top_ft INTEGER,
            bottom_ft INTEGER,
            stimulation_stages INTEGER,
            volume INTEGER,
            volume_units TEXT,
            treatment_type TEXT,
            acid_pct REAL,
            lbs_proppant INTEGER,
            max_treatment_pressure_psi INTEGER,
            max_treatment_rate REAL,
            max_treatment_rate_units TEXT,
            details TEXT,
            raw_text TEXT,
            FOREIGN KEY (well_id) REFERENCES wells(id)
        );
    """)


def insert_or_update_well(connection:sqlite3.Connection,
                          well_data:Dict[str,Optional[str]])->int:

    cursor=connection.execute(
        "SELECT id FROM wells WHERE source_file=?",
        (well_data["source_file"],)
    )

    existing_row=cursor.fetchone()

    if existing_row:
        well_id=existing_row[0]

        connection.execute("""
            UPDATE wells
               SET api=?, well_name=?, operator=?, county=?, state=?,
                   shl_location=?, latitude=?, longitude=?,
                   latitude_decimal=?, longitude_decimal=?,
                   datum=?, raw_text=?, web_well_status=?
             WHERE id=?
        """,(
            well_data.get("api"),
            well_data.get("well_name"),
            well_data.get("operator"),
            well_data.get("county"),
            well_data.get("state"),
            well_data.get("shl_location"),
            well_data.get("latitude"),
            well_data.get("longitude"),
            well_data.get("latitude_decimal"),
            well_data.get("longitude_decimal"),
            well_data.get("datum"),
            well_data.get("raw_text"),
            well_data.get("web_well_status"),
            well_id
        ))

        return well_id

    cursor=connection.execute("""
        INSERT INTO wells (
            api, well_name, operator, county, state,
            shl_location, latitude, longitude,
            latitude_decimal, longitude_decimal,
            datum, source_file, raw_text, web_well_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,(
        well_data.get("api"),
        well_data.get("well_name"),
        well_data.get("operator"),
        well_data.get("county"),
        well_data.get("state"),
        well_data.get("shl_location"),
        well_data.get("latitude"),
        well_data.get("longitude"),
        well_data.get("latitude_decimal"),
        well_data.get("longitude_decimal"),
        well_data.get("datum"),
        well_data.get("source_file"),
        well_data.get("raw_text"),
        well_data.get("web_well_status")
    ))

    return cursor.lastrowid


def insert_stimulation(connection:sqlite3.Connection,
                       well_id:int,
                       stimulation_data:Dict[str,Optional[str]])->None:

    connection.execute(
        "DELETE FROM stimulations WHERE well_id=?",
        (well_id,)
    )

    connection.execute("""
        INSERT INTO stimulations (
            well_id, date_stimulated, stimulated_formation,
            top_ft, bottom_ft, stimulation_stages,
            volume, volume_units, treatment_type,
            acid_pct, lbs_proppant,
            max_treatment_pressure_psi,
            max_treatment_rate, max_treatment_rate_units,
            details, raw_text
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,(
        well_id,
        stimulation_data.get("date_stimulated"),
        stimulation_data.get("stimulated_formation"),
        stimulation_data.get("top_ft"),
        stimulation_data.get("bottom_ft"),
        stimulation_data.get("stimulation_stages"),
        stimulation_data.get("volume"),
        stimulation_data.get("volume_units"),
        stimulation_data.get("treatment_type"),
        stimulation_data.get("acid_pct"),
        stimulation_data.get("lbs_proppant"),
        stimulation_data.get("max_treatment_pressure_psi"),
        stimulation_data.get("max_treatment_rate"),
        stimulation_data.get("max_treatment_rate_units"),
        stimulation_data.get("details"),
        stimulation_data.get("raw_text")
    ))


def export_csv(connection:sqlite3.Connection,
               output_directory:str)->None:

    os.makedirs(output_directory,exist_ok=True)

    for table_name in ("wells","stimulations"):

        rows=connection.execute(
            f"SELECT * FROM {table_name}"
        ).fetchall()

        column_info=connection.execute(
            f"PRAGMA table_info({table_name})"
        ).fetchall()

        column_names=[col[1] for col in column_info]

        output_path=os.path.join(output_directory,f"{table_name}.csv")

        with open(output_path,"w",newline="",encoding="utf-8") as handle:
            writer=csv.writer(handle)
            writer.writerow(column_names)
            writer.writerows(rows)