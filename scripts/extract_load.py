from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import pdfplumber

from database_ops import (
    initialize_database,
    insert_or_update_well,
    insert_stimulation,
    export_csv
)

MIN_TEXT_CHARS = 30


# -------------------------------
# PDF Extraction
# -------------------------------

def extract_text_from_pdf(file_path: str) -> str:
    collected_pages = []

    with pdfplumber.open(file_path) as pdf_document:
        for page in pdf_document.pages:
            text = page.extract_text() or ""
            collected_pages.append(text)

    return "\n".join(collected_pages).strip()


# -------------------------------
# PARSING LOGIC (保留在这里)
# -------------------------------

import re
from typing import Dict, Optional


def normalize_space(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = re.sub(r"\s+", " ", value).strip()
    return value or None


def extract_first(patterns, text: str, flags=re.IGNORECASE) -> Optional[str]:
    for pat in patterns:
        match = re.search(pat, text, flags)
        if match:
            return normalize_space(match.group(1))
    return None


def parse_well_info(text: str) -> Dict[str, Optional[str]]:
    info = {}
    info["api"] = extract_first([r"API\s*#?\s*([0-9\-]+)"], text)
    info["well_name"] = extract_first([r"Well Name\s*[:#]?\s*(.+)"], text)
    info["operator"] = extract_first([r"Operator\s*[:#]?\s*(.+)"], text)
    info["county"] = extract_first([r"County\s*[:#]?\s*(.+)"], text)
    info["state"] = extract_first([r"State\s*[:#]?\s*(.+)"], text)
    info["latitude"] = extract_first([r"Latitude\s*[:#]?\s*(.+)"], text)
    info["longitude"] = extract_first([r"Longitude\s*[:#]?\s*(.+)"], text)
    info["latitude_decimal"] = None
    info["longitude_decimal"] = None
    info["datum"] = None
    return info


def parse_stimulation(text: str):
    stim = {
        "date_stimulated": extract_first([r"Date\s*Stimulated\s*([0-9/\-]+)"], text),
        "stimulated_formation": extract_first([r"Stimulated\s*Formation\s*(.+)"], text),
        "top_ft": None,
        "bottom_ft": None,
        "stimulation_stages": None,
        "volume": None,
        "volume_units": None,
        "treatment_type": None,
        "acid_pct": None,
        "lbs_proppant": None,
        "max_treatment_pressure_psi": None,
        "max_treatment_rate": None,
        "max_treatment_rate_units": None,
        "details": None,
    }

    if not any(stim.values()):
        return None

    return stim


# -------------------------------
# MAIN LOGIC
# -------------------------------

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--db-path", default="oil_wells.db")
    parser.add_argument("--output-dir", default=None)
    return parser.parse_args()


def main():
    args = parse_arguments()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    pdf_paths = sorted(
        [
            os.path.join(args.data_dir, name)
            for name in os.listdir(args.data_dir)
            if name.lower().endswith(".pdf")
        ]
    )

    if not pdf_paths:
        logging.error("No PDF files found")
        return

    connection = sqlite3.connect(args.db_path)
    initialize_database(connection)

    processed = 0

    for path in pdf_paths:
        logging.info(f"Processing {os.path.basename(path)}")

        text = extract_text_from_pdf(path)

        well_info = parse_well_info(text)
        well_info["source_file"] = os.path.basename(path)
        well_info["raw_text"] = None

        well_id = insert_or_update_well(connection, well_info)

        stim = parse_stimulation(text)
        if stim:
            stim["raw_text"] = None
            insert_stimulation(connection, well_id, stim)

        processed += 1

    connection.commit()

    if args.output_dir:
        export_csv(connection, args.output_dir)

    logging.info(f"Done. Processed {processed} PDFs")

    connection.close()


if __name__ == "__main__":
    main()