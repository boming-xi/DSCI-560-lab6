from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sqlite3
from typing import Dict, Optional, Tuple

import pdfplumber


MIN_TEXT_CHARS = 30
DEGREE_SYMBOL = "\u00b0"
HEADER_PHRASES = {
    "section township range county",
    "qtr-qtr section township range county",
    "spacing unit description",
    "24-hour production rate",
    "production rate",
    "final report date",
    "location of well",
    "operator telephone number field",
    "field i pool county gas mcf",
    "telephone number field",
    "see details",
    "footages qtr-qtr section township range",
    "water bbls water bbls",
    "oil bbls oil bbls",
    "gas mcf gas mcf",
}


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


def get_lines(text: str) -> list[str]:
    return [line.strip() for line in text.splitlines() if line.strip()]


def is_header_line(line: str) -> bool:
    lower = line.strip().lower()
    if len(lower) < 3:
        return True
    compact = re.sub(r"\s+", "", lower)
    for phrase in HEADER_PHRASES:
        if phrase in lower:
            return True
        if phrase.replace(" ", "") in compact:
            return True
    return False


def find_line_after_label(lines: list[str], label: str, max_skip: int = 4) -> Optional[str]:
    label_lower = label.lower()
    for idx, line in enumerate(lines):
        if label_lower in line.lower():
            for offset in range(1, max_skip + 1):
                if idx + offset >= len(lines):
                    break
                candidate = lines[idx + offset]
                if is_header_line(candidate):
                    continue
                if candidate.lower().startswith(label_lower):
                    continue
                return candidate
    return None


def split_well_name_line(line: str) -> Tuple[Optional[str], Optional[str]]:
    pattern = re.compile(
        r"^(?P<name>.+?)\s+(?P<section>\d{1,2})\s+(?P<township>\d{1,3})\s*(?P<ns>[NS])\s+"
        r"(?P<range>\d{1,3})\s*(?P<ew>[EW])?\s+(?P<county>[A-Za-z].+)$",
        re.IGNORECASE,
    )
    match = pattern.match(line.strip())
    if not match:
        return line.strip(), None
    name = match.group("name").strip()
    county = match.group("county").strip()
    if is_header_line(county):
        return line.strip(), None
    return name, county


def clean_well_name(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    cleaned = re.sub(r"\bBefore\s+After\b", "", value, flags=re.IGNORECASE).strip()
    if is_header_line(cleaned):
        return None
    tokens = cleaned.split()
    if tokens and all(len(token) <= 1 for token in tokens):
        return None
    return cleaned or None


def extract_well_name_inline(lines: list[str]) -> Optional[str]:
    for line in lines:
        lower = line.lower()
        if not lower.startswith("well name"):
            continue
        if "well name and number" in lower or "well name well number" in lower:
            continue
        if ":" in line:
            candidate = line.split(":", 1)[1].strip()
            candidate = re.split(r"\bDirectional\s+Drillers\b", candidate, 1, flags=re.IGNORECASE)[0]
            candidate = clean_well_name(candidate)
            if candidate:
                return candidate
    return None


def is_bad_operator(value: Optional[str]) -> bool:
    if not value:
        return True
    lower = value.lower()
    if "api" in lower:
        return True
    if "telephone" in lower:
        return True
    return is_header_line(value)


def extract_operator(lines: list[str]) -> Optional[str]:
    op_line = find_line_after_label(lines, "Operator")
    if not op_line:
        return None
    match = re.search(r"^(.*?)(\d{3}[-\s]?\d{3}[-\s]?\d{4})", op_line)
    if match:
        return match.group(1).strip()
    return op_line.strip()


def extract_operator_inline(lines: list[str]) -> Optional[str]:
    for line in lines:
        lower = line.lower()
        if "operator" not in lower:
            continue
        if "operator telephone number" in lower:
            continue
        if "operator:" in lower:
            candidate = re.sub(r"^.*?operator\s*[:#]?\s*", "", line, flags=re.IGNORECASE)
            candidate = re.split(r"\bAPI\b|\bTIGHT\s+HOLE\b|\bYES\b|\bNO\b", candidate, 1, flags=re.IGNORECASE)[0]
            candidate = candidate.strip()
            if candidate and not is_header_line(candidate):
                return candidate
    return None


def extract_api(text: str) -> Optional[str]:
    match = re.search(r"API\s*[:#]?\s*([0-9]{2}[-\s]?[0-9]{3}[-\s]?[0-9]{5})", text, re.IGNORECASE)
    if match:
        digits = re.sub(r"\D", "", match.group(1))
        return f"{digits[:2]}-{digits[2:5]}-{digits[5:10]}" if len(digits) >= 10 else match.group(1)
    for line in get_lines(text):
        if "api" not in line.lower():
            continue
        digits = re.sub(r"\D", "", line)
        if len(digits) >= 10:
            digits = digits[:10]
            return f"{digits[:2]}-{digits[2:5]}-{digits[5:10]}"
    return None


def extract_state_fallback(text: str) -> Optional[str]:
    if re.search(r"\bNorth\s+Dakota\b", text, re.IGNORECASE):
        return "North Dakota"
    if re.search(r"\bND\b", text):
        return "North Dakota"
    return None


def extract_coord(label: str, text: str) -> Optional[str]:
    lines = get_lines(text)
    for idx, line in enumerate(lines):
        if label.lower() in line.lower():
            candidates = [line]
            if idx + 1 < len(lines):
                candidates.append(lines[idx + 1])
            for candidate in candidates:
                match = re.search(
                    r"(\d{1,3}\s*[^0-9\s]?\s*\d{1,2}\s*['\s]\s*\d{1,2}(?:\.\d+)?\s*[NSEW])",
                    candidate,
                    re.IGNORECASE,
                )
                if match:
                    return normalize_space(match.group(1))
    return None


def parse_county_state(raw: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not raw:
        return None, None
    raw = normalize_space(raw)
    if not raw:
        return None, None
    if raw in {".", "-", "--"}:
        return None, None
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    if len(parts) >= 2:
        county = parts[0].replace("County", "").strip()
        state = parts[1].replace("State", "").strip()
        return county or None, state or None
    tokens = raw.split()
    if len(tokens) >= 2:
        county = " ".join(tokens[:-1]).replace("County", "").strip()
        state = tokens[-1].replace("State", "").strip()
        return county or None, state or None
    return raw, None


def dms_to_decimal(dms: Optional[str]) -> Optional[float]:
    if not dms:
        return None
    raw = dms.replace(DEGREE_SYMBOL, " ").replace("'", " ").replace('"', " ")
    nums = re.findall(r"\d+(?:\.\d+)?", raw)
    if not nums:
        return None
    deg = float(nums[0])
    minutes = float(nums[1]) if len(nums) > 1 else 0.0
    seconds = float(nums[2]) if len(nums) > 2 else 0.0
    decimal = deg + minutes / 60.0 + seconds / 3600.0
    if re.search(r"\bS\b", raw, re.IGNORECASE) or re.search(r"\bW\b", raw, re.IGNORECASE):
        decimal *= -1
    return decimal


def to_int(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    cleaned = re.sub(r"[^0-9.-]", "", value)
    if not cleaned:
        return None
    try:
        return int(float(cleaned))
    except ValueError:
        return None


def to_float(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    cleaned = re.sub(r"[^0-9.-]", "", value)
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def extract_text_from_pdf(path: str, use_ocr: bool = False, ocr_dpi: int = 300) -> str:
    texts = []
    with pdfplumber.open(path) as pdf:
        for idx, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            if use_ocr and len(text.strip()) < MIN_TEXT_CHARS:
                ocr_text = ocr_page(path, idx, ocr_dpi)
                if ocr_text:
                    text = f"{text}\n{ocr_text}" if text else ocr_text
            texts.append(text)
    return "\n".join(texts).strip()


def ocr_page(path: str, page_number: int, dpi: int = 300) -> str:
    try:
        from pdf2image import convert_from_path
        import pytesseract
    except Exception as exc:  # pragma: no cover - optional deps
        logging.warning("OCR skipped (missing dependencies): %s", exc)
        return ""

    try:
        images = convert_from_path(path, dpi=dpi, first_page=page_number, last_page=page_number)
    except Exception as exc:  # pragma: no cover - poppler missing
        logging.warning("OCR skipped (pdf2image conversion failed): %s", exc)
        return ""

    if not images:
        return ""

    try:
        return pytesseract.image_to_string(images[0], config="--psm 6")
    except Exception as exc:  # pragma: no cover - tesseract missing
        logging.warning("OCR skipped (tesseract failed): %s", exc)
        return ""


def parse_well_info(text: str) -> Dict[str, Optional[str]]:
    info = {}
    info["api"] = extract_api(text)
    lines = get_lines(text)
    county_state = None
    county_state_from_well = None
    well_name = extract_first(
        [
            r"Well Name\s*and\s*Number\s*[:#]?\s*(.+)",
            r"Well Name\s*[:#]?\s*(.+)",
        ],
        text,
    )
    if well_name and is_header_line(well_name):
        well_name = None
    if not well_name:
        well_name = extract_well_name_inline(lines)
    if not well_name:
        well_name = find_line_after_label(lines, "Well Name and Number")
    if well_name:
        split_name, county_from_line = split_well_name_line(well_name)
        well_name = clean_well_name(split_name)
        if county_from_line:
            county_state_from_well = county_from_line
    if not well_name:
        well_name = find_line_after_label(lines, "Well Name Well Number")
        well_name = clean_well_name(well_name)
    info["well_name"] = well_name
    info["operator"] = extract_first(
        [
            r"Operator\s*[:#]?\s*(.+)",
            r"Name of Operator\s*[:#]?\s*(.+)",
        ],
        text,
    )
    if info["operator"] and is_bad_operator(info["operator"]):
        info["operator"] = None
    if not info["operator"]:
        info["operator"] = extract_operator_inline(lines)
    if not info["operator"]:
        info["operator"] = extract_operator(lines)
    county_state = extract_first(
        [
            r"County\s*,?\s*State\s*[:#]?\s*(.+)",
            r"County\s*State\s*[:#]?\s*(.+)",
        ],
        text,
    )
    if not county_state:
        county_state = county_state_from_well
    if county_state and is_header_line(county_state):
        county_state = None
    if not county_state:
        for line in lines:
            if "county" in line.lower() and not is_header_line(line):
                match = re.search(r"County\s*[:#]?\s*([A-Za-z\\s.-]+)", line, re.IGNORECASE)
                if match:
                    county_state = match.group(1)
                    break
    county, state = parse_county_state(county_state)
    if not state:
        state = extract_state_fallback(text)
    info["county"] = county
    info["state"] = state
    info["shl_location"] = extract_first(
        [
            r"Well Surface Hole Location\s*\(SHL\)\s*[:#]?\s*(.+)",
            r"Surface Hole Location\s*[:#]?\s*(.+)",
        ],
        text,
    )
    info["latitude"] = extract_coord("Latitude", text) or extract_coord("Lat", text)
    info["longitude"] = extract_coord("Longitude", text) or extract_coord("Long", text)
    info["datum"] = extract_first(
        [
            r"Datum\s*[:#]?\s*([A-Za-z0-9\s.-]+)",
        ],
        text,
    )
    info["latitude_decimal"] = dms_to_decimal(info["latitude"])
    info["longitude_decimal"] = dms_to_decimal(info["longitude"])
    return info


def parse_stimulation(text: str) -> Optional[Dict[str, Optional[str]]]:
    stim = {
        "date_stimulated": extract_first([r"Date\s*Stimulated\s*([0-9/\-]+)"], text),
        "stimulated_formation": extract_first([r"Stimulated\s*Formation\s*([A-Za-z0-9 /-]+)"], text),
        "top_ft": extract_first([r"Top\s*\(Ft\)\s*([0-9,]+)"], text),
        "bottom_ft": extract_first([r"Bottom\s*\(Ft\)\s*([0-9,]+)"], text),
        "stimulation_stages": extract_first([r"Stimulation\s*Stages\s*([0-9,]+)"], text),
        "volume": extract_first([r"Volume\s*([0-9,]+)"], text),
        "volume_units": extract_first([r"Volume\s*Units\s*([A-Za-z]+)"], text),
        "treatment_type": extract_first([r"Type\s*Treatment\s*([A-Za-z0-9 /-]+)"], text),
        "acid_pct": extract_first([r"Acid\s*%\s*([0-9.]+)"], text),
        "lbs_proppant": extract_first([r"Lbs\s*Proppant\s*([0-9,]+)"], text),
        "max_treatment_pressure_psi": extract_first(
            [r"Maximum\s*Treatment\s*Pressure\s*\(PSI\)\s*([0-9,]+)"],
            text,
        ),
        "max_treatment_rate": extract_first(
            [r"Maximum\s*Treatment\s*Rate\s*\(BBLS/Min\)\s*([0-9.]+)"],
            text,
        ),
    }

    details_match = re.search(r"Details\s*(.+?)(?:\n\s*\n|$)", text, re.IGNORECASE | re.DOTALL)
    if details_match:
        stim["details"] = normalize_space(details_match.group(1))
    else:
        stim["details"] = None

    if not any(v for v in stim.values()):
        return None

    return stim


def ensure_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
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
            raw_text TEXT
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
        """
    )


def upsert_well(conn: sqlite3.Connection, data: Dict[str, Optional[str]]) -> int:
    cur = conn.execute("SELECT id FROM wells WHERE source_file = ?", (data["source_file"],))
    row = cur.fetchone()
    if row:
        well_id = row[0]
        conn.execute(
            """
            UPDATE wells
               SET api = ?, well_name = ?, operator = ?, county = ?, state = ?, shl_location = ?,
                   latitude = ?, longitude = ?, latitude_decimal = ?, longitude_decimal = ?,
                   datum = ?, raw_text = ?
             WHERE id = ?
            """,
            (
                data["api"],
                data["well_name"],
                data["operator"],
                data["county"],
                data["state"],
                data["shl_location"],
                data["latitude"],
                data["longitude"],
                data["latitude_decimal"],
                data["longitude_decimal"],
                data["datum"],
                data["raw_text"],
                well_id,
            ),
        )
        return well_id

    cur = conn.execute(
        """
        INSERT INTO wells (
            api, well_name, operator, county, state, shl_location, latitude, longitude,
            latitude_decimal, longitude_decimal, datum, source_file, raw_text
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            data["api"],
            data["well_name"],
            data["operator"],
            data["county"],
            data["state"],
            data["shl_location"],
            data["latitude"],
            data["longitude"],
            data["latitude_decimal"],
            data["longitude_decimal"],
            data["datum"],
            data["source_file"],
            data["raw_text"],
        ),
    )
    return cur.lastrowid


def insert_stimulation(conn: sqlite3.Connection, well_id: int, stim: Dict[str, Optional[str]]) -> None:
    conn.execute("DELETE FROM stimulations WHERE well_id = ?", (well_id,))
    conn.execute(
        """
        INSERT INTO stimulations (
            well_id, date_stimulated, stimulated_formation, top_ft, bottom_ft,
            stimulation_stages, volume, volume_units, treatment_type, acid_pct,
            lbs_proppant, max_treatment_pressure_psi, max_treatment_rate,
            max_treatment_rate_units, details, raw_text
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            well_id,
            stim.get("date_stimulated"),
            stim.get("stimulated_formation"),
            to_int(stim.get("top_ft")),
            to_int(stim.get("bottom_ft")),
            to_int(stim.get("stimulation_stages")),
            to_int(stim.get("volume")),
            stim.get("volume_units"),
            stim.get("treatment_type"),
            to_float(stim.get("acid_pct")),
            to_int(stim.get("lbs_proppant")),
            to_int(stim.get("max_treatment_pressure_psi")),
            to_float(stim.get("max_treatment_rate")),
            "BBLS/Min" if stim.get("max_treatment_rate") else None,
            stim.get("details"),
            stim.get("raw_text"),
        ),
    )


def export_csv(conn: sqlite3.Connection, output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)

    for table in ("wells", "stimulations"):
        rows = conn.execute(f"SELECT * FROM {table}").fetchall()
        columns = conn.execute(f"PRAGMA table_info({table})").fetchall()  # cid, name, type...
        header_names = [col[1] for col in columns]
        out_path = os.path.join(output_dir, f"{table}.csv")
        with open(out_path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(header_names)
            writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract oil-well data from PDFs and load SQLite DB.")
    parser.add_argument("--data-dir", default="data", help="Directory containing PDF files.")
    parser.add_argument("--db-path", default="oil_wells.db", help="SQLite database path.")
    parser.add_argument("--output-dir", default=None, help="Optional output directory for CSV exports.")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of PDFs to process.")
    parser.add_argument("--ocr", action="store_true", help="Enable OCR for scanned pages.")
    parser.add_argument("--ocr-dpi", type=int, default=300, help="DPI for OCR conversion.")
    parser.add_argument("--keep-raw", action="store_true", help="Store raw text in DB.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    data_dir = args.data_dir
    pdf_paths = sorted(
        [os.path.join(data_dir, name) for name in os.listdir(data_dir) if name.lower().endswith(".pdf")]
    )
    if args.limit:
        pdf_paths = pdf_paths[: args.limit]

    if not pdf_paths:
        logging.error("No PDF files found in %s", data_dir)
        return

    conn = sqlite3.connect(args.db_path)
    ensure_db(conn)

    processed = 0
    for path in pdf_paths:
        logging.info("Processing %s", os.path.basename(path))
        text = extract_text_from_pdf(path, use_ocr=args.ocr, ocr_dpi=args.ocr_dpi)
        if not text:
            logging.warning("No text extracted from %s", os.path.basename(path))
            continue

        well_info = parse_well_info(text)
        well_info["source_file"] = os.path.basename(path)
        well_info["raw_text"] = text if args.keep_raw else None

        well_id = upsert_well(conn, well_info)

        stim = parse_stimulation(text)
        if stim:
            stim["raw_text"] = text if args.keep_raw else None
            insert_stimulation(conn, well_id, stim)

        processed += 1

    conn.commit()

    if args.output_dir:
        export_csv(conn, args.output_dir)
        logging.info("CSV exports written to %s", args.output_dir)

    logging.info("Done. Processed %d PDFs", processed)


if __name__ == "__main__":
    main()
