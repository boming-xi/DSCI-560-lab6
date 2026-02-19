from __future__ import annotations

import argparse
import csv
import datetime as dt
import logging
import os
import re
import sqlite3
import time
from typing import Dict, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://www.drillingedge.com"
SEARCH_URL = f"{BASE_URL}/search"
DEFAULT_TIMEOUT = 25

HEADER_PHRASES = {
    "section township range county",
    "qtr-qtr section township range county",
    "spacing unit description",
    "24-hour production rate",
    "production rate",
    "final report date",
    "location of well",
}

STATE_SLUGS = {
    "AL": "alabama",
    "AK": "alaska",
    "AZ": "arizona",
    "AR": "arkansas",
    "CA": "california",
    "CO": "colorado",
    "CT": "connecticut",
    "DE": "delaware",
    "FL": "florida",
    "GA": "georgia",
    "HI": "hawaii",
    "ID": "idaho",
    "IL": "illinois",
    "IN": "indiana",
    "IA": "iowa",
    "KS": "kansas",
    "KY": "kentucky",
    "LA": "louisiana",
    "ME": "maine",
    "MD": "maryland",
    "MA": "massachusetts",
    "MI": "michigan",
    "MN": "minnesota",
    "MS": "mississippi",
    "MO": "missouri",
    "MT": "montana",
    "NE": "nebraska",
    "NV": "nevada",
    "NH": "new-hampshire",
    "NJ": "new-jersey",
    "NM": "new-mexico",
    "NY": "new-york",
    "NC": "north-carolina",
    "ND": "north-dakota",
    "OH": "ohio",
    "OK": "oklahoma",
    "OR": "oregon",
    "PA": "pennsylvania",
    "RI": "rhode-island",
    "SC": "south-carolina",
    "SD": "south-dakota",
    "TN": "tennessee",
    "TX": "texas",
    "UT": "utah",
    "VT": "vermont",
    "VA": "virginia",
    "WA": "washington",
    "WV": "west-virginia",
    "WI": "wisconsin",
    "WY": "wyoming",
}


def slugify(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = value.strip().lower()
    value = value.replace("&", "and")
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or None


def normalize_state_slug(state: Optional[str]) -> Optional[str]:
    if not state:
        return None
    raw = state.strip()
    abbr = re.sub(r"[^A-Za-z]", "", raw).upper()
    if abbr in STATE_SLUGS:
        return STATE_SLUGS[abbr]
    return slugify(raw)


def normalize_county_slug(county: Optional[str]) -> Optional[str]:
    if not county:
        return None
    raw = county.strip()
    lower = raw.lower()
    suffix = "-county"
    if "parish" in lower:
        suffix = "-parish"
        raw = re.sub(r"(?i)\bparish\b", "", raw)
    elif "borough" in lower:
        suffix = "-borough"
        raw = re.sub(r"(?i)\bborough\b", "", raw)
    else:
        raw = re.sub(r"(?i)\bcounty\b", "", raw)
    base = slugify(raw)
    return f"{base}{suffix}" if base else None


def normalize_api(api: Optional[str]) -> Optional[str]:
    if not api:
        return None
    digits = re.sub(r"\D", "", api)
    if len(digits) == 10:
        return f"{digits[:2]}-{digits[2:5]}-{digits[5:]}"
    return api.strip()


def clean_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str) and value.strip().lower().startswith(("http://", "https://")):
        return value.strip()
    value = BeautifulSoup(value, "html.parser").get_text(" ", strip=True)
    value = re.sub(r"[\x00-\x1f\x7f]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value or None


def is_valid_api(api: Optional[str]) -> bool:
    if not api:
        return False
    digits = re.sub(r"\D", "", api)
    return len(digits) == 10


def is_probable_well_name(name: Optional[str]) -> bool:
    if not name:
        return False
    lower = name.strip().lower()
    if lower in {"n/a", "na", "none"}:
        return False
    if len(lower) < 4:
        return False
    for phrase in HEADER_PHRASES:
        if phrase in lower:
            return False
    return True


def parse_scaled_number(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    raw = value.strip().lower().replace(",", "")
    match = re.match(r"([0-9.]+)\s*([km]?)", raw)
    if not match:
        return None
    number = float(match.group(1))
    suffix = match.group(2)
    if suffix == "k":
        number *= 1_000
    elif suffix == "m":
        number *= 1_000_000
    return int(round(number))


def extract_between(text: str, start_label: str, end_labels) -> Optional[str]:
    end_pattern = "|".join(re.escape(label) for label in end_labels)
    pattern = rf"{re.escape(start_label)}\s*[:\-]?\s*(.*?)\s*(?=(?:{end_pattern})|$)"
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    if match:
        return clean_text(match.group(1))
    return None


def month_to_ym(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    for fmt in ("%b %Y", "%B %Y"):
        try:
            parsed = dt.datetime.strptime(value, fmt)
            return parsed.strftime("%Y-%m")
        except ValueError:
            continue
    return value


def fetch_html(url: str, params: Optional[dict] = None, timeout: int = DEFAULT_TIMEOUT) -> Tuple[str, str]:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; DSCI-560 bot/1.0; +https://example.com)"
    }
    response = requests.get(url, params=params, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.text, response.url


def detect_search_params(html: str) -> Tuple[Optional[str], Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    api_key = None
    well_key = None
    for inp in soup.find_all("input"):
        attrs = " ".join(str(inp.get(attr, "")) for attr in ("name", "id", "placeholder", "aria-label"))
        attrs_lower = attrs.lower()
        key = inp.get("name") or inp.get("id")
        if key and "api" in attrs_lower and not api_key:
            api_key = key
        if key and "well" in attrs_lower and "name" in attrs_lower and not well_key:
            well_key = key
    return api_key, well_key


def find_well_link_in_html(html: str, api: Optional[str]) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    api_norm = normalize_api(api) if api else None
    api_digits = re.sub(r"\D", "", api_norm or "")

    for link in soup.find_all("a", href=True):
        href = link["href"]
        text = link.get_text(" ", strip=True)
        if "/wells/" in href:
            if api_norm and api_norm in href:
                return urljoin(BASE_URL, href)
            if api_digits and api_digits in re.sub(r"\D", "", href):
                return urljoin(BASE_URL, href)
            if api_norm and api_norm in text:
                return urljoin(BASE_URL, href)
    return None


def build_well_url(api: Optional[str], well_name: Optional[str], county: Optional[str], state: Optional[str]) -> Optional[str]:
    api_norm = normalize_api(api)
    state_slug = normalize_state_slug(state)
    county_slug = normalize_county_slug(county)
    well_slug = slugify(well_name)
    if not (api_norm and state_slug and county_slug and well_slug):
        return None
    return f"{BASE_URL}/{state_slug}/{county_slug}/wells/{well_slug}/{api_norm}"


def find_well_in_county(api: Optional[str], county: Optional[str], state: Optional[str], max_pages: int) -> Optional[str]:
    api_norm = normalize_api(api)
    if not api_norm:
        return None
    state_slug = normalize_state_slug(state)
    county_slug = normalize_county_slug(county)
    if not (state_slug and county_slug):
        return None
    if state_slug not in STATE_SLUGS.values():
        return None

    base_url = f"{BASE_URL}/{state_slug}/{county_slug}/wells"
    for page in range(1, max_pages + 1):
        url = base_url if page == 1 else f"{base_url}?page={page}"
        try:
            html, _ = fetch_html(url)
        except Exception as exc:
            logging.warning("Failed county page %s: %s", url, exc)
            continue
        if api_norm not in html:
            continue
        link = find_well_link_in_html(html, api_norm)
        if link:
            return link
    return None


def search_well_page(api: Optional[str], well_name: Optional[str], county: Optional[str], state: Optional[str], max_pages: int) -> Optional[str]:
    try:
        search_html, _ = fetch_html(SEARCH_URL)
    except Exception as exc:
        logging.warning("Search page fetch failed: %s", exc)
        search_html = ""

    api_key, well_key = detect_search_params(search_html)
    params = {}
    query_terms = " ".join([term for term in (api, well_name) if term])
    if api_key and api:
        params[api_key] = api
    if well_key and well_name:
        params[well_key] = well_name
    if not params and query_terms:
        params = {"q": query_terms}

    if params:
        try:
            result_html, _ = fetch_html(SEARCH_URL, params=params)
            link = find_well_link_in_html(result_html, api)
            if link:
                return link
        except Exception as exc:
            logging.warning("Search query failed: %s", exc)

    if query_terms:
        try:
            result_html, _ = fetch_html(SEARCH_URL, params={"q": query_terms})
            link = find_well_link_in_html(result_html, api)
            if link:
                return link
        except Exception:
            pass

    direct_url = build_well_url(api, well_name, county, state)
    if direct_url:
        try:
            html, _ = fetch_html(direct_url)
            if api and normalize_api(api) in html:
                return direct_url
        except Exception:
            pass

    return find_well_in_county(api, county, state, max_pages)


def parse_well_page(html: str) -> Dict[str, Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)

    well_status = extract_between(text, "Well Status", ["Well Type", "Township", "County", "Closest City", "Latitude"])
    well_type = extract_between(text, "Well Type", ["Township", "County", "Closest City", "Latitude", "Permit Date"])
    closest_city = extract_between(text, "Closest City", ["Latitude", "Permit Date", "Spud Date", "Completion Date"])

    oil_match = re.search(
        r"([0-9.,]+\s*[kKmM]?)\s*Barrels of Oil Produced in\s*([A-Za-z]{3,9}\s+\d{4})",
        text,
    )
    gas_match = re.search(
        r"([0-9.,]+\s*[kKmM]?)\s*MCF of Gas Produced in\s*([A-Za-z]{3,9}\s+\d{4})",
        text,
    )

    oil_value = parse_scaled_number(oil_match.group(1)) if oil_match else None
    oil_month = month_to_ym(oil_match.group(2)) if oil_match else None

    gas_value = parse_scaled_number(gas_match.group(1)) if gas_match else None
    gas_month = month_to_ym(gas_match.group(2)) if gas_match else None

    return {
        "web_well_status": clean_text(well_status),
        "web_well_type": clean_text(well_type),
        "web_closest_city": clean_text(closest_city),
        "web_oil_bbls": oil_value,
        "web_oil_prod_month": clean_text(oil_month),
        "web_gas_mcf": gas_value,
        "web_gas_prod_month": clean_text(gas_month),
    }


def ensure_columns(conn: sqlite3.Connection) -> None:
    existing = {row[1] for row in conn.execute("PRAGMA table_info(wells)").fetchall()}
    columns = {
        "web_well_status": "TEXT",
        "web_well_type": "TEXT",
        "web_closest_city": "TEXT",
        "web_oil_bbls": "INTEGER",
        "web_oil_prod_month": "TEXT",
        "web_gas_mcf": "INTEGER",
        "web_gas_prod_month": "TEXT",
        "web_source_url": "TEXT",
        "web_scraped_at": "TEXT",
    }
    for name, col_type in columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE wells ADD COLUMN {name} {col_type}")


def update_well(conn: sqlite3.Connection, well_id: int, data: Dict[str, Optional[str]], source_url: Optional[str]) -> None:
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        """
        UPDATE wells
           SET web_well_status = ?, web_well_type = ?, web_closest_city = ?,
               web_oil_bbls = ?, web_oil_prod_month = ?, web_gas_mcf = ?,
               web_gas_prod_month = ?, web_source_url = ?, web_scraped_at = ?
         WHERE id = ?
        """,
        (
            data.get("web_well_status"),
            data.get("web_well_type"),
            data.get("web_closest_city"),
            data.get("web_oil_bbls"),
            data.get("web_oil_prod_month"),
            data.get("web_gas_mcf"),
            data.get("web_gas_prod_month"),
            source_url,
            now,
            well_id,
        ),
    )


def clean_text_fields(conn: sqlite3.Connection) -> None:
    text_cols = {
        "wells": [
            "api",
            "well_name",
            "operator",
            "county",
            "state",
            "shl_location",
            "latitude",
            "longitude",
            "datum",
            "web_well_status",
            "web_well_type",
            "web_closest_city",
            "web_oil_prod_month",
            "web_gas_prod_month",
            "web_source_url",
        ],
        "stimulations": [
            "date_stimulated",
            "stimulated_formation",
            "volume_units",
            "treatment_type",
            "details",
        ],
    }

    for table, columns in text_cols.items():
        for col in columns:
            rows = conn.execute(f"SELECT id, {col} FROM {table}").fetchall()
            for row_id, value in rows:
                cleaned = clean_text(value)
                if cleaned != value:
                    conn.execute(
                        f"UPDATE {table} SET {col} = ? WHERE id = ?",
                        (cleaned, row_id),
                    )


def normalize_missing(conn: sqlite3.Connection) -> None:
    clean_text_fields(conn)
    text_cols_wells = [
        "api",
        "well_name",
        "operator",
        "county",
        "state",
        "shl_location",
        "latitude",
        "longitude",
        "datum",
        "web_well_status",
        "web_well_type",
        "web_closest_city",
        "web_oil_prod_month",
        "web_gas_prod_month",
        "web_source_url",
    ]
    num_cols_wells = [
        "latitude_decimal",
        "longitude_decimal",
        "web_oil_bbls",
        "web_gas_mcf",
    ]

    for col in text_cols_wells:
        conn.execute(f"UPDATE wells SET {col} = 'N/A' WHERE {col} IS NULL OR TRIM({col}) = ''")
    for col in num_cols_wells:
        conn.execute(f"UPDATE wells SET {col} = 0 WHERE {col} IS NULL")

    text_cols_stim = [
        "date_stimulated",
        "stimulated_formation",
        "volume_units",
        "treatment_type",
        "details",
    ]
    num_cols_stim = [
        "top_ft",
        "bottom_ft",
        "stimulation_stages",
        "volume",
        "acid_pct",
        "lbs_proppant",
        "max_treatment_pressure_psi",
        "max_treatment_rate",
    ]

    for col in text_cols_stim:
        conn.execute(f"UPDATE stimulations SET {col} = 'N/A' WHERE {col} IS NULL OR TRIM({col}) = ''")
    for col in num_cols_stim:
        conn.execute(f"UPDATE stimulations SET {col} = 0 WHERE {col} IS NULL")

    normalize_dates(conn)


def normalize_dates(conn: sqlite3.Connection) -> None:
    rows = conn.execute("SELECT id, date_stimulated FROM stimulations").fetchall()
    for stim_id, date_value in rows:
        if not date_value or date_value == "N/A":
            continue
        cleaned = date_value.strip()
        parsed = None
        for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y", "%m-%d-%y"):
            try:
                parsed = dt.datetime.strptime(cleaned, fmt)
                break
            except ValueError:
                continue
        if parsed:
            conn.execute(
                "UPDATE stimulations SET date_stimulated = ? WHERE id = ?",
                (parsed.strftime("%Y-%m-%d"), stim_id),
            )


def export_csv(conn: sqlite3.Connection, output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)
    for table in ("wells", "stimulations"):
        rows = conn.execute(f"SELECT * FROM {table}").fetchall()
        columns = conn.execute(f"PRAGMA table_info({table})").fetchall()
        header_names = [col[1] for col in columns]
        out_path = os.path.join(output_dir, f"{table}.csv")
        with open(out_path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(header_names)
            writer.writerows(rows)


def export_scrape_csv(conn: sqlite3.Connection, output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)
    columns = [
        "id",
        "api",
        "well_name",
        "county",
        "state",
        "web_well_status",
        "web_well_type",
        "web_closest_city",
        "web_oil_bbls",
        "web_oil_prod_month",
        "web_gas_mcf",
        "web_gas_prod_month",
        "web_source_url",
        "web_scraped_at",
    ]
    rows = conn.execute(
        f"""
        SELECT {', '.join(columns)}
          FROM wells
         WHERE web_source_url IS NOT NULL
           AND TRIM(web_source_url) != ''
           AND web_source_url != 'N/A'
         ORDER BY id
        """
    ).fetchall()
    out_path = os.path.join(output_dir, "web_scrape.csv")
    with open(out_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape drillingedge.com for well info and preprocess DB.")
    parser.add_argument("--db-path", default="oil_wells.db", help="SQLite database path.")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of wells to process.")
    parser.add_argument("--sleep", type=float, default=1.0, help="Seconds to sleep between requests.")
    parser.add_argument("--max-pages", type=int, default=5, help="Max county pages to scan on fallback.")
    parser.add_argument("--export-dir", default=None, help="Optional directory to export CSVs.")
    parser.add_argument("--preprocess-only", action="store_true", help="Only run preprocessing steps.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    conn = sqlite3.connect(args.db_path)
    ensure_columns(conn)

    if not args.preprocess_only:
        wells = conn.execute(
            "SELECT id, api, well_name, county, state FROM wells ORDER BY id"
        ).fetchall()

        if args.limit:
            wells = wells[: args.limit]

        for idx, (well_id, api, well_name, county, state) in enumerate(wells, start=1):
            api_norm = normalize_api(api)
            query_api = api_norm if is_valid_api(api_norm) else None
            query_name = well_name if is_probable_well_name(well_name) else None
            if not query_api and not query_name:
                logging.info("[%d/%d] Skipping (no usable API/well name)", idx, len(wells))
                continue
            logging.info("[%d/%d] Searching %s", idx, len(wells), query_api or query_name)
            try:
                well_url = search_well_page(query_api, query_name, county, state, args.max_pages)
                if not well_url:
                    logging.warning("No well page found for %s", api or well_name)
                    continue
                html, _ = fetch_html(well_url)
                parsed = parse_well_page(html)
                update_well(conn, well_id, parsed, well_url)
            except Exception as exc:
                logging.warning("Failed to scrape %s: %s", api or well_name, exc)
            finally:
                conn.commit()
                time.sleep(args.sleep)

    normalize_missing(conn)
    conn.commit()

    if args.export_dir:
        export_csv(conn, args.export_dir)
        export_scrape_csv(conn, args.export_dir)
        logging.info("CSV exports written to %s", args.export_dir)

    logging.info("Done")


if __name__ == "__main__":
    main()
