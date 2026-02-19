# DSCI-560 Lab 6

## Environment / Setup
- Python 3.9+
- Required packages (see `requirements.txt`)
- OCR tools (for scanned PDFs): Tesseract OCR, Poppler (for `pdf2image`)

### Linux install (Ubuntu/Debian)
```bash
sudo apt-get update
sudo apt-get install -y tesseract-ocr poppler-utils
python -m pip install -r requirements.txt
```

### macOS (if needed)
```bash
brew install tesseract poppler
python -m pip install -r requirements.txt
```

## Data Collection / Storage
- PDFs are stored in `data/`
- Parsed data are stored in a SQLite database (default `oil_wells.db`)
- Two tables are created:
  - `wells`: API, well name, operator, county/state, SHL location, lat/long (raw + decimal), datum
  - `stimulations`: stimulation table fields from the PDF snapshot (date, formation, depths, stages, volumes, etc.)

## PDF Extraction
Script: `scripts/extract_load.py`

Run with limit 5
```bash
python scripts/extract_load.py --data-dir data --db-path oil_wells.db --output-dir outputs --limit 5 --keep-raw
```

Run with OCR enabled:
```bash
python scripts/extract_load.py --data-dir data --db-path oil_wells.db --output-dir outputs --ocr --keep-raw
```

Outputs:
- SQLite DB: `oil_wells.db`
- CSV exports: `outputs/wells.csv`, `outputs/stimulations.csv` (only when `--output-dir` is provided)

Notes:
- The script uses `pdfplumber` for embedded text and falls back to OCR when pages are scanned.
- `--keep-raw` stores the full extracted text in the DB for debugging and verification.
- If OCR tools are missing, the script logs a warning and continues using only embedded text.

## Part 4: Additional Web Scraped Information
Script: `scripts/scrape_drillingedge.py`

What it does:
- Reads each well from `oil_wells.db`.
- Searches drillingedge.com using API and well name.
- Opens the well page and extracts Well Status, Well Type, Closest City, Barrels of Oil Produced (with month), and MCF of Gas Produced (with month).
- Appends these fields to the existing `wells` table.

Run:
```bash
python scripts/scrape_drillingedge.py --db-path oil_wells.db --sleep 1.0 --export-dir outputs --limit 5
```

## Part 5: Data Preprocessing
The scraper also performs preprocessing after scraping:
- Removes HTML tags and non-printable characters from scraped text.
- Converts production values like `2.2k` to integers (2200).
- Normalizes `date_stimulated` to `YYYY-MM-DD` when possible.
- Replaces missing text values with `N/A` and missing numeric values with `0`.

Preprocess only (no scraping):
```bash
python scripts/scrape_drillingedge.py --db-path oil_wells.db --preprocess-only
```

Outputs:
- Updated SQLite DB: `oil_wells.db`
- Updated CSVs (optional): `outputs/wells.csv`, `outputs/stimulations.csv`

Notes:
- The script waits between requests (`--sleep`) to be polite to the website.
- If a well page is not found, the script skips it and continues.

## Part 6 (Web Access and Visualization)

## What this part requires
- Serve a web page that displays a map.
- Plot wells as markers using latitude/longitude from the database.
- Show a popup with well info, stimulation info, and web-scraped info.
- Document setup steps for running the web page.

## Files added
- `scripts/export_geojson.py` exports data from `oil_wells.db` to `web/data/wells.geojson`.
- `web/index.html`, `web/styles.css`, `web/app.js` implement the map and popups using Leaflet.

## How to run
1) Export GeoJSON from the database:
```bash
python scripts/export_geojson.py --db-path oil_wells.db --output web/data/wells.geojson
```

2) Serve the `web/` folder.

Option A (Apache): Copy the `web/` folder into your Apache document root and open `http://localhost/` in a browser.

Option B (quick local test):
```bash
python -m http.server 8000 --directory web
```
Open `http://localhost:8000`.

## Notes
- Only wells with valid coordinates are included in the GeoJSON.
- Popups show well details, stimulation details, and drillingedge.com data.

