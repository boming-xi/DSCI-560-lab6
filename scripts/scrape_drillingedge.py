from __future__ import annotations

import argparse
import csv
import datetime as datetime_module
import logging
import os
import re
import sqlite3
import time
from typing import Dict, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


BASE_URL="https://www.drillingedge.com"
SEARCH_URL=f"{BASE_URL}/search"
DEFAULT_TIMEOUT=25


def parse_arguments()->argparse.Namespace:
    parser=argparse.ArgumentParser(description="Scrape drillingedge well data.")
    parser.add_argument("--db-path",default="oil_wells.db")
    parser.add_argument("--delay",type=float,default=1.0)
    parser.add_argument("--limit",type=int,default=None)
    return parser.parse_args()


def fetch_page(url:str)->Optional[str]:
    try:
        response=requests.get(url,timeout=DEFAULT_TIMEOUT)
        if response.status_code==200:
            return response.text
    except Exception:
        return None
    return None


def update_web_status(connection:sqlite3.Connection,
                      well_id:int,
                      status:str)->None:
    connection.execute(
        "UPDATE wells SET web_well_status=? WHERE id=?",
        (status,well_id)
    )


def scrape_single_well(connection:sqlite3.Connection,
                       well_id:int,
                       api:str,
                       delay:float)->None:

    search_url=f"{SEARCH_URL}?api={api}"
    html=fetch_page(search_url)

    if not html:
        update_web_status(connection,well_id,"N/A")
        return

    soup=BeautifulSoup(html,"html.parser")

    status_tag=soup.find(text=re.compile("Well Status",re.IGNORECASE))
    if status_tag:
        parent=status_tag.find_parent()
        status_text=parent.get_text(strip=True)
    else:
        status_text="N/A"

    update_web_status(connection,well_id,status_text)

    time.sleep(delay)


def main()->None:
    arguments=parse_arguments()

    logging.basicConfig(level=logging.INFO,format="%(message)s")

    database_connection=sqlite3.connect(arguments.db_path)

    rows=database_connection.execute(
        "SELECT id, api FROM wells WHERE api IS NOT NULL"
    ).fetchall()

    if arguments.limit:
        rows=rows[:arguments.limit]

    processed=0

    for well_id,api in rows:
        logging.info(f"Scraping API {api}")
        scrape_single_well(
            database_connection,
            well_id,
            api,
            arguments.delay
        )
        processed+=1

    database_connection.commit()

    logging.info(f"Done. Updated {processed} wells.")

    database_connection.close()


if __name__=="__main__":
    main()