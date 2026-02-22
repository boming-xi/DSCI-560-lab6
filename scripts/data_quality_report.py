import sqlite3
import argparse


def generate_report(database_path:str)->None:
    database_connection=sqlite3.connect(database_path)

    total_pdfs=database_connection.execute(
        "SELECT COUNT(*) FROM wells"
    ).fetchone()[0]

    valid_coords=database_connection.execute("""
        SELECT COUNT(*)
        FROM wells
        WHERE latitude_decimal IS NOT NULL
          AND longitude_decimal IS NOT NULL
          AND latitude_decimal != 0
          AND longitude_decimal != 0
    """).fetchone()[0]

    invalid_coords=total_pdfs-valid_coords

    scraped_success=database_connection.execute("""
        SELECT COUNT(*)
        FROM wells
        WHERE web_well_status IS NOT NULL
          AND web_well_status != 'N/A'
    """).fetchone()[0]

    stimulation_records=database_connection.execute(
        "SELECT COUNT(*) FROM stimulations"
    ).fetchone()[0]

    print("="*50)
    print("DATA QUALITY REPORT")
    print("="*50)
    print(f"Total PDFs processed: {total_pdfs}")
    print(f"Wells with valid coordinates: {valid_coords}")
    print(f"Wells excluded (no valid coordinates): {invalid_coords}")
    print(f"Wells successfully enriched (web data): {scraped_success}")
    print(f"Total stimulation records: {stimulation_records}")
    print("="*50)

    database_connection.close()


def parse_arguments():
    parser=argparse.ArgumentParser(
        description="Generate data quality report."
    )
    parser.add_argument(
        "--db-path",
        default="oil_wells.db"
    )
    return parser.parse_args()


if __name__=="__main__":
    arguments=parse_arguments()
    generate_report(arguments.db_path)