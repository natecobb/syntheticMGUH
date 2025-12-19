#!/usr/bin/env python3

"""
Load Synthea CSV data and export to Parquet format using DuckDB.

This script:
1. Loads Synthea CSV files into DuckDB
2. Performs data cleaning and transformations
3. Exports tables to Parquet format for analytics
4. Optionally processes clinical notes

Advantages over PostgreSQL approach:
- Faster loading and querying
- Parquet files are columnar and compressed
- No database server required
- Can be synced to cloud storage easily
"""

import os
import re
from pathlib import Path
from datetime import datetime
from argparse import ArgumentParser
from dateutil import parser as date_parser
import duckdb


def main(
    synthea_data_path='../output',
    output_path='../output/parquet',
    ehr_launch_date=datetime.strptime('2010-01-01', '%Y-%m-%d'),
    include_notes=False,
    keep_descriptions=False,
    duckdb_path=None  # None = in-memory, or provide path for persistent DB
):
    """
    Load Synthea CSVs into DuckDB and export to Parquet format.

    Args:
        synthea_data_path: Path to Synthea output folder (contains csv/ subdirectory)
        output_path: Path where Parquet files will be written
        ehr_launch_date: Filter data to simulate EHR launch date
        include_notes: Whether to process clinical notes
        keep_descriptions: Keep description columns in tables (default: False, saves space)
        duckdb_path: Path for DuckDB database file (None for in-memory)
    """

    # Create output directory
    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)

    csv_path = Path(synthea_data_path) / "csv"

    # Tables and their date columns for parsing
    synthea_tables = {
        "allergies": ["STOP", "START"],
        "careplans": ["STOP", "START"],
        "claims_transactions": ["FROMDATE", "TODATE"],
        "claims": ["SERVICEDATE", "LASTBILLEDDATE1", "LASTBILLEDDATE2", "LASTBILLEDDATEP"],
        "conditions": ["STOP", "START"],
        "devices": ["STOP", "START"],
        "encounters": ["STOP", "START"],
        "imaging_studies": ["DATE"],
        "immunizations": ["DATE"],
        "medications": ["STOP", "START"],
        "observations": ["DATE"],
        "organizations": [],
        "patients": ["BIRTHDATE", "DEATHDATE"],
        "payers": [],
        "payer_transitions": ["START_DATE", "END_DATE"],
        "procedures": ["START", "STOP"],
        "providers": [],
        "supplies": ["DATE"]
    }

    # Tables to filter by date (to simulate EHR launch)
    # Synthea keeps encounters cascading from active careplans, creating enormous datasets
    # Trim EHR operational tables to only after start date (these are real-time EHR data)
    # Keep historical/backloaded tables (conditions, medications, etc. could have been manually entered)
    filtered_tables = [
        "encounters",           # EHR data - encounters only exist from launch date
        "claims",               # EHR data - claims generated from encounters
        "claims_transactions",  # EHR data - claim transactions
        "imaging_studies",      # EHR data - radiology studies
        "observations",         # EHR data - labs and vitals
        "payer_transitions",    # EHR data - insurance changes tracked in system
        "procedures",           # EHR data - procedures performed at encounters
        "supplies"              # EHR data - supplies used at encounters
    ]
    # Tables NOT filtered (historical data that could be backloaded):
    # allergies, careplans, conditions, devices, immunizations, medications,
    # organizations, patients, payers, providers

    print(f"Loading Synthea data from: {synthea_data_path}")
    print(f"Output Parquet files to: {output_path}")
    print(f"EHR launch date filter: {ehr_launch_date}")

    # Connect to DuckDB
    con = duckdb.connect(duckdb_path if duckdb_path else ':memory:')

    # Set DuckDB to use multiple threads and configure memory
    con.execute("SET threads TO 4")
    con.execute("SET memory_limit = '4GB'")

    print("\n" + "="*60)
    print("LOADING CSV FILES")
    print("="*60)

    # Load each CSV file into DuckDB
    for table_name, date_fields in synthea_tables.items():
        csv_file = csv_path / f"{table_name}.csv"

        if not csv_file.exists():
            print(f"⚠ Skipping {table_name} (file not found)")
            continue

        print(f"\n{table_name}:")
        print(f"  Reading CSV...")

        # Build date parsing columns string for DuckDB
        date_parse_cols = ""
        if date_fields:
            # DuckDB can parse timestamps automatically
            date_cols_str = ", ".join([f"TRY_CAST({col} AS TIMESTAMP) AS {col}" for col in date_fields])
            # Get all columns first
            all_cols = con.execute(f"SELECT * FROM read_csv_auto('{csv_file}', sample_size=1000) LIMIT 0").description
            all_col_names = [col[0] for col in all_cols]

            # Build SELECT with date conversions (with timezone)
            select_parts = []
            for col in all_col_names:
                if col.upper() in [d.upper() for d in date_fields]:
                    # Add America/New_York timezone (handles EST/EDT automatically)
                    select_parts.append(f"timezone('America/New_York', TRY_CAST({col} AS TIMESTAMP)) AS {col}")
                else:
                    select_parts.append(col)
            date_parse_cols = ", ".join(select_parts)
        else:
            date_parse_cols = "*"

        # Load CSV with automatic type detection
        # DuckDB is smart about type inference
        load_query = f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT {date_parse_cols}
            FROM read_csv_auto('{csv_file}',
                               header=true,
                               sample_size=10000,
                               ignore_errors=true)
        """
        con.execute(load_query)

        # Make column names lowercase (Synthea uses uppercase)
        columns = con.execute(f"DESCRIBE {table_name}").fetchall()
        rename_cols = [f'"{col[0]}" AS {col[0].lower()}' for col in columns]
        con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT {', '.join(rename_cols)}
            FROM {table_name}
        """)

        # Filter by date if applicable
        if table_name in filtered_tables and date_fields:
            date_col = date_fields[0].lower()
            print(f"  Filtering by {date_col} >= {ehr_launch_date}")
            con.execute(f"""
                DELETE FROM {table_name}
                WHERE {date_col} < '{ehr_launch_date.strftime('%Y-%m-%d')}'
                  AND {date_col} IS NOT NULL
            """)

        # Drop duplicates
        if 'id' in [col[0].lower() for col in columns]:
            print(f"  Dropping duplicates...")
            con.execute(f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT DISTINCT ON (id) *
                FROM {table_name}
            """)

        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"  ✓ Loaded {row_count:,} rows")

    # Process clinical notes if requested
    if include_notes:
        print(f"\n{'='*60}")
        print("LOADING CLINICAL NOTES")
        print('='*60)

        notes_path = Path(synthea_data_path) / "notes"
        if notes_path.exists():
            print("\nCreating notes table...")
            con.execute("""
                CREATE TABLE IF NOT EXISTS notes (
                    patient VARCHAR,
                    date TIMESTAMPTZ,
                    note_text VARCHAR
                )
            """)

            regex_id = r"(^.*_)(.*?)\.txt"
            regex_notes = r"([1-2][0-9]{3}-[0-9]{2}-[0-9]{2}.*)\n\n([\s\S]*?)\n\n\n\n"

            notes_files = [f for f in notes_path.iterdir() if f.suffix == '.txt']
            print(f"Processing {len(notes_files)} note files...")

            for i, file_path in enumerate(notes_files, 1):
                if i % 100 == 0:
                    print(f"  Processed {i}/{len(notes_files)} files...")

                patient_match = re.search(regex_id, file_path.name)
                if not patient_match:
                    continue

                patient_id = patient_match.group(2)
                all_notes = file_path.read_text()
                notes = re.findall(regex_notes, all_notes)

                for note_date, note_text in notes:
                    # Filter notes by EHR launch date
                    note_dt = date_parser.parse(note_date)
                    if note_dt < ehr_launch_date:
                        continue

                    # Escape single quotes in note text
                    note_text_escaped = note_text.replace("'", "''")
                    con.execute(f"""
                        INSERT INTO notes (patient, date, note_text)
                        VALUES ('{patient_id}', timezone('America/New_York', '{note_date}'::TIMESTAMP), '{note_text_escaped}')
                    """)

            note_count = con.execute("SELECT COUNT(*) FROM notes").fetchone()[0]
            print(f"  ✓ Loaded {note_count:,} clinical notes")
        else:
            print(f"  ⚠ Notes directory not found: {notes_path}")

    print(f"\n{'='*60}")
    print("DATA TRANSFORMATIONS")
    print('='*60)

    # Add first_encounter_id to patients
    print("\nAdding first encounter ID to patients...")
    con.execute("""
        ALTER TABLE patients ADD COLUMN IF NOT EXISTS first_encounter_id VARCHAR;

        UPDATE patients
        SET first_encounter_id = (
            SELECT id
            FROM encounters
            WHERE encounters.patient = patients.id
            ORDER BY start ASC
            LIMIT 1
        )
    """)

    # Fix orphaned encounters in certain tables
    print("Fixing orphaned encounter IDs...")
    tables_to_fix = ["devices", "medications", "conditions", "allergies", "careplans", "immunizations"]

    for table in tables_to_fix:
        if con.execute(f"SELECT table_name FROM information_schema.tables WHERE table_name = '{table}'").fetchone():
            print(f"  {table}...")
            con.execute(f"""
                UPDATE {table}
                SET encounter = (
                    SELECT first_encounter_id
                    FROM patients
                    WHERE patients.id = {table}.patient
                )
                WHERE encounter NOT IN (SELECT id FROM encounters)
                  AND encounter IS NOT NULL
            """)

    # Clean tables by removing rows without valid encounters
    print("Cleaning tables with missing encounters...")
    tables_to_clean = ["observations", "imaging_studies", "procedures", "supplies"]

    for table in tables_to_clean:
        if con.execute(f"SELECT table_name FROM information_schema.tables WHERE table_name = '{table}'").fetchone():
            print(f"  {table}...")
            con.execute(f"""
                DELETE FROM {table}
                WHERE encounter IS NOT NULL
                  AND encounter NOT IN (SELECT id FROM encounters)
            """)

    # Drop orphan observations (Synthea glitch)
    print("Dropping orphan observations...")
    con.execute("""
        DELETE FROM observations
        WHERE encounter IS NULL
    """)

    print(f"\n{'='*60}")
    print("BUILDING CODE DICTIONARY")
    print('='*60)

    # Build codes table before dropping descriptions
    # This creates a normalized lookup table for all medical codes
    print("\nExtracting codes from all tables...")
    con.execute("""
        CREATE TABLE codes AS
        SELECT DISTINCT ON (vocab, code) vocab, code, LOWER(description) AS description
        FROM (
            -- SNOMED codes from allergies
            SELECT DISTINCT 'SNOMED' AS vocab,
                           CAST(code AS VARCHAR) AS code,
                           LOWER(description) AS description
            FROM allergies
            WHERE code IS NOT NULL

            UNION

            -- SNOMED codes from careplans
            SELECT DISTINCT 'SNOMED' AS vocab,
                           CAST(code AS VARCHAR) AS code,
                           LOWER(description) AS description
            FROM careplans
            WHERE code IS NOT NULL

            UNION

            -- SNOMED codes from conditions
            SELECT DISTINCT 'SNOMED' AS vocab,
                           CAST(code AS VARCHAR) AS code,
                           LOWER(description) AS description
            FROM conditions
            WHERE code IS NOT NULL

            UNION

            -- SNOMED codes from devices
            SELECT DISTINCT 'SNOMED' AS vocab,
                           CAST(code AS VARCHAR) AS code,
                           LOWER(description) AS description
            FROM devices
            WHERE code IS NOT NULL

            UNION

            -- SNOMED codes from encounters
            SELECT DISTINCT 'SNOMED' AS vocab,
                           CAST(code AS VARCHAR) AS code,
                           LOWER(description) AS description
            FROM encounters
            WHERE code IS NOT NULL

            UNION

            -- SNOMED codes from procedures
            SELECT DISTINCT 'SNOMED' AS vocab,
                           CAST(code AS VARCHAR) AS code,
                           LOWER(description) AS description
            FROM procedures
            WHERE code IS NOT NULL

            UNION

            -- SNOMED reason codes from procedures
            SELECT DISTINCT 'SNOMED' AS vocab,
                           CAST(reasoncode AS VARCHAR) AS code,
                           LOWER(reasondescription) AS description
            FROM procedures
            WHERE reasoncode IS NOT NULL

            UNION

            -- SNOMED codes from supplies
            SELECT DISTINCT 'SNOMED' AS vocab,
                           CAST(code AS VARCHAR) AS code,
                           LOWER(description) AS description
            FROM supplies
            WHERE code IS NOT NULL

            UNION

            -- SNOMED body site codes from imaging studies
            SELECT DISTINCT 'SNOMED' AS vocab,
                           CAST(bodysite_code AS VARCHAR) AS code,
                           LOWER(bodysite_description) AS description
            FROM imaging_studies
            WHERE bodysite_code IS NOT NULL

            UNION

            -- RxNorm codes from medications
            SELECT DISTINCT 'RxNorm' AS vocab,
                           CAST(code AS VARCHAR) AS code,
                           LOWER(description) AS description
            FROM medications
            WHERE code IS NOT NULL

            UNION

            -- CVX codes from immunizations
            SELECT DISTINCT 'CVX' AS vocab,
                           CAST(code AS VARCHAR) AS code,
                           LOWER(description) AS description
            FROM immunizations
            WHERE code IS NOT NULL

            UNION

            -- LOINC codes from observations
            SELECT DISTINCT 'LOINC' AS vocab,
                           CAST(code AS VARCHAR) AS code,
                           LOWER(description) AS description
            FROM observations
            WHERE code IS NOT NULL

            UNION

            -- DICOM modality codes from imaging studies
            SELECT DISTINCT 'DICOM-DCM' AS vocab,
                           CAST(modality_code AS VARCHAR) AS code,
                           LOWER(modality_description) AS description
            FROM imaging_studies
            WHERE modality_code IS NOT NULL

            UNION

            -- DICOM SOP codes from imaging studies
            SELECT DISTINCT 'DICOM-SOP' AS vocab,
                           CAST(sop_code AS VARCHAR) AS code,
                           LOWER(sop_description) AS description
            FROM imaging_studies
            WHERE sop_code IS NOT NULL
        ) AS all_codes
        WHERE code IS NOT NULL
        ORDER BY vocab, code
    """)

    # Show code dictionary stats
    vocab_counts = con.execute("""
        SELECT vocab, COUNT(*) AS count
        FROM codes
        GROUP BY vocab
        ORDER BY vocab
    """).fetchall()

    print("\nCode dictionary statistics:")
    for vocab, count in vocab_counts:
        print(f"  {vocab:15s}: {count:,} codes")

    total_codes = sum(count for _, count in vocab_counts)
    print(f"  {'TOTAL':15s}: {total_codes:,} codes")

    # Drop description columns to save space (unless explicitly kept)
    if not keep_descriptions:
        print(f"\n{'='*60}")
        print("REMOVING DESCRIPTION COLUMNS")
        print('='*60)
        print("Description fields are redundant - use codes.parquet for lookups")

        # Tables with description columns to drop
        tables_with_desc = {
            'allergies': ['description'],
            'careplans': ['description'],
            'conditions': ['description'],
            'devices': ['description'],
            'encounters': ['description'],
            'procedures': ['description', 'reasondescription'],
            'supplies': ['description'],
            'imaging_studies': ['bodysite_description', 'modality_description', 'sop_description'],
            'medications': ['description'],
            'immunizations': ['description'],
            'observations': ['description']
        }

        for table_name, columns_to_drop in tables_with_desc.items():
            # Check if table exists
            if con.execute(f"SELECT table_name FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone():
                # Get current columns
                current_cols = con.execute(f"DESCRIBE {table_name}").fetchall()
                current_col_names = [col[0] for col in current_cols]

                # Filter out columns to drop
                keep_cols = [col for col in current_col_names if col not in columns_to_drop]

                if len(keep_cols) < len(current_col_names):
                    dropped = [col for col in columns_to_drop if col in current_col_names]
                    print(f"  {table_name}: dropping {', '.join(dropped)}")

                    # Recreate table without description columns
                    con.execute(f"""
                        CREATE TABLE {table_name}_new AS
                        SELECT {', '.join(keep_cols)}
                        FROM {table_name}
                    """)
                    con.execute(f"DROP TABLE {table_name}")
                    con.execute(f"ALTER TABLE {table_name}_new RENAME TO {table_name}")
    else:
        print("\n⚠ Keeping description columns (--keep_descriptions enabled)")

    print(f"\n{'='*60}")
    print("EXPORTING TO PARQUET")
    print('='*60)

    # Export each table to Parquet
    tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()

    for (table_name,) in tables:
        print(f"\n{table_name}:")

        parquet_file = output_path / f"{table_name}.parquet"

        # Export to Parquet with compression
        con.execute(f"""
            COPY {table_name}
            TO '{parquet_file}'
            (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
        """)

        # Get file size
        file_size_mb = parquet_file.stat().st_size / (1024 * 1024)
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        print(f"  ✓ {row_count:,} rows → {file_size_mb:.2f} MB")

    # Calculate total size
    total_size_mb = sum(f.stat().st_size for f in output_path.glob("*.parquet")) / (1024 * 1024)

    print(f"\n{'='*60}")
    print(f"✓ COMPLETE")
    print('='*60)
    print(f"Total Parquet files: {len(list(output_path.glob('*.parquet')))}")
    print(f"Total size: {total_size_mb:.2f} MB")
    print(f"Output location: {output_path.absolute()}")

    # Close connection
    con.close()

    print(f"\n{'='*60}")
    print("USAGE")
    print('='*60)
    print("Query with DuckDB:")
    print(f"  duckdb -c \"SELECT COUNT(*) FROM '{output_path}/patients.parquet'\"")
    print("\nQuery with Python:")
    print("  import duckdb")
    print(f"  con = duckdb.connect()")
    print(f"  df = con.execute(\"SELECT * FROM '{output_path}/patients.parquet'\").df()")


if __name__ == '__main__':
    parser = ArgumentParser(
        prog='load_csv_to_parquet.py',
        description='Load Synthea CSV data and export to Parquet using DuckDB'
    )
    parser.add_argument('--path', default='../output',
                       help='Path to Synthea output folder (default: ../output)')
    parser.add_argument('--output', default='../output/parquet',
                       help='Path for Parquet output files (default: ../output/parquet)')
    parser.add_argument('--start', default='2010-01-01 00:00:00Z',
                       type=lambda s: date_parser.parse(s),
                       help='EHR launch date for filtering (default: 2010-01-01)')
    parser.add_argument('--include_notes', action='store_true', default=False,
                       help='Include clinical notes processing')
    parser.add_argument('--keep_descriptions', action='store_true', default=False,
                       help='Keep description columns in tables (default: False to save space)')
    parser.add_argument('--duckdb', default=None,
                       help='Path for persistent DuckDB file (default: in-memory)')

    args = parser.parse_args()

    main(
        synthea_data_path=args.path,
        output_path=args.output,
        ehr_launch_date=args.start,
        include_notes=args.include_notes,
        keep_descriptions=args.keep_descriptions,
        duckdb_path=args.duckdb
    )
