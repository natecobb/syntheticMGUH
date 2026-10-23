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
import shutil
from pathlib import Path
from datetime import datetime, timezone
from argparse import ArgumentParser
from dateutil import parser as date_parser
import duckdb
import pandas as pd


def _dir_size_mb(path: Path) -> float:
    """Return total size in MB of all files under path."""
    if not path.exists():
        return 0.0
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file()) / (1024 * 1024)


def main(
    synthea_data_path='./output',
    output_path='./output/parquet',
    ehr_launch_date=date_parser.parse('2010-01-01 00:00:00Z'),
    include_notes=False,
    keep_descriptions=False,
    duckdb_path=None,  # None = in-memory, or provide path for persistent DB
    append=False,      # Merge with existing Parquet files in output_path
    cleanup=False,     # Delete raw csv/notes/symptoms/metadata after Parquet export
    no_claims=False    # Skip claims and claims_transactions tables
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
        append: Merge new batch with existing Parquet files in output_path
        cleanup: Remove raw csv/, notes/, symptoms/, metadata/ directories after export
    """

    # Create output directory
    synthea_root = Path(synthea_data_path)
    if not (synthea_root / "csv").exists() and (synthea_root / "latest" / "csv").exists():
        synthea_root = (synthea_root / "latest").resolve()
        if str(output_path) == './output/parquet':
            output_path = synthea_root / "parquet"

    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)

    # Keep ./output/parquet symlinked to the active seed's parquet folder
    base_out = Path("./output").resolve()
    if output_path.parent.parent.resolve() == base_out and output_path.parent.name.startswith("seed_"):
        pq_link = base_out / "parquet"
        try:
            if pq_link.is_symlink() or pq_link.exists():
                if pq_link.is_dir() and not pq_link.is_symlink():
                    shutil.rmtree(pq_link, ignore_errors=True)
                else:
                    pq_link.unlink()
            pq_link.symlink_to(Path(output_path.parent.name) / "parquet")
        except OSError:
            pass

    csv_path = synthea_root / "csv"

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
    if no_claims:
        synthea_tables.pop("claims", None)
        synthea_tables.pop("claims_transactions", None)

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

    # Load symptoms CSV if exported (output/symptoms/csv/symptoms.csv)
    symptoms_file = synthea_root / "symptoms" / "csv" / "symptoms.csv"
    if symptoms_file.exists():
        print("\nsymptoms:")
        print("  Reading CSV...")
        con.execute(f"""
            CREATE OR REPLACE TABLE symptoms AS
            SELECT *
            FROM read_csv_auto('{symptoms_file}',
                               header=true,
                               sample_size=10000,
                               ignore_errors=true)
        """)
        columns = con.execute("DESCRIBE symptoms").fetchall()
        rename_cols = [f'"{col[0]}" AS {col[0].lower()}' for col in columns]
        con.execute(f"""
            CREATE OR REPLACE TABLE symptoms AS
            SELECT {', '.join(rename_cols)}
            FROM symptoms
            WHERE LOWER("{columns[0][0]}") IS NOT NULL
        """)
        # Keep only symptoms for loaded patients
        con.execute("""
            DELETE FROM symptoms
            WHERE patient NOT IN (SELECT id FROM patients)
        """)
        row_count = con.execute("SELECT COUNT(*) FROM symptoms").fetchone()[0]
        print(f"  ✓ Loaded {row_count:,} rows")

    # Process clinical notes if requested
    if include_notes:
        print(f"\n{'='*60}")
        print("LOADING CLINICAL NOTES")
        print('='*60)

        notes_path = synthea_root / "notes"
        if notes_path.exists():
            print("\nCreating notes table...")
            con.execute("""
                CREATE OR REPLACE TABLE notes (
                    patient VARCHAR,
                    date TIMESTAMPTZ,
                    note_text VARCHAR,
                    encounter VARCHAR
                )
            """)

            regex_id = r"(^.*_)(.*?)\.txt"
            regex_notes = r"([1-2][0-9]{3}-[0-9]{2}-[0-9]{2}.*?)\n\n([\s\S]*?)(?=\n\n\n\n|\Z)"

            notes_files = sorted(f for f in notes_path.iterdir() if f.suffix == '.txt')
            print(f"Processing {len(notes_files)} note files...")

            if ehr_launch_date.tzinfo is None:
                ehr_launch_date = ehr_launch_date.replace(tzinfo=timezone.utc)

            note_rows = []
            for i, file_path in enumerate(notes_files, 1):
                if i % 100 == 0:
                    print(f"  Processed {i}/{len(notes_files)} files...")

                patient_match = re.search(regex_id, file_path.name)
                if not patient_match:
                    continue

                patient_id = patient_match.group(2)
                all_notes = file_path.read_text(encoding='utf-8')
                notes = re.findall(regex_notes, all_notes)

                for note_date, note_text in notes:
                    note_date = note_date.strip()
                    note_text = note_text.strip()
                    if not note_text:
                        continue
                    # Filter notes by EHR launch date
                    note_dt = date_parser.parse(note_date)
                    if note_dt.tzinfo is None:
                        note_dt = note_dt.replace(tzinfo=timezone.utc)
                    if note_dt < ehr_launch_date:
                        continue

                    note_rows.append((patient_id, note_date, note_text))

            if note_rows:
                notes_df = pd.DataFrame(note_rows, columns=["patient", "note_date", "note_text"])
                con.execute("""
                    INSERT INTO notes (patient, date, note_text, encounter)
                    SELECT patient,
                           timezone('America/New_York', TRY_CAST(note_date AS TIMESTAMP)),
                           note_text,
                           NULL
                    FROM notes_df
                """)
                print("  Associating notes with encounters...")
                con.execute("""
                    UPDATE notes
                    SET encounter = (
                        SELECT e.id
                        FROM encounters e
                        WHERE e.patient = notes.patient
                          AND CAST(notes.date AS DATE) BETWEEN CAST(e.start AS DATE)
                              AND COALESCE(CAST(e.stop AS DATE), CAST(notes.date AS DATE))
                        ORDER BY ABS(EPOCH(notes.date) - EPOCH(e.start)) ASC
                        LIMIT 1
                    )
                """)
                unmatched = con.execute("SELECT COUNT(*) FROM notes WHERE encounter IS NULL").fetchone()[0]
                if unmatched > 0:
                    print(f"  Removing {unmatched:,} notes without matching encounter...")
                    con.execute("DELETE FROM notes WHERE encounter IS NULL")

            note_count = con.execute("SELECT COUNT(*) FROM notes").fetchone()[0]
            print(f"  ✓ Loaded {note_count:,} clinical notes linked to encounters")
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

    # Clean claims and claims_transactions referencing filtered-out encounters
    if con.execute("SELECT table_name FROM information_schema.tables WHERE table_name = 'claims'").fetchone():
        print("  claims...")
        con.execute("""
            DELETE FROM claims
            WHERE appointmentid IS NOT NULL
              AND appointmentid NOT IN (SELECT id FROM encounters)
        """)
    if con.execute("SELECT table_name FROM information_schema.tables WHERE table_name = 'claims_transactions'").fetchone():
        print("  claims_transactions...")
        con.execute("""
            DELETE FROM claims_transactions
            WHERE (appointmentid IS NOT NULL AND appointmentid NOT IN (SELECT id FROM encounters))
               OR (claimid IS NOT NULL AND claimid NOT IN (SELECT id FROM claims))
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
    loaded_table_names = {
        r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }
    code_sources = [
        ("allergies", "SNOMED", "code", "description"),
        ("allergies", "SNOMED", "reaction1", "description1"),
        ("allergies", "SNOMED", "reaction2", "description2"),
        ("careplans", "SNOMED", "code", "description"),
        ("careplans", "SNOMED", "reasoncode", "reasondescription"),
        ("conditions", "SNOMED", "code", "description"),
        ("devices", "SNOMED", "code", "description"),
        ("encounters", "SNOMED", "code", "description"),
        ("encounters", "SNOMED", "reasoncode", "reasondescription"),
        ("medications", "SNOMED", "reasoncode", "reasondescription"),
        ("procedures", "SNOMED", "code", "description"),
        ("procedures", "SNOMED", "reasoncode", "reasondescription"),
        ("supplies", "SNOMED", "code", "description"),
        ("imaging_studies", "SNOMED", "bodysite_code", "bodysite_description"),
        ("medications", "RxNorm", "code", "description"),
        ("immunizations", "CVX", "code", "description"),
        ("observations", "LOINC", "code", "description"),
        ("imaging_studies", "DICOM-DCM", "modality_code", "modality_description"),
        ("imaging_studies", "DICOM-SOP", "sop_code", "sop_description"),
    ]
    union_queries = [
        f"""
        SELECT DISTINCT '{vocab}' AS vocab,
                        CAST({code_col} AS VARCHAR) AS code,
                        LOWER({desc_col}) AS description
        FROM {tbl}
        WHERE {code_col} IS NOT NULL
        """
        for tbl, vocab, code_col, desc_col in code_sources
        if tbl in loaded_table_names
    ]
    con.execute(f"""
        CREATE TABLE codes AS
        SELECT DISTINCT ON (vocab, code) vocab, code, LOWER(description) AS description
        FROM (
            {" UNION ".join(union_queries)}
        ) AS all_codes
        WHERE code IS NOT NULL
        ORDER BY vocab, code
    """)

    # Ensure any procedurecode in claims_transactions not already in codes is captured before dropping notes
    if "claims_transactions" in loaded_table_names:
        con.execute("""
            INSERT INTO codes
            SELECT DISTINCT ON (code)
                'SNOMED' AS vocab,
                CAST(ct.procedurecode AS VARCHAR) AS code,
                LOWER(ct.notes) AS description
            FROM claims_transactions ct
            WHERE ct.procedurecode IS NOT NULL
              AND ct.notes IS NOT NULL
              AND CAST(ct.procedurecode AS VARCHAR) NOT IN (SELECT code FROM codes)
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

    # Map symptoms.pathology (SNOMED display text) to SNOMED code from codes table
    if "symptoms" in loaded_table_names:
        symptom_cols = {c[0].lower() for c in con.execute("DESCRIBE symptoms").fetchall()}
        if "pathology" in symptom_cols and "code" not in symptom_cols:
            print("\nMapping symptoms.pathology to SNOMED codes...")
            con.execute("""
                CREATE OR REPLACE TABLE symptoms AS
                SELECT
                    s.patient,
                    s.gender,
                    s.race,
                    s.ethnicity,
                    s.age_begin,
                    s.age_end,
                    TRY_CAST(c.code AS BIGINT) AS code,
                    s.pathology,
                    s.num_symptoms,
                    s.symptoms
                FROM symptoms s
                LEFT JOIN (
                    SELECT DISTINCT ON (description) description, code
                    FROM codes
                    WHERE vocab = 'SNOMED'
                ) c ON LOWER(s.pathology) = c.description
            """)

    # Drop description columns to save space (unless explicitly kept)
    if not keep_descriptions:
        print(f"\n{'='*60}")
        print("REMOVING DESCRIPTION COLUMNS")
        print('='*60)
        print("Description fields are redundant - use codes.parquet for lookups")

        # Tables with description columns to drop
        tables_with_desc = {
            'allergies': ['description', 'description1', 'description2'],
            'careplans': ['description', 'reasondescription'],
            'claims_transactions': ['notes'],
            'conditions': ['description'],
            'devices': ['description'],
            'encounters': ['description', 'reasondescription'],
            'procedures': ['description', 'reasondescription'],
            'supplies': ['description'],
            'imaging_studies': ['bodysite_description', 'modality_description', 'sop_description'],
            'medications': ['description', 'reasondescription'],
            'immunizations': ['description'],
            'observations': ['description'],
            'symptoms': ['pathology', 'gender', 'race', 'ethnicity']
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

    # Merge with existing Parquet files when running incrementally (--append)
    existing_patients_pq = output_path / "patients.parquet"
    if append and existing_patients_pq.exists():
        print(f"\n{'='*60}")
        print("MERGING BATCH WITH EXISTING PARQUET TABLES")
        print('='*60)

        con.execute(f"""
            CREATE TEMP TABLE _existing_patients AS
            SELECT id, ssn FROM read_parquet('{existing_patients_pq}')
        """)

        # Ensure idempotency: drop any batch records for patient IDs already in existing Parquet
        overlap_count = con.execute("""
            SELECT COUNT(*) FROM patients WHERE id IN (SELECT id FROM _existing_patients)
        """).fetchone()[0]
        if overlap_count > 0:
            print(f"  Deduplicating {overlap_count:,} overlapping patients from batch...")
            con.execute("DELETE FROM patients WHERE id IN (SELECT id FROM _existing_patients)")
            for tbl, pcol in [
                ("allergies", "patient"), ("careplans", "patient"), ("claims", "patientid"),
                ("claims_transactions", "patientid"), ("conditions", "patient"), ("devices", "patient"),
                ("encounters", "patient"), ("imaging_studies", "patient"), ("immunizations", "patient"),
                ("medications", "patient"), ("observations", "patient"), ("payer_transitions", "patient"),
                ("procedures", "patient"), ("supplies", "patient"), ("symptoms", "patient"), ("notes", "patient")
            ]:
                if con.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema='main' AND table_name='{tbl}'").fetchone():
                    con.execute(f"DELETE FROM {tbl} WHERE {pcol} IN (SELECT id FROM _existing_patients)")

        # Prevent cross-batch SSN collisions (Synthea only deduplicates SSNs within a single JVM run)
        con.execute("""
            UPDATE patients
            SET ssn = FORMAT(
                '999-{:02d}-{:04d}',
                10 + (ABS(HASH(id)) % 90),
                1000 + ((ABS(HASH(id)) // 90) % 9000)
            )
            WHERE ssn IN (SELECT ssn FROM _existing_patients)
        """)

        # 1. Merge codes dictionary
        codes_pq = output_path / "codes.parquet"
        if codes_pq.exists():
            con.execute(f"""
                CREATE OR REPLACE TABLE codes AS
                SELECT DISTINCT ON (vocab, code) vocab, CAST(code AS VARCHAR) AS code, description
                FROM (
                    SELECT vocab, CAST(code AS VARCHAR) AS code, description FROM read_parquet('{codes_pq}')
                    UNION ALL
                    SELECT vocab, CAST(code AS VARCHAR) AS code, description FROM codes
                )
                ORDER BY vocab, code
            """)

        # 2. Merge organizations (sum revenue & utilization by id)
        orgs_pq = output_path / "organizations.parquet"
        if orgs_pq.exists():
            con.execute(f"""
                CREATE OR REPLACE TABLE organizations AS
                SELECT
                    id,
                    FIRST(name) AS name,
                    FIRST(address) AS address,
                    FIRST(city) AS city,
                    FIRST(state) AS state,
                    FIRST(zip) AS zip,
                    FIRST(lat) AS lat,
                    FIRST(lon) AS lon,
                    FIRST(phone) AS phone,
                    SUM(COALESCE(revenue, 0)) AS revenue,
                    SUM(COALESCE(utilization, 0)) AS utilization,
                    FIRST(npi) AS npi
                FROM (
                    SELECT * FROM read_parquet('{orgs_pq}')
                    UNION ALL BY NAME
                    SELECT * FROM organizations
                )
                GROUP BY id
            """)

        # 3. Merge providers (sum encounters & procedures by id)
        provs_pq = output_path / "providers.parquet"
        if provs_pq.exists():
            con.execute(f"""
                CREATE OR REPLACE TABLE providers AS
                SELECT
                    id,
                    FIRST(organization) AS organization,
                    FIRST(name) AS name,
                    FIRST(gender) AS gender,
                    FIRST(speciality) AS speciality,
                    FIRST(address) AS address,
                    FIRST(city) AS city,
                    FIRST(state) AS state,
                    FIRST(zip) AS zip,
                    FIRST(lat) AS lat,
                    FIRST(lon) AS lon,
                    SUM(COALESCE(encounters, 0)) AS encounters,
                    SUM(COALESCE(procedures, 0)) AS procedures,
                    FIRST(npi) AS npi
                FROM (
                    SELECT * FROM read_parquet('{provs_pq}')
                    UNION ALL BY NAME
                    SELECT * FROM providers
                )
                GROUP BY id
            """)

        # 4. Merge payers (sum counters & average qols_avg by id)
        payers_pq = output_path / "payers.parquet"
        if payers_pq.exists():
            con.execute(f"""
                CREATE OR REPLACE TABLE payers AS
                SELECT
                    id,
                    FIRST(name) AS name,
                    FIRST(ownership) AS ownership,
                    FIRST(address) AS address,
                    FIRST(city) AS city,
                    FIRST(state_headquartered) AS state_headquartered,
                    FIRST(zip) AS zip,
                    FIRST(phone) AS phone,
                    SUM(COALESCE(amount_covered, 0)) AS amount_covered,
                    SUM(COALESCE(amount_uncovered, 0)) AS amount_uncovered,
                    SUM(COALESCE(revenue, 0)) AS revenue,
                    SUM(COALESCE(covered_encounters, 0)) AS covered_encounters,
                    SUM(COALESCE(uncovered_encounters, 0)) AS uncovered_encounters,
                    SUM(COALESCE(covered_medications, 0)) AS covered_medications,
                    SUM(COALESCE(uncovered_medications, 0)) AS uncovered_medications,
                    SUM(COALESCE(covered_procedures, 0)) AS covered_procedures,
                    SUM(COALESCE(uncovered_procedures, 0)) AS uncovered_procedures,
                    SUM(COALESCE(covered_immunizations, 0)) AS covered_immunizations,
                    SUM(COALESCE(uncovered_immunizations, 0)) AS uncovered_immunizations,
                    SUM(COALESCE(unique_customers, 0)) AS unique_customers,
                    AVG(qols_avg) AS qols_avg,
                    SUM(COALESCE(member_months, 0)) AS member_months
                FROM (
                    SELECT * FROM read_parquet('{payers_pq}')
                    UNION ALL BY NAME
                    SELECT * FROM payers
                )
                GROUP BY id
            """)

        # 5. Merge all remaining patient & clinical tables
        dimension_tables = {"codes", "organizations", "providers", "payers"}
        main_tables = [
            r[0] for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
            if r[0] not in dimension_tables
        ]
        for table_name in main_tables:
            existing_pq = output_path / f"{table_name}.parquet"
            if not existing_pq.exists():
                continue
            existing_cnt = con.execute(f"SELECT COUNT(*) FROM read_parquet('{existing_pq}')").fetchone()[0]
            batch_cnt = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            if batch_cnt == 0:
                con.execute(f"""
                    CREATE OR REPLACE TABLE {table_name} AS
                    SELECT * FROM read_parquet('{existing_pq}')
                """)
            elif existing_cnt > 0:
                cols = {c[0].lower() for c in con.execute(f"DESCRIBE {table_name}").fetchall()}
                if "id" in cols:
                    con.execute(f"""
                        CREATE OR REPLACE TABLE {table_name} AS
                        SELECT DISTINCT ON (id) *
                        FROM (
                            SELECT * FROM read_parquet('{existing_pq}')
                            UNION ALL BY NAME
                            SELECT * FROM {table_name}
                        )
                    """)
                else:
                    con.execute(f"""
                        CREATE OR REPLACE TABLE {table_name} AS
                        SELECT * FROM read_parquet('{existing_pq}')
                        UNION ALL BY NAME
                        SELECT * FROM {table_name}
                    """)
            total_cnt = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"  {table_name:22s}: +{batch_cnt:,} rows → {total_cnt:,} total")

        con.execute("DROP TABLE IF EXISTS _existing_patients")

    print(f"\n{'='*60}")
    print("EXPORTING TO PARQUET")
    print('='*60)

    # Export each table to temporary Parquet first, then atomically rename
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()

    staged_files = []
    # Optional sort keys to cluster rows by patient/claim before ZSTD Parquet compression
    sort_orders = {
        "claims_transactions": "ORDER BY patientid, claimid, chargeid",
        "claims": "ORDER BY patientid, servicedate, id",
        "encounters": "ORDER BY patient, start, id",
        "observations": "ORDER BY patient, encounter, date",
        "procedures": "ORDER BY patient, encounter, start",
        "medications": "ORDER BY patient, start",
        "conditions": "ORDER BY patient, start",
        "notes": "ORDER BY patient, date",
    }
    for (table_name,) in tables:
        parquet_file = output_path / f"{table_name}.parquet"
        tmp_file = output_path / f".tmp_{table_name}.parquet"
        order_clause = sort_orders.get(table_name, "")

        con.execute(f"""
            COPY (SELECT * FROM {table_name} {order_clause})
            TO '{tmp_file}'
            (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
        """)
        staged_files.append((table_name, tmp_file, parquet_file))

    # Atomically swap all staged Parquet files into place
    for table_name, tmp_file, parquet_file in staged_files:
        tmp_file.replace(parquet_file)
        file_size_mb = parquet_file.stat().st_size / (1024 * 1024)
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"  ✓ {table_name:22s}: {row_count:10,d} rows → {file_size_mb:6.2f} MB")

    # Calculate total size
    total_size_mb = sum(f.stat().st_size for f in output_path.glob("*.parquet")) / (1024 * 1024)

    # Optionally delete raw human-readable exports (csv/, notes/, symptoms/, metadata/)
    if cleanup:
        print(f"\n{'='*60}")
        print("CLEANING UP RAW HUMAN-READABLE EXPORTS")
        print('='*60)
        raw_root = synthea_root
        freed_mb = 0.0
        for subdir in ["csv", "notes", "symptoms", "metadata"]:
            target_dir = raw_root / subdir
            if target_dir.exists():
                sz = _dir_size_mb(target_dir)
                shutil.rmtree(target_dir, ignore_errors=True)
                freed_mb += sz
                print(f"  ✓ Removed {target_dir} ({sz:.2f} MB)")
        print(f"  Total raw disk space freed: {freed_mb:.2f} MB")

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
    parser.add_argument('--seed', type=int, default=None,
                       help='Seed number to target ./output/seed_<seed> automatically')
    parser.add_argument('--path', default='./output',
                       help='Path to Synthea output folder (default: ./output or ./output/seed_<seed>)')
    parser.add_argument('--output', default='./output/parquet',
                       help='Path for Parquet output files (default: ./output/parquet or ./output/seed_<seed>/parquet)')
    parser.add_argument('--start', default='2010-01-01 00:00:00Z',
                       type=lambda s: date_parser.parse(s),
                       help='EHR launch date for filtering (default: 2010-01-01)')
    parser.add_argument('--include_notes', action='store_true', default=False,
                       help='Include clinical notes processing')
    parser.add_argument('--keep_descriptions', action='store_true', default=False,
                       help='Keep description columns in tables (default: False to save space)')
    parser.add_argument('--duckdb', default=None,
                       help='Path for persistent DuckDB file (default: in-memory)')
    parser.add_argument('--append', action='store_true', default=False,
                       help='Merge new batch with existing Parquet files in output directory')
    parser.add_argument('--cleanup', action='store_true', default=False,
                       help='Delete raw csv/, notes/, symptoms/, and metadata/ directories after export')
    parser.add_argument('--no_claims', action='store_true', default=False,
                       help='Skip claims and claims_transactions tables to reduce storage footprint')

    args = parser.parse_args()
    if args.seed is not None:
        if args.path == './output':
            args.path = f'./output/seed_{args.seed}'
        if args.output == './output/parquet':
            args.output = f'./output/seed_{args.seed}/parquet'

    main(
        synthea_data_path=args.path,
        output_path=args.output,
        ehr_launch_date=args.start,
        include_notes=args.include_notes,
        keep_descriptions=args.keep_descriptions,
        duckdb_path=args.duckdb,
        append=args.append,
        cleanup=args.cleanup,
        no_claims=args.no_claims
    )
