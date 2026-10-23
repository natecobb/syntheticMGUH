#!/usr/bin/env python3
"""
Validation test suite for Synthea DuckDB/Parquet output.

Verifies:
1. Parquet file existence and non-empty core tables
2. Primary key uniqueness (id, vocab+code)
3. Synthea 4.0+ features (NPI columns, no 1970-01-01 stop dates, no QALY/DALY/QOLS orphan observations, unique SSNs, MedStar-only DC routing)
4. EHR launch date filtering across operational tables
5. First encounter ID tracking on patients
6. Referential integrity (foreign keys across all clinical, claims, symptoms, and notes tables)
7. Code dictionary coverage (codes, reasoncodes, allergy reactions) and description column removal
8. Data quality checks
"""

import sys
from pathlib import Path
from datetime import datetime
from argparse import ArgumentParser
from dateutil import parser as date_parser
import duckdb


class TestResults:
    """Track test results and provide summary."""

    def __init__(self):
        self.passed = []
        self.failed = []
        self.warnings = []

    def add_pass(self, test_name: str, message: str = ""):
        self.passed.append((test_name, message))
        print(f"✓ {test_name}")
        if message:
            print(f"  {message}")

    def add_fail(self, test_name: str, message: str = ""):
        self.failed.append((test_name, message))
        print(f"✗ {test_name}")
        if message:
            print(f"  ERROR: {message}")

    def add_warning(self, test_name: str, message: str = ""):
        self.warnings.append((test_name, message))
        print(f"⚠ {test_name}")
        if message:
            print(f"  WARNING: {message}")

    def summary(self) -> bool:
        total = len(self.passed) + len(self.failed)
        print(f"\n{'='*60}")
        print("TEST SUMMARY")
        print(f"{'='*60}")
        print(f"Passed:   {len(self.passed)}/{total}")
        print(f"Failed:   {len(self.failed)}/{total}")
        print(f"Warnings: {len(self.warnings)}")

        if self.failed:
            print("\nFailed tests:")
            for name, msg in self.failed:
                print(f"  - {name}: {msg}")

        return len(self.failed) == 0


def load_parquet_views(con: duckdb.DuckDBPyConnection, parquet_dir: Path) -> set[str]:
    """Register all Parquet files in parquet_dir as DuckDB views."""
    loaded = set()
    for pq_file in sorted(parquet_dir.glob("*.parquet")):
        table_name = pq_file.stem
        con.execute(f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{pq_file}')")
        loaded.add(table_name)
    return loaded


def test_tables_exist(loaded_tables: set[str], results: TestResults):
    """Test that all expected Parquet tables exist."""
    print(f"\n{'='*60}")
    print("TESTING PARQUET FILE EXISTENCE")
    print(f"{'='*60}")

    expected_tables = [
        "allergies", "careplans", "claims", "claims_transactions",
        "codes", "conditions", "devices", "encounters", "imaging_studies",
        "immunizations", "medications", "observations", "organizations",
        "patients", "payers", "payer_transitions", "procedures",
        "providers", "supplies", "symptoms"
    ]

    for table in expected_tables:
        if table in loaded_tables:
            results.add_pass(f"Parquet table '{table}' exists")
        else:
            results.add_fail(f"Parquet table '{table}' missing", f"{table}.parquet not found")

    if "notes" in loaded_tables:
        results.add_pass("Parquet table 'notes' exists (optional clinical notes enabled)")


def test_record_counts(con: duckdb.DuckDBPyConnection, loaded_tables: set[str], results: TestResults):
    """Test that core tables have records."""
    print(f"\n{'='*60}")
    print("TESTING RECORD COUNTS")
    print(f"{'='*60}")

    required_non_empty = [
        "patients", "encounters", "observations", "organizations",
        "providers", "payers", "codes"
    ]

    for table in sorted(loaded_tables):
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if table in required_non_empty and count == 0:
            results.add_fail(f"Table '{table}' is empty", "Expected at least 1 record")
        else:
            results.add_pass(f"Table '{table}' has {count:,} records")


def test_primary_keys(con: duckdb.DuckDBPyConnection, loaded_tables: set[str], results: TestResults):
    """Test primary key uniqueness and non-null constraints."""
    print(f"\n{'='*60}")
    print("TESTING PRIMARY KEY UNIQUENESS")
    print(f"{'='*60}")

    tables_with_id_pk = [
        "patients", "encounters", "organizations", "providers", "payers",
        "careplans", "claims", "claims_transactions", "imaging_studies"
    ]

    for table in tables_with_id_pk:
        if table not in loaded_tables:
            continue
        total, distinct_id, null_id = con.execute(f"""
            SELECT COUNT(*), COUNT(DISTINCT id), COUNT(*) FILTER (WHERE id IS NULL)
            FROM {table}
        """).fetchone()

        if null_id > 0:
            results.add_fail(f"PK '{table}.id' has NULLs", f"{null_id} NULL ids found")
        elif total != distinct_id:
            results.add_fail(f"PK '{table}.id' has duplicates", f"{total - distinct_id} duplicate ids found")
        else:
            results.add_pass(f"PK '{table}.id' is unique and non-null ({total:,} rows)")

    if "codes" in loaded_tables:
        total, distinct_pair = con.execute("""
            SELECT COUNT(*), COUNT(DISTINCT (vocab || ':' || code))
            FROM codes
        """).fetchone()
        if total == distinct_pair:
            results.add_pass(f"PK 'codes.(vocab, code)' is unique ({total:,} codes)")
        else:
            results.add_fail("PK 'codes.(vocab, code)' has duplicates", f"{total - distinct_pair} duplicates")


def test_synthea_4_features(con: duckdb.DuckDBPyConnection, loaded_tables: set[str], results: TestResults):
    """Test Synthea 4.0+ features and bug fixes."""
    print(f"\n{'='*60}")
    print("TESTING SYNTHEA 4.0+ ENHANCEMENTS")
    print(f"{'='*60}")

    # 1. NPI columns in organizations and providers
    for table in ["organizations", "providers"]:
        if table in loaded_tables:
            cols = [c[0] for c in con.execute(f"DESCRIBE {table}").fetchall()]
            if "npi" in cols:
                null_npis = con.execute(f"SELECT COUNT(*) FROM {table} WHERE npi IS NULL").fetchone()[0]
                if null_npis == 0:
                    results.add_pass(f"Synthea 4.0 '{table}.npi' column present and 100% populated")
                else:
                    results.add_fail(f"Synthea 4.0 '{table}.npi' has NULLs", f"{null_npis} rows missing NPI")
            else:
                results.add_fail(f"Synthea 4.0 '{table}.npi' column missing", "Expected NPI column from Synthea 4.0+")

    # 2. No 1970-01-01 unset stop dates
    tables_with_stop = ["allergies", "careplans", "conditions", "devices", "encounters", "medications", "procedures"]
    epoch_bugs = 0
    for table in tables_with_stop:
        if table in loaded_tables:
            cnt = con.execute(f"""
                SELECT COUNT(*) FROM {table}
                WHERE stop IS NOT NULL AND EXTRACT(YEAR FROM stop) <= 1970
            """).fetchone()[0]
            epoch_bugs += cnt
    if epoch_bugs == 0:
        results.add_pass("No unset 1970-01-01 epoch stop timestamps across clinical tables")
    else:
        results.add_fail("Found 1970-01-01 stop timestamps", f"{epoch_bugs} rows have 1970 stop dates")

    # 3. No orphan QALY/DALY/QOLS observations
    if "observations" in loaded_tables:
        qol_count = con.execute("""
            SELECT COUNT(*) FROM observations
            WHERE code IN ('QALY', 'DALY', 'QOLS') OR encounter IS NULL
        """).fetchone()[0]
        if qol_count == 0:
            results.add_pass("No orphan QALY/DALY/QOLS observations in observations.parquet")
        else:
            results.add_fail("Found QALY/DALY/QOLS or encounter-less observations", f"{qol_count} rows found")

    # 4. Unique patient SSNs
    if "patients" in loaded_tables:
        total_p, distinct_ssn = con.execute("SELECT COUNT(*), COUNT(DISTINCT ssn) FROM patients").fetchone()
        if total_p == distinct_ssn:
            results.add_pass(f"All patient SSNs are unique ({total_p:,}/{distinct_ssn:,})")
        else:
            results.add_warning("Duplicate patient SSNs found", f"{total_p - distinct_ssn} duplicates")

    # 5. MedStar DC organization routing (no VA Vet Center leakage)
    if "organizations" in loaded_tables:
        va_count = con.execute("""
            SELECT COUNT(*) FROM organizations
            WHERE LOWER(name) LIKE '%vet center%' OR LOWER(name) LIKE '%veterans%'
        """).fetchone()[0]
        if va_count == 0:
            results.add_pass("All organizations are MedStar/DC facilities (0 VA Vet Center leaks)")
        else:
            results.add_fail("Non-MedStar VA facility found in organizations", f"{va_count} VA organizations found")


def test_date_filtering(
    con: duckdb.DuckDBPyConnection,
    loaded_tables: set[str],
    ehr_launch_date: datetime,
    results: TestResults
):
    """Test that operational EHR tables are properly filtered by ehr_launch_date."""
    print(f"\n{'='*60}")
    print("TESTING EHR LAUNCH DATE FILTERING")
    print(f"{'='*60}")

    filtered_tables = {
        "encounters": "stop",
        "claims": "servicedate",
        "claims_transactions": "fromdate",
        "imaging_studies": "date",
        "observations": "date",
        "payer_transitions": "start_date",
        "procedures": "start",
        "supplies": "date",
    }
    if "notes" in loaded_tables:
        filtered_tables["notes"] = "date"

    launch_str = ehr_launch_date.strftime("%Y-%m-%d")

    for table, date_col in filtered_tables.items():
        if table not in loaded_tables:
            continue
        old_count = con.execute(f"""
            SELECT COUNT(*)
            FROM {table}
            WHERE {date_col} < '{launch_str}'
              AND {date_col} IS NOT NULL
        """).fetchone()[0]

        if old_count == 0:
            results.add_pass(f"Table '{table}.{date_col}' properly filtered (>= {launch_str})")
        else:
            results.add_fail(f"Table '{table}.{date_col}' has {old_count:,} records before {launch_str}",
                             "Date filtering failed")


def test_first_encounter_id(con: duckdb.DuckDBPyConnection, loaded_tables: set[str], results: TestResults):
    """Test that patients have first_encounter_id populated and valid."""
    print(f"\n{'='*60}")
    print("TESTING FIRST ENCOUNTER ID TRACKING")
    print(f"{'='*60}")

    if "patients" not in loaded_tables:
        return

    cols = [c[0] for c in con.execute("DESCRIBE patients").fetchall()]
    if "first_encounter_id" not in cols:
        results.add_fail("Column 'first_encounter_id' missing from patients", "Should be added in 3_convert_to_parquet.py")
        return

    results.add_pass("Column 'first_encounter_id' exists in patients")
    total, with_id = con.execute("SELECT COUNT(*), COUNT(first_encounter_id) FROM patients").fetchone()
    pct = (with_id / total * 100) if total > 0 else 0.0
    if pct >= 95.0:
        results.add_pass(f"First encounter ID populated: {with_id:,}/{total:,} ({pct:.1f}%)")
    elif pct >= 80.0:
        results.add_warning(f"First encounter ID populated: {with_id:,}/{total:,} ({pct:.1f}%)", "Expected >=95%")
    else:
        results.add_fail(f"First encounter ID populated: {with_id:,}/{total:,} ({pct:.1f}%)", "Too many patients without encounters")


def test_referential_integrity(con: duckdb.DuckDBPyConnection, loaded_tables: set[str], results: TestResults):
    """Test foreign key relationships across all tables (0 orphans)."""
    print(f"\n{'='*60}")
    print("TESTING REFERENTIAL INTEGRITY")
    print(f"{'='*60}")

    fk_checks = [
        ("encounters", "patient", "patients", "id"),
        ("encounters", "provider", "providers", "id"),
        ("encounters", "organization", "organizations", "id"),
        ("encounters", "payer", "payers", "id"),
        ("allergies", "patient", "patients", "id"),
        ("allergies", "encounter", "encounters", "id"),
        ("careplans", "patient", "patients", "id"),
        ("careplans", "encounter", "encounters", "id"),
        ("conditions", "patient", "patients", "id"),
        ("conditions", "encounter", "encounters", "id"),
        ("devices", "patient", "patients", "id"),
        ("devices", "encounter", "encounters", "id"),
        ("imaging_studies", "patient", "patients", "id"),
        ("imaging_studies", "encounter", "encounters", "id"),
        ("immunizations", "patient", "patients", "id"),
        ("immunizations", "encounter", "encounters", "id"),
        ("medications", "patient", "patients", "id"),
        ("medications", "encounter", "encounters", "id"),
        ("observations", "patient", "patients", "id"),
        ("observations", "encounter", "encounters", "id"),
        ("procedures", "patient", "patients", "id"),
        ("procedures", "encounter", "encounters", "id"),
        ("supplies", "patient", "patients", "id"),
        ("supplies", "encounter", "encounters", "id"),
        ("symptoms", "patient", "patients", "id"),
        ("notes", "patient", "patients", "id"),
        ("notes", "encounter", "encounters", "id"),
    ]

    for child_table, fk_col, parent_table, pk_col in fk_checks:
        if child_table not in loaded_tables or parent_table not in loaded_tables:
            continue
        orphans = con.execute(f"""
            SELECT COUNT(*)
            FROM {child_table} c
            LEFT JOIN {parent_table} p ON CAST(c.{fk_col} AS VARCHAR) = CAST(p.{pk_col} AS VARCHAR)
            WHERE c.{fk_col} IS NOT NULL AND p.{pk_col} IS NULL
        """).fetchone()[0]

        if orphans == 0:
            results.add_pass(f"FK {child_table}.{fk_col} -> {parent_table}.{pk_col} (0 orphans)")
        else:
            results.add_fail(f"FK {child_table}.{fk_col} -> {parent_table}.{pk_col}",
                             f"{orphans:,} orphan rows found")


def test_code_normalization(
    con: duckdb.DuckDBPyConnection,
    loaded_tables: set[str],
    keep_descriptions: bool,
    results: TestResults
):
    """Test that all clinical codes, reasoncodes, and reactions are in codes.parquet and descriptions are dropped."""
    print(f"\n{'='*60}")
    print("TESTING CODE DICTIONARY & DESCRIPTION NORMALIZATION")
    print(f"{'='*60}")

    if "codes" not in loaded_tables:
        results.add_fail("Table 'codes' missing", "Cannot validate code normalization")
        return

    code_mappings = [
        ("allergies", "code", "SNOMED"),
        ("allergies", "reaction1", "SNOMED"),
        ("allergies", "reaction2", "SNOMED"),
        ("careplans", "code", "SNOMED"),
        ("careplans", "reasoncode", "SNOMED"),
        ("conditions", "code", "SNOMED"),
        ("devices", "code", "SNOMED"),
        ("encounters", "code", "SNOMED"),
        ("encounters", "reasoncode", "SNOMED"),
        ("procedures", "code", "SNOMED"),
        ("procedures", "reasoncode", "SNOMED"),
        ("supplies", "code", "SNOMED"),
        ("imaging_studies", "bodysite_code", "SNOMED"),
        ("imaging_studies", "modality_code", "DICOM-DCM"),
        ("imaging_studies", "sop_code", "DICOM-SOP"),
        ("medications", "code", "RxNorm"),
        ("medications", "reasoncode", "SNOMED"),
        ("immunizations", "code", "CVX"),
        ("observations", "code", "LOINC"),
    ]

    for table, col, vocab in code_mappings:
        if table not in loaded_tables:
            continue
        missing = con.execute(f"""
            SELECT COUNT(DISTINCT t.{col})
            FROM {table} t
            LEFT JOIN codes c ON c.vocab = '{vocab}' AND c.code = CAST(t.{col} AS VARCHAR)
            WHERE t.{col} IS NOT NULL AND c.code IS NULL
        """).fetchone()[0]

        if missing == 0:
            results.add_pass(f"Code coverage: {table}.{col} ({vocab}) -> codes.parquet (100%)")
        else:
            results.add_fail(f"Code coverage: {table}.{col} ({vocab})",
                             f"{missing} distinct codes missing from codes.parquet")

    if not keep_descriptions:
        expected_dropped = {
            "allergies": ["description", "description1", "description2"],
            "careplans": ["description", "reasondescription"],
            "conditions": ["description"],
            "devices": ["description"],
            "encounters": ["description", "reasondescription"],
            "procedures": ["description", "reasondescription"],
            "supplies": ["description"],
            "imaging_studies": ["bodysite_description", "modality_description", "sop_description"],
            "medications": ["description", "reasondescription"],
            "immunizations": ["description"],
            "observations": ["description"],
        }
        for table, dropped_cols in expected_dropped.items():
            if table not in loaded_tables:
                continue
            actual_cols = {c[0] for c in con.execute(f"DESCRIBE {table}").fetchall()}
            leftover = [c for c in dropped_cols if c in actual_cols]
            if not leftover:
                results.add_pass(f"Redundant descriptions removed from '{table}' ({', '.join(dropped_cols)})")
            else:
                results.add_fail(f"Redundant descriptions still in '{table}'",
                                 f"Found columns: {', '.join(leftover)}")


def test_data_quality(con: duckdb.DuckDBPyConnection, loaded_tables: set[str], results: TestResults):
    """Test basic clinical data quality checks."""
    print(f"\n{'='*60}")
    print("TESTING DATA QUALITY")
    print(f"{'='*60}")

    if "patients" in loaded_tables:
        missing_names = con.execute("SELECT COUNT(*) FROM patients WHERE first IS NULL OR last IS NULL").fetchone()[0]
        if missing_names == 0:
            results.add_pass("All patients have first and last names")
        else:
            results.add_fail("Patients missing names", f"{missing_names} rows")

        missing_bday = con.execute("SELECT COUNT(*) FROM patients WHERE birthdate IS NULL").fetchone()[0]
        if missing_bday == 0:
            results.add_pass("All patients have birthdates")
        else:
            results.add_fail("Patients missing birthdates", f"{missing_bday} rows")

    if "encounters" in loaded_tables:
        missing_start = con.execute("SELECT COUNT(*) FROM encounters WHERE start IS NULL").fetchone()[0]
        if missing_start == 0:
            results.add_pass("All encounters have start dates")
        else:
            results.add_fail("Encounters missing start dates", f"{missing_start} rows")


def main(
    parquet_dir: str = "./output/parquet",
    ehr_launch_date: datetime = date_parser.parse("2010-01-01 00:00:00Z"),
    keep_descriptions: bool = False,
) -> int:
    parquet_path = Path(parquet_dir)
    print(f"\n{'='*60}")
    print("SYNTHEA PARQUET / DUCKDB VALIDATION TEST SUITE")
    print(f"{'='*60}")
    print(f"Parquet directory: {parquet_path.resolve()}")
    print(f"EHR Launch Date:   {ehr_launch_date.date()}")

    if not parquet_path.exists():
        print(f"\n✗ FATAL ERROR: Parquet directory not found: {parquet_path}")
        return 1

    con = duckdb.connect(":memory:")
    loaded_tables = load_parquet_views(con, parquet_path)

    results = TestResults()
    test_tables_exist(loaded_tables, results)
    test_record_counts(con, loaded_tables, results)
    test_primary_keys(con, loaded_tables, results)
    test_synthea_4_features(con, loaded_tables, results)
    test_date_filtering(con, loaded_tables, ehr_launch_date, results)
    test_first_encounter_id(con, loaded_tables, results)
    test_referential_integrity(con, loaded_tables, results)
    test_code_normalization(con, loaded_tables, keep_descriptions, results)
    test_data_quality(con, loaded_tables, results)

    con.close()
    success = results.summary()
    return 0 if success else 1


if __name__ == "__main__":
    parser = ArgumentParser(
        prog="test_parquet_load.py",
        description="Validate Synthea Parquet output integrity using DuckDB",
    )
    parser.add_argument(
        "--path",
        default="./output/parquet",
        help="Path to Parquet directory (default: ./output/parquet)",
    )
    parser.add_argument(
        "--start",
        default="2010-01-01 00:00:00Z",
        type=lambda s: date_parser.parse(s),
        help="EHR launch date used for filtering (default: 2010-01-01)",
    )
    parser.add_argument(
        "--keep_descriptions",
        action="store_true",
        default=False,
        help="Expect description columns to be retained in Parquet tables",
    )
    args = parser.parse_args()
    sys.exit(
        main(
            parquet_dir=args.path,
            ehr_launch_date=args.start,
            keep_descriptions=args.keep_descriptions,
        )
    )
