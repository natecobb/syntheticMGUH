#!/usr/bin/env python3
"""
Test suite for validating Synthea database load.
Verifies data integrity, foreign keys, date filtering, and table structure.
"""

import sys
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from datetime import datetime
from typing import Dict, Any, List


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

    def summary(self):
        total = len(self.passed) + len(self.failed)
        print(f"\n{'='*60}")
        print("TEST SUMMARY")
        print(f"{'='*60}")
        print(f"Passed:   {len(self.passed)}/{total}")
        print(f"Failed:   {len(self.failed)}/{total}")
        print(f"Warnings: {len(self.warnings)}")

        if self.failed:
            print(f"\nFailed tests:")
            for name, msg in self.failed:
                print(f"  - {name}: {msg}")

        return len(self.failed) == 0


def test_tables_exist(db_engine, results: TestResults):
    """Test that all expected tables exist."""
    expected_tables = [
        "allergies", "careplans", "claims", "claims_transactions",
        "conditions", "devices", "encounters", "imaging_studies",
        "immunizations", "medications", "observations", "organizations",
        "patients", "payers", "payer_transitions", "procedures",
        "providers", "supplies"
    ]

    print(f"\n{'='*60}")
    print("TESTING TABLE EXISTENCE")
    print(f"{'='*60}")

    inspector = inspect(db_engine)
    actual_tables = inspector.get_table_names()

    for table in expected_tables:
        if table in actual_tables:
            results.add_pass(f"Table '{table}' exists")
        else:
            results.add_fail(f"Table '{table}' missing", "Expected table not found")


def test_record_counts(db_engine, results: TestResults):
    """Test that tables have data and check minimum expected counts."""

    print(f"\n{'='*60}")
    print("TESTING RECORD COUNTS")
    print(f"{'='*60}")

    # Tables that should always have data
    required_data_tables = {
        "patients": 100,  # Minimum 100 patients
        "encounters": 1000,  # Should have many encounters
        "observations": 1000,  # Should have many observations
        "organizations": 1,  # At least one organization
        "providers": 1,  # At least one provider
        "payers": 1  # At least one payer
    }

    with db_engine.connect() as conn:
        for table, min_count in required_data_tables.items():
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = result.scalar()

            if count == 0:
                results.add_fail(f"Table '{table}' is empty", "Expected at least some records")
            elif count < min_count:
                results.add_warning(f"Table '{table}' has {count:,} records",
                                   f"Expected at least {min_count:,}")
            else:
                results.add_pass(f"Table '{table}' has {count:,} records")


def test_primary_keys(db_engine, results: TestResults):
    """Test that primary keys exist where expected."""

    print(f"\n{'='*60}")
    print("TESTING PRIMARY KEYS")
    print(f"{'='*60}")

    tables_with_pk = [
        "patients", "encounters", "organizations", "providers", "payers",
        "careplans", "claims", "claims_transactions", "imaging_studies"
    ]

    inspector = inspect(db_engine)

    for table in tables_with_pk:
        pk = inspector.get_pk_constraint(table)
        if pk and pk['constrained_columns']:
            results.add_pass(f"Table '{table}' has primary key on {pk['constrained_columns']}")
        else:
            results.add_fail(f"Table '{table}' missing primary key", "Expected 'id' as primary key")


def test_foreign_keys(db_engine, results: TestResults):
    """Test that foreign key constraints exist."""

    print(f"\n{'='*60}")
    print("TESTING FOREIGN KEY CONSTRAINTS")
    print(f"{'='*60}")

    # Sample of critical foreign keys
    critical_fks = [
        ("encounters", "patient", "patients"),
        ("encounters", "provider", "providers"),
        ("encounters", "organization", "organizations"),
        ("conditions", "patient", "patients"),
        ("conditions", "encounter", "encounters"),
        ("medications", "patient", "patients"),
        ("observations", "encounter", "encounters")
    ]

    inspector = inspect(db_engine)

    for table, fk_column, ref_table in critical_fks:
        fks = inspector.get_foreign_keys(table)
        found = False

        for fk in fks:
            if fk_column in fk['constrained_columns'] and fk['referred_table'] == ref_table:
                found = True
                break

        if found:
            results.add_pass(f"FK: {table}.{fk_column} -> {ref_table}")
        else:
            results.add_fail(f"FK: {table}.{fk_column} -> {ref_table}", "Foreign key missing")


def test_date_filtering(db_engine, results: TestResults, ehr_launch_date: datetime):
    """Test that date filtering worked correctly.

    Note: The load script filters by STOP date for conditions/medications,
    keeping historical records that are still active or stopped after EHR launch.
    """

    print(f"\n{'='*60}")
    print("TESTING DATE FILTERING")
    print(f"{'='*60}")

    # Tables that should be filtered - use the FIRST date field from load script
    # This matches the filtering logic in load_csv_to_rdbms.py
    filtered_tables = {
        "encounters": "stop",  # Filters by STOP (first field)
        "conditions": "stop",  # Filters by STOP (first field)
        "medications": "stop",  # Filters by STOP (first field)
        "observations": "date",  # Filters by DATE (only field)
        "procedures": "start",  # Filters by START (first field)
        "immunizations": "date"  # Filters by DATE (only field)
    }

    with db_engine.connect() as conn:
        for table, date_col in filtered_tables.items():
            # Check for records before EHR launch date (should be null or after)
            query = text(f"""
                SELECT COUNT(*)
                FROM {table}
                WHERE {date_col} < :launch_date
                  AND {date_col} IS NOT NULL
            """)
            result = conn.execute(query, {"launch_date": ehr_launch_date})
            old_count = result.scalar()

            if old_count > 0:
                results.add_fail(f"Table '{table}' has {old_count} records before {ehr_launch_date.date()}",
                               "Date filtering may not have worked")
            else:
                results.add_pass(f"Table '{table}' properly filtered (no records before {ehr_launch_date.date()})")


def test_first_encounter_id(db_engine, results: TestResults):
    """Test that patients have first_encounter_id populated."""

    print(f"\n{'='*60}")
    print("TESTING FIRST ENCOUNTER ID TRACKING")
    print(f"{'='*60}")

    with db_engine.connect() as conn:
        # Check that first_encounter_id column exists
        result = conn.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'patients'
              AND column_name = 'first_encounter_id'
        """))

        if result.fetchone():
            results.add_pass("Column 'first_encounter_id' exists in patients table")

            # Check that most patients have it populated
            result = conn.execute(text("""
                SELECT
                    COUNT(*) as total,
                    COUNT(first_encounter_id) as with_id
                FROM patients
            """))
            row = result.fetchone()
            total, with_id = row.total, row.with_id

            pct = (with_id / total * 100) if total > 0 else 0

            if pct > 95:
                results.add_pass(f"First encounter ID populated: {with_id}/{total} ({pct:.1f}%)")
            elif pct > 80:
                results.add_warning(f"First encounter ID populated: {with_id}/{total} ({pct:.1f}%)",
                                  "Should be >95%")
            else:
                results.add_fail(f"First encounter ID populated: {with_id}/{total} ({pct:.1f}%)",
                               "Most patients should have first_encounter_id")
        else:
            results.add_fail("Column 'first_encounter_id' missing from patients table",
                           "This column should be added during load")


def test_referential_integrity(db_engine, results: TestResults):
    """Test that foreign key relationships are valid (no orphans)."""

    print(f"\n{'='*60}")
    print("TESTING REFERENTIAL INTEGRITY")
    print(f"{'='*60}")

    with db_engine.connect() as conn:
        # Test: All encounters have valid patient IDs
        result = conn.execute(text("""
            SELECT COUNT(*)
            FROM encounters e
            LEFT JOIN patients p ON e.patient = p.id
            WHERE p.id IS NULL
        """))
        orphan_count = result.scalar()

        if orphan_count == 0:
            results.add_pass("All encounters have valid patient references")
        else:
            results.add_fail("Orphan encounters found", f"{orphan_count} encounters without valid patients")

        # Test: All conditions have valid encounter IDs
        result = conn.execute(text("""
            SELECT COUNT(*)
            FROM conditions c
            LEFT JOIN encounters e ON c.encounter = e.id
            WHERE e.id IS NULL AND c.encounter IS NOT NULL
        """))
        orphan_count = result.scalar()

        if orphan_count == 0:
            results.add_pass("All conditions have valid encounter references")
        else:
            results.add_fail("Orphan conditions found", f"{orphan_count} conditions without valid encounters")

        # Test: All observations have valid encounters
        result = conn.execute(text("""
            SELECT COUNT(*)
            FROM observations o
            LEFT JOIN encounters e ON o.encounter = e.id
            WHERE e.id IS NULL AND o.encounter IS NOT NULL
        """))
        orphan_count = result.scalar()

        if orphan_count == 0:
            results.add_pass("All observations have valid encounter references")
        else:
            results.add_fail("Orphan observations found", f"{orphan_count} observations without valid encounters")


def test_data_quality(db_engine, results: TestResults):
    """Test basic data quality checks."""

    print(f"\n{'='*60}")
    print("TESTING DATA QUALITY")
    print(f"{'='*60}")

    with db_engine.connect() as conn:
        # Test: Patients have names
        result = conn.execute(text("""
            SELECT COUNT(*)
            FROM patients
            WHERE first IS NULL OR last IS NULL
        """))
        missing_names = result.scalar()

        if missing_names == 0:
            results.add_pass("All patients have first and last names")
        else:
            results.add_fail("Patients with missing names", f"{missing_names} patients missing names")

        # Test: Patients have birth dates
        result = conn.execute(text("""
            SELECT COUNT(*)
            FROM patients
            WHERE birthdate IS NULL
        """))
        missing_birthdate = result.scalar()

        if missing_birthdate == 0:
            results.add_pass("All patients have birthdates")
        else:
            results.add_fail("Patients with missing birthdates", f"{missing_birthdate} patients missing birthdates")

        # Test: Encounters have dates
        result = conn.execute(text("""
            SELECT COUNT(*)
            FROM encounters
            WHERE start IS NULL
        """))
        missing_dates = result.scalar()

        if missing_dates == 0:
            results.add_pass("All encounters have start dates")
        else:
            results.add_fail("Encounters with missing dates", f"{missing_dates} encounters missing start dates")


def test_indexes(db_engine, results: TestResults):
    """Test that indexes exist on key columns."""

    print(f"\n{'='*60}")
    print("TESTING INDEXES")
    print(f"{'='*60}")

    inspector = inspect(db_engine)

    # Key indexes to check
    critical_indexes = [
        ("encounters", "patient"),
        ("encounters", "start"),
        ("conditions", "patient"),
        ("medications", "patient"),
        ("observations", "patient"),
        ("observations", "date")
    ]

    for table, column in critical_indexes:
        indexes = inspector.get_indexes(table)
        found = False

        for idx in indexes:
            if column in idx['column_names']:
                found = True
                break

        if found:
            results.add_pass(f"Index on {table}.{column}")
        else:
            results.add_warning(f"Missing index on {table}.{column}",
                              "Performance may be impacted")


def main(uid='postgres', pwd='', host='localhost', db='syntheticmguh',
         ehr_launch_date=datetime.strptime('2010-01-01', '%Y-%m-%d')):

    print(f"\n{'='*60}")
    print("SYNTHEA DATABASE VALIDATION TEST SUITE")
    print(f"{'='*60}")
    print(f"Database: {db}@{host}")
    print(f"User: {uid}")
    print(f"EHR Launch Date: {ehr_launch_date.date()}")

    # Connect to database
    db_connection_string = f'postgresql+psycopg2://{uid}:{pwd}@{host}/{db}'

    try:
        db_engine = create_engine(db_connection_string)

        # Test connection
        with db_engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.scalar()
            print(f"PostgreSQL: {version.split(',')[0]}")

        results = TestResults()

        # Run all tests
        test_tables_exist(db_engine, results)
        test_record_counts(db_engine, results)
        test_primary_keys(db_engine, results)
        test_foreign_keys(db_engine, results)
        test_date_filtering(db_engine, results, ehr_launch_date)
        test_first_encounter_id(db_engine, results)
        test_referential_integrity(db_engine, results)
        test_data_quality(db_engine, results)
        test_indexes(db_engine, results)

        # Print summary
        success = results.summary()

        return 0 if success else 1

    except Exception as e:
        print(f"\n✗ FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    from argparse import ArgumentParser
    from dateutil import parser as date_parser

    parser = ArgumentParser(
        prog='test_database_load.py',
        description='Validate Synthea database load integrity'
    )
    parser.add_argument('--uid', default='postgres', help='Database user')
    parser.add_argument('--pwd', default='', help='Database password')
    parser.add_argument('--host', default='localhost', help='Database host')
    parser.add_argument('--db', default='syntheticmguh', help='Database name')
    parser.add_argument('--start', default='2010-01-01 00:00:00Z',
                       type=lambda s: date_parser.parse(s),
                       help='EHR launch date used for filtering')

    args = parser.parse_args()

    exit_code = main(
        uid=args.uid,
        pwd=args.pwd,
        host=args.host,
        db=args.db,
        ehr_launch_date=args.start
    )

    sys.exit(exit_code)
