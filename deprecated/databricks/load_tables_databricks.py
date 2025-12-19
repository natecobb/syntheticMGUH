"""
Databricks script to load Parquet files from Unity Catalog Volume as managed tables

Run this in a Databricks notebook to create tables in mi2.i2l_syntheticmguh schema
"""

# Configuration
CATALOG = "mi2"
SCHEMA = "i2l_syntheticmguh"
VOLUME_PATH = "/Volumes/mi2/i2l_syntheticmguh/raw/parquet"

# List of all Synthea tables
TABLES = [
    "allergies",
    "careplans",
    "claims",
    "claims_transactions",
    "codes",
    "conditions",
    "devices",
    "encounters",
    "imaging_studies",
    "immunizations",
    "medications",
    "observations",
    "organizations",
    "patients",
    "payer_transitions",
    "payers",
    "procedures",
    "providers",
    "supplies"
]

# Create schema if it doesn't exist
print(f"Creating schema {CATALOG}.{SCHEMA} if not exists...")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Load each table
for table_name in TABLES:
    parquet_path = f"{VOLUME_PATH}/{table_name}.parquet"
    full_table_name = f"{CATALOG}.{SCHEMA}.{table_name}"

    print(f"Loading {table_name}...", end=" ")

    try:
        # Read Parquet file
        df = spark.read.parquet(parquet_path)

        # Write as managed table (overwrite if exists)
        df.write.mode("overwrite").saveAsTable(full_table_name)

        # Get row count
        count = spark.table(full_table_name).count()
        print(f"✓ ({count:,} rows)")

    except Exception as e:
        print(f"✗ Error: {e}")

print("\n" + "="*60)
print("Table creation complete!")
print("="*60)

# Display summary
print("\nVerifying tables:")
tables_df = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}")
display(tables_df)

# Show sample counts
print("\nTable row counts:")
for table_name in TABLES:
    try:
        count = spark.table(f"{CATALOG}.{SCHEMA}.{table_name}").count()
        print(f"  {table_name}: {count:,} rows")
    except:
        print(f"  {table_name}: NOT FOUND")