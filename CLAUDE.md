# CLAUDE.md - AI Assistant Guide

## Project Overview

Synthetic EHR data generation for MedStar Georgetown University Hospital using Synthea v3.4.0. Outputs DC-specific patient populations with 17 linked Parquet tables for Databricks analytics.

**Key Facts:**
- ~1GB per 1,000 patients
- 13 years historical data (EHR launch: 2010-01-01)
- DuckDB → Parquet workflow (no database server)
- Branch: 2023 | Main: master

## Repository Structure

```
syntheticMGUH/
├── 1_build_providers.py              # Step 1: Create DC/MedStar provider files
├── 2_generate_data.bat               # Step 2: Run Synthea to generate synthetic data
├── 3_convert_to_parquet.py           # Step 3: Load CSVs, build code dictionary, export Parquet
├── 4_sync_to_databricks.sh           # Step 4: Sync Parquet files to Databricks
├── synthea.properties                # Custom Synthea configuration
├── pyproject.toml                    # Python project configuration (uv)
├── uv.lock                           # Python dependency lock file
├── README.md                         # User documentation
├── CLAUDE.md                         # AI assistant guide (this file)
├── databricks/                       # Databricks-specific scripts
│   ├── load_tables_databricks.py     # Create managed tables (run in Databricks)
│   └── load_tables_databricks.sql    # SQL version of table creation
├── scripts/                          # Utility scripts
│   ├── generate_samples.sh           # Generate sample datasets
│   ├── process_notes.py              # Optional LLM note transformation
│   └── test_parquet_load.py          # Test suite for data validation
├── deprecated/                       # Legacy PostgreSQL workflow files
├── examples/                         # Example scripts and notebooks
└── OMOP/                            # OMOP CDM conversion scripts
```

## Key Files and Components

### Python Scripts

#### `1_build_providers.py`
Filters Synthea provider files to create DC/MedStar-specific versions:
- Filters hospitals to MGUH (and optionally MWHC)
- Adds MedStar PromptCare urgent care locations
- Configures specialized facilities (hospice, dialysis, rehab, home health)
- Uses NPI (National Provider Identifier) filtering

**Usage:**
```bash
uv run python 1_build_providers.py --hospitals MGUH  # or 'both', 'WHC'
```

**Key NPIs:**
- MGUH: 1427145176
- MWHC: 1548378235
- National Rehab: 1326496456
- Bridgepoint (long-term): 1285772772
- MedStar VNA (home health): 1407960248

#### `3_convert_to_parquet.py`
Main data processing script that:
- Loads Synthea CSV output using DuckDB
- Performs data transformations (encounter fixes, date filtering)
- **Builds normalized code dictionary** (codes.parquet)
- **Drops redundant description columns** (saves 30-50% space)
- Exports all tables to Parquet format with ZSTD compression
- Optionally processes clinical notes
- Supports EHR launch date filtering (default: 2010-01-01)

**Key Features:**
- 10x faster than PostgreSQL workflow
- No database server required
- Parquet files are ~10x smaller than CSV
- Cloud-ready (Databricks, Spark compatible)
- Smart type inference and date parsing
- Built-in code dictionary generation
- Automatic space optimization

**Usage:**
```bash
# Basic usage (drops descriptions, builds code dictionary)
uv run python 3_convert_to_parquet.py

# With clinical notes
uv run python 3_convert_to_parquet.py --include_notes

# Keep description columns (for backwards compatibility)
uv run python 3_convert_to_parquet.py --keep_descriptions

# Custom paths
uv run python 3_convert_to_parquet.py --path ../output --output ../output/parquet
```

**Parameters:**
- `--path`: Synthea output directory (default: `../output`)
- `--output`: Parquet output directory (default: `../output/parquet`)
- `--start`: EHR launch date (default: `2010-01-01`)
- `--include_notes`: Process clinical notes
- `--keep_descriptions`: Keep description columns (default: False to save space)

**Output:**
- All Synthea tables as Parquet files (without descriptions by default)
- `codes.parquet` - Normalized code dictionary for lookups

#### `4_sync_to_databricks.sh`
Syncs Parquet files to Databricks Unity Catalog:
- Compresses CSV and notes with gzip
- Copies Parquet files as-is (already compressed)
- Parallel compression for performance
- Shows compression statistics

**Usage:**
```bash
./4_sync_to_databricks.sh
```

### Configuration

**synthea.properties** - Custom Synthea config:
- 13 years history, CSV-only output, clinical notes enabled
- Individual mandate year: 2014 (DC-specific)

**pyproject.toml** - Python 3.12+, uv package manager
- Dependencies: duckdb, pandas, python-dateutil, requests

## Data Pipeline

### Standard Workflow

The pipeline consists of 4 numbered steps that run in sequence:

**Step 1: Provider File Generation**
```bash
uv run python 1_build_providers.py --hospitals MGUH
```
Creates filtered provider files in `../src/main/resources/providers/syntheticMGUH/`

**Step 2: Synthetic Data Generation**
```bash
./2_generate_data.bat 500 12345 14
```
Generates 500 patients with seed 12345, 14 years history
Output: CSV files in `../output/csv/`

**Step 3: Convert to Parquet & Build Code Dictionary**
```bash
uv run python 3_convert_to_parquet.py --include_notes
```
Loads CSVs into DuckDB, builds code dictionary, drops redundant descriptions, exports to Parquet
Output: Parquet files in `../output/parquet/` (includes codes.parquet)

**Step 4: Sync to Databricks**
```bash
./4_sync_to_databricks.sh
```
Uploads Parquet files to Databricks Unity Catalog Volume

## Data Schema

### Main Synthea Tables (Parquet Format)

**Patient Demographics:**
- `patients` - Core patient data (birthdate, deathdate, demographics)
- `payers` - Insurance payers
- `payer_transitions` - Insurance change history
- `organizations` - Healthcare organizations
- `providers` - Healthcare providers

**Clinical Data:**
- `encounters` - Patient visits (with start/stop times)
- `conditions` - Diagnoses (SNOMED codes)
- `procedures` - Procedures performed (SNOMED codes)
- `medications` - Medication orders (RxNorm codes)
- `immunizations` - Vaccinations (CVX codes)
- `observations` - Labs, vitals (LOINC codes)
- `allergies` - Allergy records
- `careplans` - Care planning
- `devices` - Medical devices
- `imaging_studies` - Radiology studies (DICOM codes)
- `supplies` - Medical supplies
- `notes` - Clinical notes (if enabled)

**Financial:**
- `claims` - Insurance claims
- `claims_transactions` - Claim transactions

**Derived Tables:**
- `codes.parquet` - Normalized code dictionary (created by normalize_codes_parquet.py)

### Key Identifiers

All tables use UUID strings for primary/foreign keys:
- `id` - Primary key (UUID as VARCHAR)
- `patient` - Patient foreign key
- `encounter` - Encounter foreign key
- `provider` - Provider foreign key
- `organization` - Organization foreign key
- `payer` - Payer foreign key

### File Format Benefits

**Parquet advantages:**
- Columnar storage for fast analytics
- Built-in compression (ZSTD) - ~10x smaller than CSV
- Native support in Spark, DuckDB, Pandas, R
- Schema included in file (no separate DDL needed)
- Predicate pushdown for fast filtering
- No database server required

## Dependencies

### System Requirements
- Java 11+ (LTS versions 11 or 17 recommended) - for Synthea
- Python 3.12+
- uv package manager
- Databricks CLI (for sync)

### Python Dependencies
```
duckdb >= 1.1.0
pandas >= 2.3.3
python-dateutil >= 2.9.0.post0
requests >= 2.32.5
```

### Installation
```bash
# Python dependencies
uv sync

# Databricks CLI (optional, for sync)
pip install databricks-cli
# or
brew install databricks
```

## Common Tasks

### Generate Small Test Dataset
```bash
uv run python 1_build_providers.py
./2_generate_data.bat 100 42 5
uv run python 3_convert_to_parquet.py
```

### Generate Full Dataset with Notes
```bash
uv run python 1_build_providers.py --hospitals both
./2_generate_data.bat 5000 12345 13
uv run python 3_convert_to_parquet.py --include_notes
```

### Keep Description Columns (Backwards Compatible)
```bash
uv run python 3_convert_to_parquet.py --keep_descriptions
```

### Query Data (Python + DuckDB)
```python
import duckdb

con = duckdb.connect()

# Patient count
con.execute("SELECT COUNT(*) FROM '../output/parquet/patients.parquet'").fetchone()

# Encounters by type
con.execute("""
    SELECT encounterclass, COUNT(*) as count
    FROM '../output/parquet/encounters.parquet'
    GROUP BY encounterclass
    ORDER BY count DESC
""").df()

# Most common conditions
con.execute("""
    SELECT c.description, COUNT(*) as freq
    FROM '../output/parquet/conditions.parquet' co
    JOIN '../output/parquet/codes.parquet' c
      ON CAST(co.code AS VARCHAR) = c.code AND c.vocab = 'SNOMED'
    GROUP BY c.description
    ORDER BY freq DESC
    LIMIT 10
""").df()
```

### Sync to Databricks
```bash
./4_sync_to_databricks.sh
```

Then query in Databricks:
```python
patients = spark.read.parquet("dbfs:/Volumes/mi2/i2l_syntheticmguh/raw/parquet/patients.parquet")
encounters = spark.read.parquet("dbfs:/Volumes/mi2/i2l_syntheticmguh/raw/parquet/encounters.parquet")
```

## Implementation Notes

**Key Paths:**
- Run scripts from: `syntheticMGUH/` directory
- Synthea CSV: `../output/csv/`
- Parquet output: `../output/parquet/`
- Provider files: `../src/main/resources/providers/syntheticMGUH/`

**Technical Details:**
- DuckDB supports PostgreSQL extensions (DISTINCT ON, etc.)
- Use `uv` package manager, not pip
- CSV format is fragile - DuckDB handles with `ignore_errors=true`
- Description columns dropped by default - join with `codes.parquet` for lookups
- Files with `.deprecated` extension are legacy PostgreSQL workflows

**Common Requests:**
- Generate data: Run 4-step pipeline
- Change hospital: `1_build_providers.py --hospitals MGUH|WHC|both`
- Add patients: Modify first arg to `2_generate_data.bat`
- Keep descriptions: Use `--keep_descriptions` on step 3 (30-50% larger files)
