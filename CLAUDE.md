# CLAUDE.md - Developer & AI Assistant Guide

## Project Overview

Synthetic EHR dataset generation for MedStar Georgetown University Hospital (MGUH) and Washington, DC facilities using Synthea v4.0+. Exports 22 core linked, ZSTD-compressed Parquet tables plus optional transfer metadata, with a normalized code dictionary (`codes.parquet`) enriched with OHDSI Athena OMOP, ICD-10-CM, and HCPCS mappings.

- **Primary User Documentation**: [`readme.md`](readme.md)
- **Full Schema, Crosswalk & Query Reference**: [`docs/DATA_DICTIONARY.md`](docs/DATA_DICTIONARY.md)

## Repository Structure

```
syntheticMGUH/
├── 0_setup_synthea.sh                # Step 0: Install stock Synthea 4.0+ fat JAR into ./synthea/bin/
├── 1_build_providers.py              # Step 1 (optional): Rebuild DC/MedStar provider CSVs in ./synthea/providers/
├── 2_generate_data.sh                # Step 2a: Shell wrapper around 2_generate_incremental.py
├── 2_generate_incremental.py         # Step 2b: Iterative, crash-recoverable batch generator + Parquet loader
├── 3_convert_to_parquet.py           # Step 3: Load CSVs, build enriched code dictionary, export Parquet
├── 4_sync_to_databricks.sh           # Step 4: Sync Parquet files to Databricks Unity Catalog
├── docs/
│   └── DATA_DICTIONARY.md            # Complete schema reference, ER diagram, crosswalk guide & query cookbook
├── synthea/                          # Self-contained Synthea runtime, config, modules, and crosswalk
│   ├── synthea.properties            # Custom Synthea configuration
│   ├── athena_code_crosswalk.parquet # Static OHDSI Athena OMOP + ICD-10-CM + HCPCS crosswalk
│   ├── modules/                      # 15 custom specialty GMF v2 modules + 2 COVID-19 disable overrides
│   ├── providers/syntheticMGUH/      # Pre-filtered MedStar DC provider CSVs (committed)
│   └── bin/                          # Stock Synthea fat JAR & unpacked physiology configs (gitignored)
├── scripts/
│   ├── build_athena_crosswalk.py     # Rebuild synthea/athena_code_crosswalk.parquet from Athena vocab
│   ├── process_notes.py              # LLM clinical note rewriting with DuckDB vitals/exam/score enrichment
│   ├── templates/                    # 10 encounter-class templates + 10 clinician voices + style_mapping.json
│   └── test_parquet_load.py          # DuckDB/Parquet validation test suite (source and transfer checks)
├── output/                           # Generated CSV, notes, and Parquet output (gitignored)
├── pyproject.toml                    # Python project configuration (uv)
├── uv.lock                           # Python dependency lock file
├── readme.md                         # User documentation
├── CLAUDE.md                         # Developer/agent guide (this file)
├── deprecated/                       # Legacy PostgreSQL workflow files
└── OMOP/                             # OMOP CDM conversion scripts
```

## Key Architectural Invariants

1. **Stock Untouched Synthea JAR (`./synthea/bin/`)**:
   - `0_setup_synthea.sh` installs an unmodified `synthea-with-dependencies.jar` and unpacks `physiology/*` into `synthea/bin/resources/`.
   - Never mutate the JAR binary directly. All module additions and overrides (`covid19.json`, `COVID-19 Immunization Module.json`, and the 15 specialty modules) live in `synthea/modules/` and are loaded at runtime via `-d ./synthea/modules`.
   - Always include `synthea` first on the Java classpath (`-cp "synthea:synthea/bin/resources:synthea/bin/synthea-with-dependencies.jar"`) so `synthea/providers/syntheticMGUH/*.csv` resolves via `Utilities.readResource()` and `synthea/templates/notes/note.ftl` supplies precise encounter-start headers and acute onset markers. Conversion extracts the markers into `transfer_signals` and removes them from note prose; legacy date-only exports remain readable.
   - `synthea/modules/cirrhosis.json` calls the shared liver modules for decompensation. Regenerate `synthea/modules/liver/*.json` with `uv run python scripts/build_liver_modules.py` after editing its compiler/config; see `docs/CIRRHOSIS_PATHWAY.md` for ownership and calibration limits.

2. **Code Normalization & Athena Crosswalk (`3_convert_to_parquet.py`)**:
   - All text description columns are dropped from clinical and claims tables by default (unless `--keep_descriptions` is passed) and normalized into `codes.parquet` (`PRIMARY KEY (vocab, code)`).
   - `3_convert_to_parquet.py` automatically joins `synthea/athena_code_crosswalk.parquet` during both initial and `--append` runs to populate `omop_concept_id`, `omop_domain_id`, `icd10cm_code`, `icd10cm_description`, and `hcpcs_code`.
   - If new clinical modules are added to `synthea/modules/`, re-run `uv run python scripts/build_athena_crosswalk.py` to update `synthea/athena_code_crosswalk.parquet`.

3. **EHR Launch Date (`2010-01-01`) & Referential Integrity**:
   - Operational tables (`encounters`, `observations`, `procedures`, `imaging_studies`, `supplies`, `payer_transitions`, `claims`, `claims_transactions`, `notes`) are filtered to `>= 2010-01-01`.
   - Historical records (`conditions`, `allergies`, `careplans`, `devices`, `medications`, `immunizations`) from trimmed pre-2010 encounters are reassigned to `patients.first_encounter_id` so 0 foreign-key orphans exist across all 22 tables.

4. **Validation (`scripts/test_parquet_load.py`)**:
   - Always run `uv run python scripts/test_parquet_load.py` after modifying conversion or schema logic (all applicable checks must pass with `0` failures).

5. **Hospital transfers**: `scripts/process_transfers.py` publishes a separate atomic
   snapshot after conversion. Existing patient fingerprints and policy versions are
   immutable; added patients are processed once. See `docs/HOSPITAL_TRANSFERS.md` for
   recognition precision, financial allocation, note identities and validation.
