# CLAUDE.md - AI Assistant Guide

## Project Overview

Synthetic EHR data generation for MedStar Georgetown University Hospital using Synthea v4.0+. Outputs DC-specific patient populations with linked Parquet tables for Databricks analytics.

**Key Facts:**
- ~60MB per 1,000 patients in ZSTD-compressed Parquet (~587 MB for 10,000 living / 13,981 total patients)
- 13+ years historical data (EHR launch: 2010-01-01)
- DuckDB → Parquet workflow (no database server)
- Branch: 2026 | Main: master

## Repository Structure

```
syntheticMGUH/
├── 0_setup_synthea.sh                # Step 0: Install stock Synthea 4.0+ fat JAR into ./synthea/bin/
├── 1_build_providers.py              # Step 1 (optional): Rebuild DC/MedStar provider CSVs in ./synthea/providers/
├── 2_generate_data.sh                # Step 2a: Shell wrapper around 2_generate_incremental.py
├── 2_generate_incremental.py         # Step 2b: Iterative, crash-recoverable batch generator + Parquet loader
├── 3_convert_to_parquet.py           # Step 3: Load CSVs, build enriched code dictionary, export Parquet
├── 4_sync_to_databricks.sh           # Step 4: Sync Parquet files to Databricks
├── synthea/                          # Self-contained Synthea runtime, config, modules, and crosswalk
│   ├── synthea.properties            # Custom Synthea configuration
│   ├── athena_code_crosswalk.parquet # Static OHDSI Athena OMOP + ICD-10-CM + HCPCS crosswalk
│   ├── modules/                      # 15 custom specialty GMF v2 modules + 2 COVID-19 disable overrides
│   ├── providers/syntheticMGUH/      # Pre-filtered MedStar DC provider CSVs (committed)
│   └── bin/                          # Stock Synthea fat JAR & unpacked physiology configs (gitignored)
├── scripts/                          # Utility scripts
│   ├── build_athena_crosswalk.py     # Rebuild synthea/athena_code_crosswalk.parquet from Athena vocab
│   ├── process_notes.py              # LLM clinical note rewriting with DuckDB vitals/exam/score enrichment
│   ├── templates/                    # 10 encounter-class templates + 10 clinician voices + style_mapping.json
│   └── test_parquet_load.py          # DuckDB/Parquet validation test suite (145 checks)
├── output/                           # Generated CSV, notes, and Parquet output (gitignored)
├── pyproject.toml                    # Python project configuration (uv)
├── uv.lock                           # Python dependency lock file
├── readme.md                         # User documentation
├── CLAUDE.md                         # AI assistant guide (this file)
├── deprecated/                       # Legacy PostgreSQL workflow files
└── OMOP/                             # OMOP CDM conversion scripts
```

## Key Files and Components

### Python & Shell Scripts

#### `0_setup_synthea.sh`
Installs a stock untouched `synthea/bin/synthea-with-dependencies.jar` (from `../synthea-4.0.0` or cloned from `https://github.com/synthetichealth/synthea.git`) and unpacks `physiology/*` into `synthea/bin/resources/physiology/`. No JAR binary patching is required; all custom modules and overrides live in `synthea/modules/` and are loaded via `-d ./synthea/modules`.

#### `1_build_providers.py`
Filters Synthea provider files to create DC/MedStar-specific versions in `synthea/providers/syntheticMGUH/` (already committed for `MGUH` by default; re-run only when switching `--hospitals MGUH|WHC|both`).

#### `2_generate_incremental.py` & `2_generate_data.sh`
Iterative, crash-recoverable pipeline that generates, converts to Parquet, and deletes raw human-readable files every `X` patients:
- Runs stock Synthea with `-cp "synthea:synthea/bin/resources:synthea/bin/synthea-with-dependencies.jar" App ... -c ./synthea/synthea.properties -d ./synthea/modules`
- Keeps clinician seed (`-cs`) and reference date (`-r`) fixed across batches so all batches share identical organizations and clinicians (`providers.parquet`)
- Uses atomic `.tmp_*.parquet` replacement and saves batch progress in `output/seed_<seed>/parquet/_checkpoint.json`
- Automatically resumes from the last completed batch if interrupted (or if `--total` is increased)

**Usage:**
```bash
uv run python 2_generate_incremental.py --total 5000 --batch_size 250 --seed 12345 --years 14
# Or via 2_generate_data.sh:
./2_generate_data.sh 5000 12345 14 250
```

#### `3_convert_to_parquet.py`
Main data processing script that:
- Loads Synthea CSV output (plus `output/symptoms/csv/symptoms.csv`) using DuckDB
- Performs data transformations (encounter fixes, EHR launch date filtering `2010-01-01`, normalizing `symptoms.pathology` to SNOMED code)
- **Builds normalized code dictionary (`codes.parquet`)** and enriches it via `synthea/athena_code_crosswalk.parquet` with `omop_concept_id`, `omop_domain_id`, `icd10cm_code`, `icd10cm_description`, and `hcpcs_code`
- **Drops redundant description columns** and exports ZSTD-compressed Parquet files sorted by patient/date

#### `scripts/process_notes.py`
Incrementally rewrites structured Synthea clinical notes (`notes.parquet`) into natural physician notes (`notes_llm.parquet`) via an OpenAI-compatible `/v1/chat/completions` API (**oMLX** locally on Apple Silicon or **vLLM** on a remote NVIDIA GPU):
- Enriches prompts via DuckDB with encounter vitals (`vital-signs`), physical exam findings (`exam`), screening questionnaire total scores (`survey`), and encounter reason diagnoses (`reasoncode`)
- Maps `encounters.encounterclass` to 10 structural templates (`template_*.txt`) and `encounters.provider` to 10 clinician voices (`voice_*.txt`), applying post-generation `apply_hurried_typos` for the `hurried_typos` voice
- Processes patients in 100-patient cohorts sorted by `(cohort, provider_id, encounterclass)` for KV prefix-cache reuse

#### `scripts/test_parquet_load.py`
Automated validation test suite (`145/145` checks) for `./output/parquet/*.parquet`:
- Verifies all Parquet tables exist and are populated
- Checks primary key uniqueness (`id`, `vocab + code`) and foreign key referential integrity across all 21 tables
- Verifies Synthea 4.0+ enhancements, code normalization, and OHDSI Athena OMOP / ICD-10-CM / HCPCS crosswalk coverage (`100%` `omop_domain_id`, `>95%` `omop_concept_id`, `100%` `icd10cm_code` on `Condition` diagnoses)
