# Synthetic MGUH

This repository contains scripts to build a synthetic MedStar Georgetown University Hospital (MGUH) EHR dataset using [Synthea](https://github.com/synthetichealth/synthea) v4.0+. The dataset consists of linked clinical, demographic, provider, and financial tables exported as ZSTD-compressed Parquet files (with an automatically generated `codes.parquet` lookup dictionary) ready for DuckDB or Databricks Unity Catalog.

## Overview & Revisions

- **2026 Revision (Synthea 4.0+ & Standalone Layout)**:
  - **Standalone Repository**: `syntheticMGUH` no longer needs to be nested inside a Synthea source checkout. `./0_setup_synthea.sh` builds/installs `bin/synthea-with-dependencies.jar` (and unpacks `bin/resources/physiology/`), and `1_build_providers.py` extracts base provider CSVs directly from the JAR into `./providers/syntheticMGUH/`.
  - **Synthea 4.0+ (`master`, Gradle 9, Java 17+)**: Includes `NPI` columns in `organizations` and `providers`, fixes `1970-01-01` unset stop timestamps in CSV exports, fixes SSN/Passport/Driver's License uniqueness collisions at population scale, and disables orphan QALY/DALY/QOLS rows via `exporter.csv.include_quality_of_life_observations = false`.
  - Restores NPI-based DC/MedStar provider filtering (`1_build_providers.py`) and parameterized history generation (`2_generate_data.bat`) alongside the DuckDB/Parquet conversion (`3_convert_to_parquet.py`) and Databricks sync (`4_sync_to_databricks.sh`) workflow.
- **2025 Revision**:
  - Replaced legacy PostgreSQL workflow with a 10x faster serverless DuckDB → Parquet pipeline and Databricks Unity Catalog sync.
  - Automatically builds `codes.parquet` and drops redundant description columns from clinical tables (saving 30–50% storage).
- **2023 Revision**:
  - Trims encounter/operational data prior to the simulated EHR launch date (`2010-01-01`) while retaining historical records (allergies, conditions, etc.) linked to the patient's first EHR encounter.
  - NPI-based provider filtering including MGUH (`1427145176`), MWHC (`1548378235`), National Rehabilitation Hospital (`1326496456`), Bridgepoint (`1285772772` / `1205923604`), MedStar Health VNA (`1407960248`), and MedStar PromptCare urgent care locations.

## Installation

Requires **Java 17+** and **uv** (Python 3.13+):

```bash
git clone https://github.com/natecobb/syntheticMGUH
cd syntheticMGUH
uv sync
./0_setup_synthea.sh
```

## 4-Step Build Pipeline

Run all steps from within the `syntheticMGUH/` directory:

1. **Build DC/MedStar provider files**:
   ```bash
   uv run python 1_build_providers.py --hospitals MGUH  # or 'both', 'WHC'
   ```
2. **Generate synthetic population CSVs with Synthea**:
   - **Iterative, crash-recoverable batch mode (recommended for large populations)**:
     Generates, merges into Parquet, checkpoints (`output/seed_<seed>/parquet/_checkpoint.json`), and deletes raw CSV/note files every `X` patients:
     ```bash
     uv run python 2_generate_incremental.py --total 5000 --batch_size 250 --seed 12345 --years 14
     # Optional: pass --no_claims to skip claims/claims_transactions (~78% smaller Parquet)
     # Or pass batch_size as 4th argument to 2_generate_data.bat:
     ./2_generate_data.bat 5000 12345 14 250
     ```
   - **Single-shot mode**:
     ```bash
     ./2_generate_data.bat 500 12345 14
     ```
     *(Arguments: `<num_patients>` `<seed>` `<years_of_history>` `[batch_size]`)*
3. **Convert CSVs to Parquet & build code dictionary** *(automatic in batch mode)*:
   ```bash
   uv run python 3_convert_to_parquet.py --include_notes --cleanup
   ```
   *(Add `--cleanup` to delete raw `output/csv` and `output/notes` after conversion; add `--keep_descriptions` to retain text description columns alongside `codes.parquet`.)*
4. **Validate Parquet output (optional)**:
   ```bash
   uv run python scripts/test_parquet_load.py
   ```
5. **Rewrite Clinical Notes with an LLM (optional)**:
   `scripts/process_notes.py` rewrites structured Synthea notes (`notes.parquet`) into natural physician notes (`notes_llm.parquet`) against any OpenAI-compatible `/v1` endpoint (e.g., local **oMLX** or remote **vLLM**). By default, it maps `encounters.encounterclass` to 10 structural note templates (`template_*.txt`) and `encounters.provider` to 10 clinician writing voices (`voice_*.txt`) via `scripts/templates/style_mapping.json`, sorting by `(provider, encounterclass)` for KV prefix-cache reuse:
   ```bash
   uv run python scripts/process_notes.py transform \
     --base_url http://localhost:8081/v1 \
     --model Qwen3-4B-Instruct-2507-4bit \
     --concurrency 8
   ```
   **Recommended Models & Throughput Benchmarks** *(~62,100 post-2010 notes per 1,000 patients)*:
   | Hardware / Backend | Recommended Model Type | Concurrency | Throughput | Est. Time / 1,000 Patients |
   |---|---|---:|---:|---:|
   | **Apple M4 Max (128GB, oMLX)** | `4B` Dense 4-bit (`Qwen3-4B-Instruct-2507-4bit`) | `8` | `1.41 notes/s` (`200–276 out tok/s`, `806 tot tok/s`) | ~12.2 hrs |
   | **Apple M4 Max (128GB, oMLX)** | `35B-A3B` MoE (`Qwen3.6-35B-A3B-oQ6-mtp`) | `8` | `0.80 notes/s` (`137 out tok/s`, `608 tot tok/s`) | ~21.5 hrs |
   | **NVIDIA RTX 5080 (16GB, vLLM)** | `4B` Dense FP8 (`Qwen3-4B-Instruct-2507-FP8`) | `32–64` | `~13–17 notes/s` (`~2,400+ out tok/s`) | ~60–80 mins |
   | **NVIDIA RTX 5080 (16GB, vLLM)** | `7B–8B` 4-bit / FP4 (`Qwen2.5-7B-AWQ` / `Llama-3.1-8B-FP4`) | `32` | `~9–12 notes/s` (`~1,700 out tok/s`) | ~1.4–1.9 hrs |
6. **Sync Parquet files to Databricks Unity Catalog (optional)**:
   ```bash
   ./4_sync_to_databricks.sh
   ```

## Stock Synthea Modifications

### Provider Files (`1_build_providers.py`)
- Filters hospitals by NPI to MGUH (`1427145176`) and optionally MWHC (`1548378235`)
- Includes MedStar DC primary care clinics (`1417571530` One MedStar Primary Care, `1285636522` MedStar Georgetown Medical Center, `1922395581` MMG Georgetown Family Practice, `1194374710` GUH-Kids Mobile Medical Clinic)
- Configures Bridgepoint for long-term/SNF, MGUH for inpatient hospice, MGUH/MWHC for dialysis, National Rehab for rehabilitation, and MedStar VNA for home health
- Routes veteran care to MedStar hospitals via an empty `va_facilities.csv` override (`generate.providers.default_to_hospital_on_failure = true`)
- Adds MedStar PromptCare urgent care locations (Adams Morgan and Capitol Hill)

### Properties File (`synthea.properties`)
- CSV-only export (`exporter.fhir.export = false`, `exporter.csv.export = true`)
- Excludes quality-of-life orphan observations (`exporter.csv.include_quality_of_life_observations = false`)
- Sets ACA individual mandate year to `2014` (`generate.insurance.mandate.year = 2014`)
- Exports clinical notes (`exporter.clinical_note.export = true`) and symptoms (`exporter.symptoms.csv.export = true`)
- Enables physiology generators (`physiology.generators.enabled = true`, `physiology.state.enabled = true`) and death by natural causes (`lifecycle.death_by_natural_causes = true`)
- Disables numeric suffixes on person names (`generate.append_numbers_to_person_names = false`)
