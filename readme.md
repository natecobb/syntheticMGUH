# Synthetic MGUH

This repository contains scripts to build a synthetic MedStar Georgetown University Hospital (MGUH) EHR dataset using [Synthea](https://github.com/synthetichealth/synthea) v4.0+. The dataset consists of linked clinical, demographic, provider, and financial tables exported as ZSTD-compressed Parquet files (with an automatically generated `codes.parquet` lookup dictionary enriched with OHDSI Athena OMOP, ICD-10-CM, and HCPCS mappings) ready for DuckDB or Databricks Unity Catalog.

## Overview & Revisions

- **2026 Revision (Synthea 4.0+ & Standalone Layout)**:
  - **Self-Contained `synthea/` Runtime & Overlay**: `./0_setup_synthea.sh` installs a stock untouched `synthea/bin/synthea-with-dependencies.jar` (and unpacks `synthea/bin/resources/physiology/`). All custom MedStar configurations (`synthea/synthea.properties`), pre-filtered DC/MedStar provider CSVs (`synthea/providers/syntheticMGUH/`), custom clinical modules (`synthea/modules/`), and the OHDSI Athena code crosswalk (`synthea/athena_code_crosswalk.parquet`) live under `./synthea/` and are applied at runtime via `-c ./synthea/synthea.properties -d ./synthea/modules`.
  - **Synthea 4.0+ (`master`, Gradle 9, Java 17+)**: Includes `NPI` columns in `organizations` and `providers`, fixes `1970-01-01` unset stop timestamps in CSV exports, fixes SSN/Passport/Driver's License uniqueness collisions at population scale, and disables orphan QALY/DALY/QOLS rows via `exporter.csv.include_quality_of_life_observations = false`.
  - **Expanded Civilian Specialty Modules & COVID-19 Removal**: Adds 15 civilian specialty GMF v2 modules (`bipolar_disorder`, `cirrhosis`, `generalized_anxiety_disorder`, `gerd`, `glaucoma`, `hepatitis_b`, `hepatitis_c`, `inflammatory_bowel_disease`, `major_depressive_disorder`, `migraine`, `multiple_sclerosis`, `parkinsons_disease`, `pneumonia`, `prostate_cancer`, `psoriasis`) and disables built-in COVID-19 infection and immunization modules via runtime overrides in `synthea/modules/`.
  - **OHDSI Athena OMOP + ICD-10-CM / HCPCS Code Crosswalk**: `3_convert_to_parquet.py` automatically enriches `codes.parquet` with `omop_concept_id`, `omop_domain_id`, `icd10cm_code`, `icd10cm_description`, and `hcpcs_code` using `synthea/athena_code_crosswalk.parquet` (100% `icd10cm_code` coverage across all `Condition`-domain diagnoses).
  - **Iterative Crash-Recoverable Pipeline & LLM Note Rewriting**: `2_generate_incremental.py` (and wrapper `2_generate_data.sh`) generates, converts to Parquet, checkpoints, and deletes raw CSV/note files every `X` patients. `scripts/process_notes.py` rewrites structured notes into natural clinician notes enriched with encounter vitals, exam findings, and screening scores.
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

## Build Pipeline

Run all steps from within the `syntheticMGUH/` directory:

1. **(Optional) Rebuild DC/MedStar provider files**:
   Default MGUH provider CSVs are pre-committed in `synthea/providers/syntheticMGUH/`. Re-run only if switching hospital filters (`MGUH`, `WHC`, or `both`):
   ```bash
   uv run python 1_build_providers.py --hospitals MGUH  # or 'both', 'WHC'
   ```
2. **Generate synthetic population & convert to Parquet**:
   - **Iterative, crash-recoverable batch mode (recommended for large populations)**:
     Generates, merges into Parquet, checkpoints (`output/seed_<seed>/parquet/_checkpoint.json`), and deletes raw CSV/note files every `X` patients:
     ```bash
     uv run python 2_generate_incremental.py --total 5000 --batch_size 250 --seed 12345 --years 14
     # Optional: pass --no_claims to skip claims/claims_transactions (~78% smaller Parquet)
     # Or via 2_generate_data.sh:
     ./2_generate_data.sh 5000 12345 14 250
     ```
   - **Single-batch mode**:
     ```bash
     ./2_generate_data.sh 500 12345 14
     ```
     *(Arguments: `<num_patients>` `<seed>` `<years_of_history>` `[batch_size]`)*
3. **Convert existing raw CSVs to Parquet & build code dictionary** *(automatic in step 2)*:
   ```bash
   uv run python 3_convert_to_parquet.py --include_notes --cleanup
   ```
   *(Add `--cleanup` to delete raw `output/csv` and `output/notes` after conversion; add `--keep_descriptions` to retain text description columns alongside `codes.parquet`.)*
4. **Validate Parquet output (optional)**:
   ```bash
   uv run python scripts/test_parquet_load.py
   ```
5. **Rewrite Clinical Notes with an LLM (optional)**:
   `scripts/process_notes.py` rewrites structured Synthea notes (`notes.parquet`) into natural physician notes (`notes_llm.parquet`) against any OpenAI-compatible `/v1` endpoint (e.g., local **oMLX** or remote **vLLM**). It automatically enriches each prompt via DuckDB with encounter vitals (`vital-signs`), physical exam findings (`exam`), screening questionnaire total scores (`PHQ-2`, `PHQ-9`, `GAD-7`, `AUDIT-C`, `DAST-10`, `HARK`, `Morse Fall Scale`), and encounter reason diagnoses, maps `encounters.encounterclass` to 10 structural note templates (`template_*.txt`) and `encounters.provider` to 10 clinician writing voices (`voice_*.txt`), and processes patients in 100-patient cohorts sorted by `(cohort, provider, encounterclass)` for KV prefix-cache reuse:
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

## Stock Synthea Customizations (`./synthea/`)

### Provider Files (`synthea/providers/syntheticMGUH/` & `1_build_providers.py`)
- Filters hospitals by NPI to MGUH (`1427145176`) and optionally MWHC (`1548378235`)
- Includes MedStar DC primary care clinics (`1417571530` One MedStar Primary Care, `1285636522` MedStar Georgetown Medical Center, `1922395581` MMG Georgetown Family Practice, `1194374710` GUH-Kids Mobile Medical Clinic)
- Configures Bridgepoint for long-term/SNF, MGUH for inpatient hospice, MGUH/MWHC for dialysis, National Rehab for rehabilitation, and MedStar VNA for home health
- Routes veteran care to MedStar hospitals via an empty `va_facilities.csv` override (`generate.providers.default_to_hospital_on_failure = true`)
- Adds MedStar PromptCare urgent care locations (Adams Morgan and Capitol Hill)

### Clinical Modules Overlay (`synthea/modules/`)
- Loaded at runtime via Synthea's `-d ./synthea/modules` flag without modifying the stock Synthea JAR
- Disables built-in COVID-19 infection and vaccination modules via no-op overrides (`covid19.json` and `COVID-19 Immunization Module.json`)
- Adds 15 civilian specialty GMF v2 disease modules (adapted and patched from `TIET-AI/tietai-synthea`): Bipolar Disorder, Cirrhosis, Generalized Anxiety Disorder, GERD, Glaucoma, Hepatitis B, Hepatitis C, Inflammatory Bowel Disease, Major Depressive Disorder, Migraine, Multiple Sclerosis, Parkinson's Disease, Pneumonia, Prostate Cancer, and Psoriasis

### Properties File (`synthea/synthea.properties`)
- CSV-only export (`exporter.fhir.export = false`, `exporter.csv.export = true`)
- Excludes quality-of-life orphan observations (`exporter.csv.include_quality_of_life_observations = false`)
- Sets ACA individual mandate year to `2014` (`generate.insurance.mandate.year = 2014`)
- Exports clinical notes (`exporter.clinical_note.export = true`) and symptoms (`exporter.symptoms.csv.export = true`)
- Enables physiology generators (`physiology.generators.enabled = true`, `physiology.state.enabled = true`) and death by natural causes (`lifecycle.death_by_natural_causes = true`)
- Disables numeric suffixes on person names (`generate.append_numbers_to_person_names = false`)
