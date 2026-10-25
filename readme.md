# Synthetic MGUH

A realistic, linked synthetic Electronic Health Record (EHR) dataset for **MedStar Georgetown University Hospital (MGUH)** and Washington, DC healthcare facilities built on [Synthea](https://github.com/synthetichealth/synthea) v4.0+.

The dataset exports **22 ZSTD-compressed Parquet tables** (~58.7 MB per 1,000 living patients) with a normalized medical vocabulary and **OHDSI Athena OMOP / ICD-10-CM / HCPCS code crosswalk** (`codes.parquet`), ready for immediate querying in **DuckDB**, **Python** (`duckdb` / `polars` / `pandas`), **R**, or **Databricks Unity Catalog**.

> 📖 **Full Schema & Query Reference**: See [**`docs/DATA_DICTIONARY.md`**](docs/DATA_DICTIONARY.md) for the entity-relationship diagram, column-by-column schema definitions for all 22 Parquet tables, code normalization & crosswalk details, and copy-paste SQL/Python recipes.

---

## Key Dataset Features

- **DC & MedStar Facility Network**: Encounters route across NPI-matched MedStar DC hospitals, primary care clinics, urgent care centers, subacute/rehab facilities, home health, and hospice (`100%` of organizations and providers include 10-digit `NPI` identifiers).
- **Simulated `2010-01-01` EHR Launch Cutoff**: Operational event tables (`encounters`, `observations`, `procedures`, `imaging_studies`, `supplies`, `claims`, `notes`) are filtered to `>= 2010-01-01`, while pre-2010 chronic conditions, allergies, medications, immunizations, careplans, and devices are preserved and linked to each patient's earliest EHR visit (`patients.first_encounter_id`) with zero orphan foreign keys.
- **Normalized Code Dictionary + OHDSI Athena Crosswalk (`codes.parquet`)**: Redundant text description columns are stripped from clinical tables (saving 30–50% storage) and consolidated into `codes.parquet`, automatically enriched with `omop_concept_id`, `omop_domain_id`, `icd10cm_code`, `icd10cm_description`, and `hcpcs_code` (**100% `icd10cm_code` coverage** across all `Condition`-domain diagnosis rows).
- **Expanded Civilian Specialty Modules (COVID-19 Excluded)**: Includes 105 clinical modules (stock Synthea 4.0 plus 15 civilian specialty GMF v2 modules in `synthea/modules/`, with built-in COVID-19 infection and immunization modules disabled).
- **Natural Clinician Notes (`notes_llm.parquet`)**: Optional LLM rewriting pipeline (`scripts/process_notes.py`) enriches structured Synthea notes with encounter vitals, physical exam findings, and screening scores (`PHQ-2`, `PHQ-9`, `GAD-7`, `AUDIT-C`, `DAST-10`, `HARK`, `Morse Fall Scale`) across 10 encounter-class templates and 10 provider writing voices.

---

## Parquet Tables Overview

All generated Parquet files are written to `./output/seed_<seed>/parquet/` (symlinked at `./output/parquet/`). See [**`docs/DATA_DICTIONARY.md`**](docs/DATA_DICTIONARY.md) for full column schemas.

| Category | Parquet Tables | Primary Keys & Key Joins |
|---|---|---|
| **Demographics & Directory** | `patients`, `organizations`, `providers`, `payers`, `payer_transitions` | `patients.id`, `organizations.id` (`npi`), `providers.id` (`npi`), `payers.id` |
| **Clinical Encounters & Care** | `encounters`, `conditions`, `medications`, `procedures`, `observations`, `immunizations`, `allergies`, `careplans`, `devices`, `imaging_studies`, `supplies`, `symptoms` | Linked via `patient` → `patients.id` and `encounter` → `encounters.id` |
| **Clinical Notes** | `notes`, `notes_llm` | Linked to `encounters` via `(patient, encounter)` |
| **Financial / Claims** | `claims`, `claims_transactions` | Linked via `patientid`, `appointmentid` → `encounters.id`, and `claimid` → `claims.id` |
| **Vocabulary & Crosswalk** | `codes` | `(vocab, code)` — includes `description`, `omop_concept_id`, `omop_domain_id`, `icd10cm_code`, `icd10cm_description`, `hcpcs_code` |

### Quick Query Example (Python + DuckDB)

```python
import duckdb

con = duckdb.connect()

# Top 10 medical diagnoses with ICD-10-CM and OMOP mappings
df = con.execute("""
    SELECT
        c.icd10cm_code,
        c.icd10cm_description,
        c.omop_concept_id,
        COUNT(DISTINCT co.patient) AS patients,
        COUNT(*) AS diagnosis_rows
    FROM 'output/parquet/conditions.parquet' co
    JOIN 'output/parquet/codes.parquet' c
      ON c.vocab = 'SNOMED' AND c.code = CAST(co.code AS VARCHAR)
    WHERE c.omop_domain_id = 'Condition'
    GROUP BY 1, 2, 3
    ORDER BY patients DESC
    LIMIT 10
""").df()
print(df)
```

---

## Installation & Setup

Requires **Java 17+** and **uv** (Python 3.13+):

```bash
git clone https://github.com/natecobb/syntheticMGUH
cd syntheticMGUH
uv sync
./0_setup_synthea.sh
```

`./0_setup_synthea.sh` installs a stock Synthea 4.0+ fat JAR into `synthea/bin/synthea-with-dependencies.jar` and unpacks `synthea/bin/resources/physiology/`. All MedStar configurations (`synthea/synthea.properties`), pre-filtered DC provider CSVs (`synthea/providers/syntheticMGUH/`), custom clinical modules (`synthea/modules/`), and the OHDSI Athena crosswalk (`synthea/athena_code_crosswalk.parquet`) are committed in `./synthea/` and applied at runtime.

---

## Dataset Generation Pipeline

Run all steps from the repository root (`syntheticMGUH/`):

### 1. (Optional) Rebuild DC/MedStar Provider Files
Default MGUH provider files are pre-committed in `synthea/providers/syntheticMGUH/`. Re-run only if switching hospital filters (`MGUH`, `WHC`, or `both`):
```bash
uv run python 1_build_providers.py --hospitals MGUH  # Choices: MGUH, WHC, MWHC, both
```

### 2. Generate Synthetic Population & Convert to Parquet
`2_generate_incremental.py` (and shell wrapper `2_generate_data.sh`) runs Synthea in crash-recoverable batches of `--batch_size` patients, converts each batch to DuckDB/Parquet, enriches `codes.parquet`, checkpoints progress to `output/seed_<seed>/parquet/_checkpoint.json`, and deletes raw CSV/note files after each batch:

```bash
# Recommended: Generate 5,000 living patients in recoverable batches of 250
uv run python 2_generate_incremental.py --total 5000 --batch_size 250 --seed 12345 --years 14

# Or via positional arguments on 2_generate_data.sh (<total> <seed> <years> [batch_size]):
./2_generate_data.sh 5000 12345 14 250
```

| Flag (`2_generate_incremental.py`) | Default | Description |
|---|---|---|
| `-p`, `--total` | `500` | Target number of living patients across all batches |
| `-b`, `--batch_size` | `100` | Patients per batch before Parquet merge and raw file cleanup |
| `-s`, `--seed` | `12345` | Base random seed for population and fixed clinician seed (`-cs`) |
| `-y`, `--years` | `14` | Years of history retained by Synthea exporter |
| `--start` | `2010-01-01` | Simulated EHR launch date for filtering operational tables |
| `--no_claims` | `False` | Exclude `claims` and `claims_transactions` (~78% smaller Parquet footprint) |
| `--no_notes` | `False` | Disable clinical note generation and parsing |
| `--keep_descriptions` | `False` | Retain redundant text `description` columns alongside `codes.parquet` |
| `-t`, `--threads` | `10` | Max CPU threads used by Synthea and DuckDB |
| `--reset` | `False` | Ignore existing checkpoint and start a fresh build |

### 3. Standalone CSV-to-Parquet Conversion *(Automatic in Step 2)*
If converting an existing raw Synthea output directory manually:
```bash
uv run python 3_convert_to_parquet.py --include_notes --cleanup
```

### 4. Validate Parquet Output
Runs 145 automated checks verifying table schemas, primary keys, zero foreign-key orphans across all 22 tables, Synthea 4.0+ features, `codes.parquet` normalization, and OHDSI Athena crosswalk coverage:
```bash
uv run python scripts/test_parquet_load.py
```

### 5. Rewrite Clinical Notes with an LLM (Optional)
`scripts/process_notes.py` rewrites structured Synthea notes (`notes.parquet`) into natural clinician notes (`notes_llm.parquet`) against any OpenAI-compatible `/v1` endpoint. It enriches prompts via DuckDB with encounter vitals (`vital-signs`), physical exam findings (`exam`), screening questionnaire total scores (`survey`), and encounter reason diagnoses, maps `encounters.encounterclass` to 10 structural note templates (`template_*.txt`) and `encounters.provider` to 10 clinician writing voices (`voice_*.txt`), and processes patients in 100-patient cohorts sorted by `(cohort, provider, encounterclass)` for KV prefix-cache reuse:

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

### 6. Sync to Databricks Unity Catalog (Optional)
```bash
./4_sync_to_databricks.sh
```

---

## MedStar & Synthea Customizations (`./synthea/`)

### Included MedStar DC Facilities (`synthea/providers/syntheticMGUH/`)

| Care Setting | Facility Name | NPI | Notes |
|---|---|---|---|
| **Acute Hospital** | MedStar Georgetown University Hospital (MGUH) | `1427145176` | Default hospital (`--hospitals MGUH` or `both`) |
| **Acute Hospital** | MedStar Washington Hospital Center (MWHC) | `1548378235` | Enabled with `--hospitals WHC` or `both` |
| **Primary Care** | One MedStar Primary Care (1001 G St NW) | `1417571530` | Adult & general primary care |
| **Primary Care** | MedStar Georgetown Medical Center | `1285636522` | Outpatient primary & specialty clinic |
| **Primary Care** | MMG Georgetown Family Practice | `1922395581` | Family medicine |
| **Primary Care** | GUH-Kids Mobile Medical Clinic | `1194374710` | Pediatric primary care |
| **Ambulatory Surgery** | MedStar Surgery Center at Lafayette Centre | `1912090432` | Outpatient surgical center |
| **Rehabilitation** | MedStar National Rehabilitation Hospital | `1326496456` | Inpatient & outpatient rehab |
| **Long-Term / LTACH** | Bridgepoint Hospital Capitol Hill | `1285772772` | Long-term acute care |
| **Skilled Nursing (SNF)** | Bridgepoint Sub-Acute and Rehab Capitol Hill | `1205923604` | Skilled nursing facility |
| **Home Health** | MedStar Health Visiting Nurse Association (VNA) | `1407960248` | Home health services |
| **Inpatient Hospice** | MedStar Georgetown University Hospital | `1427145176` | Routed to MGUH |
| **Dialysis** | MGUH / MWHC | `1427145176` | Routed to active hospital(s) |
| **Urgent Care** | MedStar PromptCare (Adams Morgan & Capitol Hill) | Custom (`4001`, `4002`) | Freestanding urgent care clinics |
| **Veteran Care** | Routed to MedStar facilities | — | Empty `va_facilities.csv` + `default_to_hospital_on_failure = true` |

### Clinical Modules Overlay (`synthea/modules/`)
Loaded at runtime via `-d ./synthea/modules` without modifying the stock Synthea JAR:
- **COVID-19 Disabled**: `covid19.json` and `COVID-19 Immunization Module.json` override the built-in infection and vaccination modules with immediate `Initial -> Terminal` transitions.
- **15 Added Civilian Specialty Modules** (adapted and patched from `TIET-AI/tietai-synthea`):
  - **Psychiatry**: `bipolar_disorder.json`, `generalized_anxiety_disorder.json`, `major_depressive_disorder.json`
  - **Neurology**: `migraine.json`, `multiple_sclerosis.json`, `parkinsons_disease.json`
  - **Gastroenterology & Hepatology**: `cirrhosis.json`, `gerd.json`, `hepatitis_b.json`, `hepatitis_c.json`, `inflammatory_bowel_disease.json`
  - **Ophthalmology, Dermatology, Oncology & Infectious Disease**: `glaucoma.json`, `psoriasis.json`, `prostate_cancer.json`, `pneumonia.json`

The existing [cirrhosis pathway](docs/CIRRHOSIS_PATHWAY.md) now sends complications through emergency assessment and severity-based ward or ICU care. Medical recovery returns to chronic management; transplantation ends the native-cirrhosis loop. Its mixed-cause diagnosis is preserved unless an alcohol cause is explicitly supplied. The callable [`liver/` submodules](docs/LIVER_MODEL.md) provide correlated daily labs and configurable outcomes; the [ICU/transplant course](docs/LIVER_CARE.md) adds organ support, evaluation and graft waiting, surgery, discharge, and recipient follow-up. Fuller alcohol-specific history and population entry for toxic injury remain in issues #6/#7.

The [calibration runner](docs/LIVER_CALIBRATION.md) separately models a 25% MGUH referral share for DC-metro cases and transplantation among actual ICU entrants. The original three-cause reference cohort retains its 25% transplant target. For the integrated cirrhosis pathway, the user accepted approximately 21% after a training result of 20.74%; its fitted evaluation parameter is separate from the reference cohort's. Entry severity, incidence, mortality, and other clinical parameters remain demonstration assumptions. Existing hospital-cohort patients are not filtered through referral a second time.

Clinical notes use a repository template override at `synthea/templates/notes/note.ftl` to include encounter-start timestamps. This keeps same-day emergency and inpatient notes linked to their respective encounters. The stock JAR remains unchanged; keep `synthea` first on the runtime classpath. Older date-only notes retain the converter's date-based matching behavior.

### Key Properties (`synthea/synthea.properties`)
- CSV-only export (`exporter.fhir.export = false`, `exporter.csv.export = true`)
- Excludes quality-of-life orphan observations (`exporter.csv.include_quality_of_life_observations = false`)
- Sets ACA individual mandate year to `2014` (`generate.insurance.mandate.year = 2014`)
- Exports clinical notes (`exporter.clinical_note.export = true`) and symptoms (`exporter.symptoms.csv.export = true`)
- Enables physiology generators (`physiology.generators.enabled = true`, `physiology.state.enabled = true`) and death by natural causes (`lifecycle.death_by_natural_causes = true`)
- Disables numeric suffixes on person names (`generate.append_numbers_to_person_names = false`)

---

## Revision History

- **2026 Revision**: Upgraded to Synthea 4.0+ (`master`, Java 17+) in a self-contained `./synthea/` layout; added crash-recoverable incremental generation (`2_generate_incremental.py`), OHDSI Athena OMOP / ICD-10-CM / HCPCS crosswalk enrichment (`synthea/athena_code_crosswalk.parquet`), 15 civilian specialty modules (with COVID-19 disabled), and LLM note rewriting (`scripts/process_notes.py`).
- **2025 Revision**: Replaced legacy PostgreSQL workflow with serverless DuckDB → Parquet conversion (`3_convert_to_parquet.py`), `codes.parquet` normalization, and Databricks Unity Catalog sync.
- **2023 Revision**: Introduced `2010-01-01` EHR launch date filtering (`first_encounter_id` historical linking) and NPI-based DC/MedStar provider filtering (`1_build_providers.py`).
