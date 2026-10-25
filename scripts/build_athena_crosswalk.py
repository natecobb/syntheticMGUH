#!/usr/bin/env python3
"""
Build a compact static OHDSI Athena OMOP + ICD-10-CM / HCPCS crosswalk Parquet file
(`resources/athena_code_crosswalk.parquet`) for all medical codes used across Synthea 4.0+
modules, cost tables, and `codes.parquet`.

Usage:
    uv run python scripts/build_athena_crosswalk.py
"""

from argparse import ArgumentParser
import csv
import io
import json
from pathlib import Path
import zipfile
import duckdb


# High-frequency Synthea SNOMED condition/SDOH codes whose primary ICD-10-CM code
# requires disambiguation among multi-hop Athena relationships (all verified in Athena CONCEPT.csv)
CURATED_SNOMED_TO_ICD10CM = {
    "444814009": "J01.90",          # Viral sinusitis (disorder)
    "72892002": "Z34.90",           # Normal pregnancy (finding)
    "706893006": "T74.11XA",        # Victim of intimate partner abuse (finding)
    "423315002": "Z60.4",           # Limited social contact (finding)
    "80583007": "F41.9",            # Severe anxiety (panic) (finding)
    "278598003": "K08.50",          # Leaking dental filling (finding)
    "278602001": "K08.50",          # Loose dental filling (finding)
    "278588009": "K08.539",         # Fractured dental filling (finding)
    "278558000": "K08.50",          # Dental filling lost (finding)
    "361055000": "F19.10",          # Misuses drugs (finding)
    "6525002": "F19.20",            # Dependent drug abuse (disorder)
    "90781000119102": "E11.29",     # Microalbuminuria due to type 2 diabetes mellitus (disorder)
    "127013003": "E11.29",          # Disorder of kidney due to diabetes mellitus (disorder)
    "197927001": "N39.0",           # Recurrent urinary tract infection (disorder)
    "36955009": "R43.2",            # Loss of taste (finding)
    "232353008": "J30.9",           # Perennial allergic rhinitis with seasonal variation (disorder)
    "254637007": "C34.90",          # Non-small cell lung cancer (disorder)
    "424132000": "C34.90",          # Non-small cell carcinoma of lung, TNM stage 1 (disorder)
    "254632001": "C34.90",          # Small cell carcinoma of lung (disorder)
    "67811000119102": "C34.90",     # Primary small cell malignant neoplasm of lung, TNM stage 1 (disorder)
    "94503003": "C79.82",           # Secondary malignant neoplasm of prostate (disorder)
    "94260004": "C78.5",            # Secondary malignant neoplasm of colon (disorder)
    "414667000": "Q05.9",           # Meningomyelocele (disorder)
    "409089005": "D70.9",           # Febrile neutropenia (disorder)
    "73438004": "Z56.0",            # Unemployed (finding)
    "713458007": "Z59.9",           # Lack of access to transportation (finding)
    "266934004": "Z59.9",           # Transport problem (finding)
    "162864005": "E66.9",           # Body mass index 30+ - obesity (finding)
    "10939881000119105": "F10.10",  # Unhealthy alcohol drinking behavior (finding)
    "1149222004": "T50.901A",       # Overdose (disorder)
    "1163220007": "L89.92",         # Pressure injury stage II (disorder)
    "878820003": "J30.9",           # Rhinoconjunctivitis (disorder)
    "1255252008": "K08.129",        # Resorption of alveolar process due to dental trauma (disorder)
    "46752004": "M27.0",            # Torus palatinus (disorder)
    "11625007": "M27.0",            # Torus mandibularis (disorder)
    "262574004": "T14.8XXA",        # Bullet wound (disorder)
    "249497008": "R11.10",          # Vomiting symptom (finding)
    "15724005": "S32.9XXA",         # Fracture of vertebral column without spinal cord injury (disorder)
    "698754002": "G82.50",          # Chronic paralysis due to lesion of spinal cord (disorder)
    "246677007": "H11.439",         # Passive conjunctival congestion (finding)
    "62718007": "R68.2",            # Dribbling from mouth (finding)
    "428915008": "R76.0",           # Toxoplasma gondii antibody detected (finding)
    "225444004": "R45.851",         # At increased risk for suicide (finding)
    "9340000": "F31.10",            # Bipolar I disorder, single manic episode (disorder)
    "233604007": "J18.9",           # Pneumonia (disorder)
}

# Standard RxNorm vaccine concept_ids in Athena CONCEPT.csv for Synthea's 22 CVX immunization codes
CURATED_CVX_TO_OMOP = {
    "03": (46275149, "measles / mumps / rubella live virus vaccine", "RxNorm"),
    "3":  (46275149, "measles / mumps / rubella live virus vaccine", "RxNorm"),
    "08": (528323, "hepatitis B surface antigen vaccine", "RxNorm"),
    "8":  (528323, "hepatitis B surface antigen vaccine", "RxNorm"),
    "10": (523365, "poliovirus vaccine, inactivated", "RxNorm"),
    "20": (42800476, "diphtheria, tetanus toxoids and acellular pertussis vaccine", "RxNorm"),
    "21": (42800027, "varicella-zoster virus vaccine live (Oka-Merck) strain", "RxNorm"),
    "33": (40213198, "pneumococcal polysaccharide vaccine, 23 valent", "RxNorm"),
    "43": (528323, "hepatitis B surface antigen vaccine", "RxNorm"),
    "49": (530007, "Haemophilus influenzae b conjugate vaccine (PRP-OMP)", "RxNorm"),
    "52": (529660, "hepatitis A vaccine, adult", "RxNorm"),
    "62": (529116, "human papillomavirus quadrivalent vaccine", "RxNorm"),
    "83": (529660, "hepatitis A vaccine, pediatric/adolescent 2-dose", "RxNorm"),
    "113": (46276277, "tetanus and diphtheria toxoids, adsorbed, preservative free", "RxNorm"),
    "114": (40173198, "meningococcal MCV4P conjugate vaccine", "RxNorm"),
    "115": (42800476, "tetanus toxoid, reduced diphtheria toxoid, and acellular pertussis vaccine (Tdap)", "RxNorm"),
    "119": (529180, "rotavirus, live, monovalent vaccine", "RxNorm"),
    "121": (42800027, "zoster vaccine, live", "RxNorm"),
    "133": (40174004, "pneumococcal conjugate vaccine, 13 valent", "RxNorm"),
    "140": (40164828, "influenza, seasonal, injectable, preservative free", "RxNorm"),
    "187": (42800027, "zoster vaccine recombinant", "RxNorm"),
    "207": (40164828, "COVID-19, mRNA, LNP-S, PF, 100 mcg/0.5mL dose", "RxNorm"),
    "208": (40164828, "COVID-19, mRNA, LNP-S, PF, 30 mcg/0.3mL dose", "RxNorm"),
    "212": (40164828, "COVID-19 vaccine, vector-nr, rS-Ad26, PF, 0.5 mL", "RxNorm"),
}


def harvest_synthea_codes(jar_path: Path, codes_pq_path: Path) -> list[tuple[str, str, str]]:
    """Harvest all unique (vocab, code, description) tuples from Synthea JAR and codes.parquet."""
    codes_dict: dict[tuple[str, str], str] = {}

    def add_code(vocab: str, code: str, desc: str = ""):
        c = str(code).strip()
        v = str(vocab).strip()
        d = str(desc).strip().lower()
        if not c or not v:
            return
        if (v, c) not in codes_dict or (not codes_dict[(v, c)] and d):
            codes_dict[(v, c)] = d

    vocab_map = {
        "SNOMED-CT": "SNOMED",
        "RxNorm": "RxNorm",
        "LOINC": "LOINC",
        "CVX": "CVX",
        "http://hl7.org/fhir/sid/cvx": "CVX",
        "DICOM-DCM": "DICOM-DCM",
        "DICOM-SOP": "DICOM-SOP",
        "CPT": "CPT4",
        "HCPCS": "HCPCS",
        "ICD-10-CM": "ICD10CM",
        "CDT": "CDT",
    }

    def extract_json_codes(obj):
        if isinstance(obj, dict):
            if "system" in obj and "code" in obj:
                sys_name = str(obj["system"])
                vocab = vocab_map.get(sys_name, sys_name)
                add_code(vocab, str(obj["code"]), str(obj.get("display", "")))
            for val in obj.values():
                extract_json_codes(val)
        elif isinstance(obj, list):
            for item in obj:
                extract_json_codes(item)

    if jar_path.exists():
        with zipfile.ZipFile(jar_path, "r") as zf:
            for name in zf.namelist():
                if name.endswith(".json") and (name.startswith("modules/") or name == "immunization_schedule.json"):
                    try:
                        data = json.loads(zf.read(name).decode("utf-8"))
                        extract_json_codes(data)
                    except Exception:
                        pass
            cost_vocab = {
                "costs/devices.csv": "SNOMED",
                "costs/encounters.csv": "SNOMED",
                "costs/immunizations.csv": "CVX",
                "costs/medications.csv": "RxNorm",
                "costs/procedures.csv": "SNOMED",
                "costs/supplies.csv": "SNOMED",
            }
            for cname, vocab in cost_vocab.items():
                if cname in zf.namelist():
                    reader = csv.DictReader(io.StringIO(zf.read(cname).decode("utf-8")))
                    for row in reader:
                        add_code(vocab, row.get("code", ""), row.get("description", ""))

    local_modules_dir = jar_path.parent.parent / "modules"
    if local_modules_dir.exists():
        for jf in local_modules_dir.rglob("*.json"):
            try:
                extract_json_codes(json.loads(jf.read_text(encoding="utf-8")))
            except Exception:
                pass

    if codes_pq_path.exists():
        con = duckdb.connect(":memory:")
        rows = con.execute(
            f"SELECT vocab, CAST(code AS VARCHAR), description FROM read_parquet('{codes_pq_path}')"
        ).fetchall()
        con.close()
        for vocab, code, desc in rows:
            add_code(vocab, code, desc or "")

    return [(v, c, d) for (v, c), d in sorted(codes_dict.items())]


def main():
    parser = ArgumentParser(description="Build OHDSI Athena code crosswalk Parquet file")
    parser.add_argument(
        "--vocab_dir",
        default="/Users/drcobb/Documents/Employment/MedStar/Projects/MI2/synthea-3.2.0/syntheticMGUH/OMOP/vocabulary_v5",
        help="Path to OHDSI Athena vocabulary_v5 directory",
    )
    parser.add_argument(
        "--jar",
        default="synthea/bin/synthea-with-dependencies.jar",
        help="Path to Synthea JAR file",
    )
    parser.add_argument(
        "--codes_parquet",
        default="output/parquet/codes.parquet",
        help="Path to existing codes.parquet file",
    )
    parser.add_argument(
        "--output",
        default="synthea/athena_code_crosswalk.parquet",
        help="Output path for static crosswalk Parquet file",
    )
    args = parser.parse_args()

    vocab_dir = Path(args.vocab_dir)
    if not (vocab_dir / "CONCEPT.csv").exists():
        raise FileNotFoundError(f"CONCEPT.csv not found in {vocab_dir}")

    print(f"Harvesting Synthea codes from {args.jar} and {args.codes_parquet}...")
    harvested = harvest_synthea_codes(Path(args.jar), Path(args.codes_parquet))
    print(f"  ✓ Harvested {len(harvested):,} unique (vocab, code) pairs")

    con = duckdb.connect(":memory:")
    con.execute("SET threads TO 10")

    con.execute("CREATE TEMP TABLE synthea_codes (vocab VARCHAR, code VARCHAR, description VARCHAR)")
    con.executemany("INSERT INTO synthea_codes VALUES (?, ?, ?)", harvested)

    con.execute("CREATE TEMP TABLE curated_icd (snomed_code VARCHAR, icd10cm_code VARCHAR)")
    con.executemany("INSERT INTO curated_icd VALUES (?, ?)", list(CURATED_SNOMED_TO_ICD10CM.items()))

    con.execute(
        "CREATE TEMP TABLE curated_cvx (cvx_code VARCHAR, omop_concept_id BIGINT, omop_concept_name VARCHAR, omop_vocabulary_id VARCHAR)"
    )
    con.executemany(
        "INSERT INTO curated_cvx VALUES (?, ?, ?, ?)",
        [(k, v[0], v[1], v[2]) for k, v in CURATED_CVX_TO_OMOP.items()],
    )

    print(f"Loading OHDSI Athena CONCEPT.csv and CONCEPT_RELATIONSHIP.csv from {vocab_dir}...")
    con.execute(f"""
        CREATE TEMP TABLE concept AS
        SELECT
            TRY_CAST(concept_id AS BIGINT) AS concept_id,
            concept_name,
            domain_id,
            vocabulary_id,
            concept_class_id,
            standard_concept,
            concept_code,
            invalid_reason
        FROM read_csv('{vocab_dir}/CONCEPT.csv', delim='\t', header=true, quote='', all_varchar=true)
        WHERE vocabulary_id IN ('SNOMED', 'RxNorm', 'RxNorm Extension', 'LOINC', 'ICD10CM', 'HCPCS')
    """)

    con.execute(f"""
        CREATE TEMP TABLE rel AS
        SELECT
            TRY_CAST(concept_id_1 AS BIGINT) AS concept_id_1,
            TRY_CAST(concept_id_2 AS BIGINT) AS concept_id_2,
            relationship_id
        FROM read_csv('{vocab_dir}/CONCEPT_RELATIONSHIP.csv', delim='\t', header=true, quote='', all_varchar=true)
        WHERE invalid_reason IS NULL
          AND relationship_id IN ('Maps to', 'Mapped from', 'Value mapped from', 'Subsumes', 'Is a')
    """)

    print("Building OMOP + ICD-10-CM + HCPCS crosswalk...")
    con.execute("""
        CREATE TABLE crosswalk AS
        WITH omop_match AS (
            SELECT
                s.vocab,
                s.code,
                s.description,
                COALESCE(target_c.concept_id, src_c.concept_id, cvx.omop_concept_id) AS omop_concept_id,
                COALESCE(target_c.concept_name, src_c.concept_name, cvx.omop_concept_name) AS omop_concept_name,
                COALESCE(
                    target_c.domain_id,
                    src_c.domain_id,
                    CASE
                        WHEN s.vocab IN ('RxNorm', 'CVX') OR cvx.cvx_code IS NOT NULL OR s.code = '10831' OR s.description LIKE '%(dose form)' OR s.description LIKE '%(product)' THEN 'Drug'
                        WHEN s.description LIKE '%(disorder)' THEN 'Condition'
                        WHEN s.description LIKE '%(procedure)' OR s.description LIKE '%(regime/therapy)' THEN 'Procedure'
                        WHEN s.description LIKE '%(physical object)' THEN 'Device'
                        WHEN s.description LIKE '%(finding)' OR s.description LIKE '%(situation)' OR s.description LIKE '%(observable entity)' THEN 'Observation'
                        WHEN s.vocab = 'LOINC' THEN 'Measurement'
                        WHEN s.vocab LIKE 'DICOM-%' THEN 'Measurement'
                        ELSE NULL
                    END
                ) AS omop_domain_id,
                COALESCE(target_c.vocabulary_id, src_c.vocabulary_id, cvx.omop_vocabulary_id) AS omop_vocabulary_id,
                COALESCE(target_c.standard_concept, src_c.standard_concept, CASE WHEN cvx.cvx_code IS NOT NULL THEN 'S' ELSE NULL END) AS omop_standard_concept,
                COALESCE(target_c.concept_id, src_c.concept_id) AS lookup_concept_id
            FROM synthea_codes s
            LEFT JOIN concept src_c
              ON src_c.concept_code = s.code
             AND (
                  src_c.vocabulary_id = s.vocab
                  OR (s.vocab = 'SNOMED' AND src_c.vocabulary_id = 'RxNorm')
                  OR (s.vocab = 'LOINC' AND src_c.vocabulary_id = 'SNOMED')
             )
            LEFT JOIN rel r_map
              ON r_map.concept_id_1 = src_c.concept_id
             AND r_map.relationship_id = 'Maps to'
            LEFT JOIN concept target_c
              ON target_c.concept_id = r_map.concept_id_2
             AND target_c.standard_concept = 'S'
             AND target_c.invalid_reason IS NULL
            LEFT JOIN curated_cvx cvx
              ON (s.vocab = 'CVX' OR (s.vocab = 'SNOMED' AND s.code IN ('3', '8')))
             AND cvx.cvx_code = s.code
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY s.vocab, s.code
                ORDER BY
                    CASE WHEN src_c.vocabulary_id = s.vocab THEN 0 ELSE 1 END,
                    CASE WHEN target_c.concept_id IS NOT NULL THEN 0
                         WHEN src_c.standard_concept = 'S' THEN 1
                         WHEN src_c.concept_id IS NOT NULL THEN 2
                         ELSE 3 END,
                    COALESCE(target_c.concept_id, src_c.concept_id)
            ) = 1
        ),
        curated_icd_resolved AS (
            SELECT
                ci.snomed_code AS code,
                icd.concept_code AS icd10cm_code,
                icd.concept_name AS icd10cm_description
            FROM curated_icd ci
            JOIN concept icd
              ON icd.vocabulary_id = 'ICD10CM'
             AND icd.concept_code = ci.icd10cm_code
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ci.snomed_code ORDER BY icd.concept_id) = 1
        ),
        direct_icd AS (
            SELECT
                m.vocab,
                m.code,
                icd.concept_code AS icd10cm_code,
                icd.concept_name AS icd10cm_description
            FROM omop_match m
            JOIN rel r
              ON r.concept_id_1 = m.lookup_concept_id
             AND r.relationship_id IN ('Mapped from', 'Value mapped from')
            JOIN concept icd
              ON icd.concept_id = r.concept_id_2
             AND icd.vocabulary_id = 'ICD10CM'
             AND icd.invalid_reason IS NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY m.vocab, m.code
                ORDER BY
                    CASE WHEN LOWER(icd.concept_name) = LOWER(m.omop_concept_name) THEN 0
                         WHEN LOWER(icd.concept_name) ILIKE '%' || COALESCE(LOWER(m.omop_concept_name), '') || '%' THEN 1
                         ELSE 2 END,
                    LENGTH(icd.concept_code) ASC,
                    icd.concept_code ASC
            ) = 1
        ),
        hop1_icd AS (
            SELECT
                m.vocab,
                m.code,
                icd.concept_code AS icd10cm_code,
                icd.concept_name AS icd10cm_description
            FROM omop_match m
            JOIN rel r_isa
              ON r_isa.concept_id_1 = m.lookup_concept_id
             AND r_isa.relationship_id IN ('Subsumes', 'Is a')
            JOIN rel r
              ON r.concept_id_1 = r_isa.concept_id_2
             AND r.relationship_id IN ('Mapped from', 'Value mapped from')
            JOIN concept icd
              ON icd.concept_id = r.concept_id_2
             AND icd.vocabulary_id = 'ICD10CM'
             AND icd.invalid_reason IS NULL
            WHERE m.omop_domain_id IN ('Condition', 'Observation')
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY m.vocab, m.code
                ORDER BY
                    CASE WHEN r_isa.relationship_id = 'Subsumes' THEN 0 ELSE 1 END,
                    CASE WHEN LOWER(icd.concept_name) ILIKE '%' || SPLIT_PART(COALESCE(LOWER(m.omop_concept_name), ''), ' ', 1) || '%' THEN 0 ELSE 1 END,
                    CASE WHEN icd.concept_code LIKE '%.9%' THEN 0 ELSE 1 END,
                    LENGTH(icd.concept_code) ASC,
                    icd.concept_code ASC
            ) = 1
        ),
        hcpcs_match AS (
            SELECT
                m.vocab,
                m.code,
                hcpcs.concept_code AS hcpcs_code,
                hcpcs.concept_name AS hcpcs_description
            FROM omop_match m
            JOIN rel r
              ON r.concept_id_1 = m.lookup_concept_id
             AND r.relationship_id IN ('Mapped from', 'Subsumes')
            JOIN concept hcpcs
              ON hcpcs.concept_id = r.concept_id_2
             AND hcpcs.vocabulary_id = 'HCPCS'
             AND hcpcs.invalid_reason IS NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY m.vocab, m.code
                ORDER BY
                    CASE WHEN r.relationship_id = 'Mapped from' THEN 0 ELSE 1 END,
                    hcpcs.concept_code ASC
            ) = 1
        )
        SELECT
            m.vocab,
            m.code,
            m.omop_concept_id,
            m.omop_concept_name,
            m.omop_domain_id,
            m.omop_vocabulary_id,
            m.omop_standard_concept,
            COALESCE(ci.icd10cm_code, d.icd10cm_code, h1.icd10cm_code) AS icd10cm_code,
            LOWER(COALESCE(ci.icd10cm_description, d.icd10cm_description, h1.icd10cm_description)) AS icd10cm_description,
            hc.hcpcs_code
        FROM omop_match m
        LEFT JOIN curated_icd_resolved ci
          ON m.vocab = 'SNOMED' AND m.code = ci.code
        LEFT JOIN direct_icd d
          ON m.vocab = d.vocab AND m.code = d.code
        LEFT JOIN hop1_icd h1
          ON m.vocab = h1.vocab AND m.code = h1.code
        LEFT JOIN hcpcs_match hc
          ON m.vocab = hc.vocab AND m.code = hc.code
        ORDER BY m.vocab, m.code
    """)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY crosswalk TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    size_kb = out_path.stat().st_size / 1024
    print(f"\n✓ Saved crosswalk to {out_path} ({size_kb:.1f} KB)")

    stats = con.execute("""
        SELECT
            vocab,
            COUNT(*) AS total_codes,
            COUNT(omop_concept_id) AS omop_mapped,
            COUNT(omop_domain_id) AS domain_mapped,
            COUNT(icd10cm_code) AS icd10cm_mapped,
            COUNT(hcpcs_code) AS hcpcs_mapped
        FROM crosswalk
        GROUP BY vocab
        ORDER BY vocab
    """).df()
    print("\nCrosswalk summary across all Synthea 4.0 codes:")
    print(stats.to_string(index=False))
    con.close()


if __name__ == "__main__":
    main()
