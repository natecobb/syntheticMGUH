#!/usr/bin/env python3
"""
Process Synthea clinical notes in two passes:

Pass 1 (parse):
  - Parse text files into structured format
  - Associate notes with encounters using patient_id and date
  - Output to notes.parquet
  (Note: 3_convert_to_parquet.py now performs Pass 1 automatically during Parquet conversion.)

Pass 2 (transform):
  - Read notes.parquet (joined with encounters.parquet / providers.parquet / organizations.parquet)
  - Map encounter.encounterclass -> structural note template (template_*.txt)
  - Map encounter.provider -> clinician writing voice (voice_*.txt) via scripts/templates/style_mapping.json
  - Group notes by (provider, encounterclass) for KV prefix-cache reuse and concurrent batching
  - Append batches incrementally to notes_llm.parquet
"""

import os
import re
import json
import time
import duckdb
import requests
import multiprocessing
from pathlib import Path
from argparse import ArgumentParser
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timezone
from dateutil import parser as date_parser
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
try:
    from .note_identity import assign_note_ids
except ImportError:
    from note_identity import assign_note_ids


def estimate_tokens(text: str) -> int:
    """
    Estimate token count for text (~4 characters per token for English text).
    """
    if not text:
        return 0
    return len(text) // 4


def resolve_api_key(explicit_key: Optional[str] = None) -> str:
    """
    Resolve API key from CLI arg, environment variables, or ~/.omlx/settings.json.
    """
    if explicit_key:
        return explicit_key
    for env_var in ("OMLX_API_KEY", "OPENAI_API_KEY"):
        if os.environ.get(env_var):
            return os.environ[env_var]
    omlx_settings = Path.home() / ".omlx" / "settings.json"
    if omlx_settings.exists():
        try:
            with open(omlx_settings, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            key = cfg.get("auth", {}).get("api_key")
            if key:
                return key
        except Exception:
            pass
    return "omlx-local"


def check_llm_server_available(base_url: str, model: str, api_key: str) -> bool:
    """Check if the OpenAI-compatible LLM server (oMLX / vLLM / Ollama) is running and has the model."""
    base_url = base_url.rstrip("/")
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    try:
        response = requests.get(f"{base_url}/models", headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json().get("data", [])
            model_ids = [m.get("id") for m in data]
            if model in model_ids:
                print(f"✓ Model '{model}' is available at {base_url}")
                return True
            else:
                print(f"✗ Model '{model}' not found at {base_url}")
                print(f"  Available models: {', '.join(str(m) for m in model_ids)}")
                return False
        else:
            print(f"✗ Server returned HTTP {response.status_code}: {response.text[:200]}")
            return False
    except requests.exceptions.ConnectionError:
        print(f"✗ Cannot connect to LLM server at {base_url}")
        print("  Ensure oMLX (or your remote GPU server) is running.")
        return False


def build_system_prompt(template: Optional[str] = None, voice_example: Optional[str] = None) -> str:
    """
    Build the static system prompt for a (template, voice) pair so oMLX / vLLM can KV-cache the prefix.
    """
    parts = [
        "Transform the structured clinical note provided by the user into a natural physician's note. "
        "Write ONLY the clinical note, with no preamble, commentary, or markdown code fences."
    ]

    if template:
        parts.append(f"\nUse this note template/structure:\n{template.strip()}")

    if voice_example:
        parts.append(
            "\nStyle Reference Only (adopt ONLY the tone, sentence structure, formatting, and abbreviation density "
            "of this example—NEVER copy any symptoms, diagnoses, procedures, or medications from this example):\n"
            f"{voice_example.strip()}"
        )

    parts.append(
        "\nStrict Guidelines:\n"
        "- Include all clinical facts explicitly present in the user's Structured Note (including Vital Signs, Exam Findings, Screening Scores, and Encounter Reason when provided)\n"
        "- Do NOT invent or add physical exam findings, vital signs, review-of-systems negatives, or return precautions that are not in the Structured Note\n"
        "- Remove markdown headers (# symbols)\n"
        "- Strictly match the length, brevity, and abbreviation density of the Style Reference"
    )
    return "\n".join(parts)


_HURRIED_WORD_SWAPS = {
    "patient": "pt",
    "presents": "presnets",
    "presenting": "presneting",
    "history": "hx",
    "medications": "meds",
    "medication": "med",
    "assessment": "assesment",
    "screening": "screenign",
    "performed": "preformed",
    "administered": "adminstered",
    "continued": "contd",
    "continue": "cont",
    "referral": "refferal",
    "referred": "reffered",
    "evaluation": "eval",
    "evaluated": "evald",
    "hypertension": "htn",
    "diabetes": "dm",
    "prescription": "rx",
    "prescribed": "rx'd",
    "without": "w/o",
    "with": "w",
}


def apply_hurried_typos(text: str, seed_str: str = "") -> str:
    """
    Post-process notes for voice_hurried_typos so small RLHF models that auto-correct spelling
    still exhibit realistic lowercase hurried typing and keyboard transpositions while preserving
    all numbers and dosages.
    """
    import hashlib
    out = text.lower()
    out = re.sub(r"\bfollow[\s-]+up\b", "follwo up", out)
    out = re.sub(r"\bno known (drug )?allergies\b", "nkda", out)

    def _transform_token(match: re.Match) -> str:
        word = match.group(0)
        if word in _HURRIED_WORD_SWAPS:
            return _HURRIED_WORD_SWAPS[word]
        if len(word) >= 6 and word.isalpha():
            h = hashlib.md5(f"{seed_str}:{word}".encode("utf-8")).digest()[0]
            if h < 10:  # ~4% of long alphabetic words get an interior adjacent-letter transposition
                pos = 2 + (h % (len(word) - 3))
                chars = list(word)
                chars[pos], chars[pos + 1] = chars[pos + 1], chars[pos]
                return "".join(chars)
        return word

    return re.sub(r"\b[a-z]{4,}\b", _transform_token, out)


class StyleResolver:
    """
    Resolves (encounterclass -> template) and (provider -> voice) from style_mapping.json,
    caching file contents and compiled system prompts in memory.
    """

    def __init__(
        self,
        style_map_path: Optional[str] = "scripts/templates/style_mapping.json",
        override_template_path: Optional[str] = None,
        override_voice_path: Optional[str] = None
    ):
        self.override_template_path = override_template_path
        self.override_voice_path = override_voice_path
        self.default_template = None
        self.default_voice = None
        self.encounter_templates: Dict[str, str] = {}
        self.by_provider_id: Dict[str, str] = {}
        self.by_npi: Dict[str, str] = {}
        self.by_org_name: Dict[str, str] = {}
        self._file_cache: Dict[str, str] = {}
        self._prompt_cache: Dict[Tuple[Optional[str], Optional[str]], str] = {}

        if style_map_path and Path(style_map_path).exists():
            with open(style_map_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            self.default_template = cfg.get("default_template")
            self.default_voice = cfg.get("default_voice")
            for k, v in cfg.get("encounter_templates", {}).items():
                self.encounter_templates[k.lower().strip()] = v
            for entry in cfg.get("provider_voices", []):
                voice = entry.get("voice")
                if not voice:
                    continue
                if entry.get("provider_id"):
                    self.by_provider_id[str(entry["provider_id"]).strip()] = voice
                if entry.get("npi"):
                    self.by_npi[str(entry["npi"]).strip()] = voice
                if entry.get("organization_name"):
                    self.by_org_name[str(entry["organization_name"]).strip().upper()] = voice

    def _read_cached(self, path_str: Optional[str]) -> Optional[str]:
        if not path_str:
            return None
        if path_str not in self._file_cache:
            p = Path(path_str)
            self._file_cache[path_str] = p.read_text(encoding="utf-8") if p.exists() else None
        return self._file_cache[path_str]

    def resolve(
        self,
        encounterclass: Optional[str],
        provider_id: Optional[str],
        provider_npi: Optional[str] = None,
        org_name: Optional[str] = None
    ) -> Tuple[Optional[str], Optional[str], str]:
        """
        Return (template_path, voice_path, compiled_system_prompt).
        """
        if self.override_template_path:
            t_path = self.override_template_path
        else:
            ec_key = (encounterclass or "").lower().strip()
            t_path = self.encounter_templates.get(ec_key, self.default_template)

        if self.override_voice_path:
            v_path = self.override_voice_path
        else:
            pid = (provider_id or "").strip()
            npi = str(provider_npi or "").strip()
            org = (org_name or "").strip().upper()
            v_path = (
                self.by_provider_id.get(pid)
                or self.by_npi.get(npi)
                or self.by_org_name.get(org)
                or self.default_voice
            )

        key = (t_path, v_path)
        if key not in self._prompt_cache:
            t_text = self._read_cached(t_path)
            v_text = self._read_cached(v_path)
            self._prompt_cache[key] = build_system_prompt(template=t_text, voice_example=v_text)

        return t_path, v_path, self._prompt_cache[key]


def parse_notes_pass1(
    notes_dir: str,
    parquet_dir: str,
    output_path: str,
    limit: int = None
) -> Dict[str, Any]:
    """
    Pass 1: Parse all note text files and create notes.parquet with encounter associations.
    """
    notes_path = Path(notes_dir)
    parquet_path = Path(parquet_dir)
    output_file = Path(output_path)

    if not notes_path.exists():
        print(f"✗ Notes directory not found: {notes_dir}")
        return {"error": "notes_dir not found"}

    if not parquet_path.exists():
        print(f"✗ Parquet directory not found: {parquet_dir}")
        return {"error": "parquet_dir not found"}

    note_files = sorted(notes_path.glob("*.txt"))
    if not note_files:
        print(f"✗ No .txt files found in {notes_dir}")
        return {"error": "no files found"}

    if limit:
        note_files = note_files[:limit]

    cpu_count = multiprocessing.cpu_count()

    print(f"\n{'='*60}")
    print("PASS 1: PARSING NOTES AND ASSOCIATING WITH ENCOUNTERS")
    print(f"{'='*60}")
    print(f"Processing {len(note_files)} patient files using {cpu_count} CPU cores")

    con = duckdb.connect(':memory:')

    encounters_file = parquet_path / "encounters.parquet"
    if not encounters_file.exists():
        print(f"✗ encounters.parquet not found in {parquet_dir}")
        return {"error": "encounters.parquet not found"}

    con.execute(f"""
        CREATE TABLE encounters AS
        SELECT * FROM read_parquet('{encounters_file}')
    """)

    con.execute("""
        CREATE TABLE notes_raw (
            patient VARCHAR,
            date DATE,
            note_text VARCHAR
        )
    """)

    total_notes = 0
    batch_data = []

    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        future_to_file = {
            executor.submit(parse_notes_file_wrapper, note_file): note_file
            for note_file in note_files
        }
        for future in as_completed(future_to_file):
            _, note_records = future.result()
            if note_records:
                for record in note_records:
                    note_date = date_parser.parse(record['date'])
                    batch_data.append((
                        record['patient_id'],
                        note_date.strftime('%Y-%m-%d'),
                        record['note_text']
                    ))
                total_notes += len(note_records)

    if batch_data:
        con.executemany("INSERT INTO notes_raw VALUES (?, ?::DATE, ?)", batch_data)

    con.execute("""
        CREATE TABLE notes AS
        SELECT
            n.patient,
            n.date,
            n.note_text,
            e.id AS encounter
        FROM notes_raw n
        JOIN encounters e
          ON e.patient = n.patient
         AND n.date BETWEEN CAST(e.start AS DATE) AND COALESCE(CAST(e.stop AS DATE), n.date)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY n.patient, n.date, n.note_text ORDER BY e.start DESC) = 1
    """)

    matched = con.execute("SELECT COUNT(*) FROM notes").fetchone()[0]
    unmatched = total_notes - matched

    con.execute(f"""
        COPY (SELECT * FROM notes ORDER BY patient, date)
        TO '{output_file}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    con.close()

    return {
        "total_files": len(note_files),
        "total_notes": total_notes,
        "matched_encounters": matched,
        "unmatched_encounters": unmatched
    }


def parse_notes_file(file_path: Path) -> List[Dict[str, str]]:
    """Parse a single patient notes file into individual note records."""
    regex_id = r"(^.*_)(.*?)\.txt"
    file_name = os.path.basename(file_path)
    match = re.search(regex_id, file_name)
    if not match:
        return []

    patient_id = match.group(2)
    with open(file_path, 'r', encoding='utf-8') as f:
        all_notes = f.read()

    regex_notes = r"([1-2][0-9]{3}-[0-9]{2}-[0-9]{2}.*?)\n\n([\s\S]*?)(?=\n\n\n\n|\Z)"
    notes = re.findall(regex_notes, all_notes)

    note_records = []
    for date_str, note_text in notes:
        date_str = date_str.strip()
        note_text = note_text.strip()
        if note_text:
            note_records.append({
                "patient_id": patient_id,
                "date": date_str,
                "note_text": note_text
            })
    return note_records


def parse_notes_file_wrapper(note_file: Path) -> Tuple[str, List[Dict[str, str]]]:
    return (note_file.name, parse_notes_file(note_file))


def transform_note_with_llm(
    session: requests.Session,
    base_url: str,
    api_key: str,
    note_text: str,
    system_prompt: str,
    model: str,
    temperature: float,
    max_tokens: int = 1200,
    template_path: str = None,
    voice_path: str = None,
    encounterclass: str = None,
    provider_id: str = None
) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """
    Transform a structured note into a natural clinical note via OpenAI-compatible /v1/chat/completions.
    """
    user_content = f"Structured Note:\n{note_text}\n\nClinical Note:"
    input_chars = len(note_text)
    input_tokens_est = estimate_tokens(note_text)
    prompt_chars = len(system_prompt) + len(user_content)
    prompt_tokens_est = estimate_tokens(system_prompt) + estimate_tokens(user_content)

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content}
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
        "stream": False,
        "chat_template_kwargs": {"enable_thinking": False}
    }

    start_time = time.time()
    try:
        response = session.post(
            f"{base_url.rstrip('/')}/chat/completions",
            headers=headers,
            json=payload,
            timeout=180
        )
        elapsed_time = time.time() - start_time

        if response.status_code != 200:
            return None, {"error": f"HTTP {response.status_code}: {response.text[:200]}"}

        result = response.json()
        choices = result.get("choices", [])
        if not choices:
            return None, {"error": "Empty choices in response"}

        message = choices[0].get("message", {})
        response_text = (message.get("content") or "").strip()

        if "<think>" in response_text and "</think>" in response_text:
            response_text = response_text.split("</think>")[-1].strip()

        if not response_text:
            return None, {"error": "Empty content after stripping thinking"}

        if voice_path and "voice_hurried_typos" in voice_path:
            response_text = apply_hurried_typos(response_text, seed_str=note_text[:120])

        usage = result.get("usage", {})
        actual_prompt_tokens = usage.get("prompt_tokens", prompt_tokens_est)
        actual_output_tokens = usage.get("completion_tokens", estimate_tokens(response_text))
        cached_tokens = usage.get("prompt_tokens_details", {}).get("cached_tokens", 0)

        metadata = {
            "model": model,
            "temperature": temperature,
            "encounterclass": encounterclass,
            "provider": provider_id,
            "template_used": template_path,
            "voice_used": voice_path,
            "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            "input_chars": input_chars,
            "input_tokens_estimate": input_tokens_est,
            "prompt_chars": prompt_chars,
            "prompt_tokens_estimate": prompt_tokens_est,
            "prompt_tokens_actual": actual_prompt_tokens,
            "cached_prompt_tokens": cached_tokens,
            "output_chars": len(response_text),
            "output_tokens_estimate": estimate_tokens(response_text),
            "output_tokens_actual": actual_output_tokens,
            "elapsed_time_seconds": round(elapsed_time, 3),
            "tokens_per_second": round(actual_output_tokens / elapsed_time, 1) if elapsed_time > 0 else 0.0
        }
        return response_text, metadata

    except Exception as e:
        return None, {"error": str(e)}


def transform_notes_pass2(
    notes_parquet: str,
    output_parquet: str,
    base_url: str,
    api_key: str,
    model: str,
    temperature: float,
    style_resolver: StyleResolver,
    limit: int = None,
    batch_size: int = 100,
    concurrency: int = 8,
    max_tokens: int = 1200
) -> Dict[str, Any]:
    """
    Pass 2: Incrementally transform notes with LLM using encounter-mapped templates,
    provider-mapped voices, prefix caching, and concurrent batching.
    """
    notes_file = Path(notes_parquet).resolve()  # Pin one immutable transfer snapshot.
    output_file = Path(output_parquet)
    parquet_dir = notes_file.parent
    if (output_file.parent.resolve() / "_transfers.json").exists():
        raise ValueError("Write LLM notes outside the immutable transfer snapshot")

    if not notes_file.exists():
        print(f"✗ Notes file not found: {notes_parquet}")
        return {"error": "notes file not found"}

    print(f"\n{'='*60}")
    print("PASS 2: TRANSFORMING NOTES WITH LLM")
    print(f"{'='*60}")

    con = duckdb.connect(':memory:')
    con.execute("SET TimeZone='UTC'")

    print(f"\nLoading notes from {notes_file}...")
    con.execute(f"""
        CREATE TABLE notes AS
        SELECT * FROM read_parquet('{notes_file}')
    """)
    _ensure_note_identity(con, "notes")
    total_notes = con.execute("SELECT COUNT(*) FROM notes").fetchone()[0]
    print(f"✓ Loaded {total_notes:,} notes")

    # Join encounterclass, provider metadata, encounter reason, and real-time observations
    # (vital-signs, exam findings, and waiting-room screening total scores)
    encounters_file = parquet_dir / "encounters.parquet"
    providers_file = parquet_dir / "providers.parquet"
    organizations_file = parquet_dir / "organizations.parquet"
    observations_file = parquet_dir / "observations.parquet"
    codes_file = parquet_dir / "codes.parquet"

    if encounters_file.exists():
        con.execute(f"""
            CREATE TABLE encounters AS
            SELECT id, encounterclass, provider, CAST(code AS VARCHAR) AS code, CAST(reasoncode AS VARCHAR) AS reasoncode
            FROM read_parquet('{encounters_file}')
        """)
        if codes_file.exists():
            con.execute(f"CREATE TABLE codes AS SELECT code, vocab, description FROM read_parquet('{codes_file}')")
        else:
            con.execute("CREATE TABLE codes (code VARCHAR, vocab VARCHAR, description VARCHAR)")

        if observations_file.exists() and codes_file.exists():
            print("Enriching notes with encounter Vital Signs, Exam Findings, and Screening Scores...")
            con.execute(f"""
                CREATE TABLE obs_summary AS
                SELECT
                    ob.encounter,
                    STRING_AGG(
                        c.description || ': ' || ob.value ||
                        CASE
                            WHEN ob.units IS NULL OR ob.units IN ('{{nominal}}', '{{score}}', '{{ratio}}', '{{logmar}}') THEN ''
                            WHEN ob.units = 'mm[Hg]' THEN ' mmHg'
                            ELSE ' ' || ob.units
                        END,
                        '; ' ORDER BY c.description
                    ) FILTER (WHERE ob.category = 'vital-signs') AS vitals_summary,
                    STRING_AGG(
                        c.description || ': ' || ob.value ||
                        CASE
                            WHEN ob.units IS NULL OR ob.units IN ('{{nominal}}', '{{score}}', '{{ratio}}', '{{logmar}}') THEN ''
                            ELSE ' ' || ob.units
                        END,
                        '; ' ORDER BY c.description
                    ) FILTER (WHERE ob.category = 'exam' AND c.description NOT ILIKE '%certificate of death%') AS exam_summary,
                    STRING_AGG(
                        c.description || ': ' || ob.value,
                        '; ' ORDER BY c.description
                    ) FILTER (WHERE ob.category = 'survey' AND c.description ILIKE '%score%') AS screening_summary
                FROM read_parquet('{observations_file}') ob
                JOIN codes c
                  ON CAST(ob.code AS VARCHAR) = c.code AND c.vocab = 'LOINC'
                WHERE ob.category IN ('vital-signs', 'exam')
                   OR (ob.category = 'survey' AND c.description ILIKE '%score%')
                GROUP BY ob.encounter
            """)
        else:
            con.execute("""
                CREATE TABLE obs_summary (
                    encounter VARCHAR,
                    vitals_summary VARCHAR,
                    exam_summary VARCHAR,
                    screening_summary VARCHAR
                )
            """)

        if providers_file.exists() and organizations_file.exists():
            con.execute(f"CREATE TABLE providers AS SELECT id, CAST(npi AS VARCHAR) AS npi, organization FROM read_parquet('{providers_file}')")
            con.execute(f"CREATE TABLE organizations AS SELECT id, name FROM read_parquet('{organizations_file}')")
        else:
            con.execute("CREATE TABLE providers (id VARCHAR, npi VARCHAR, organization VARCHAR)")
            con.execute("CREATE TABLE organizations (id VARCHAR, name VARCHAR)")

        con.execute("""
            CREATE TABLE notes_enriched AS
            SELECT
                n.patient,
                n.date,
                n.note_text,
                n.note_text || CASE WHEN n.transfer_id IS NOT NULL THEN '' ELSE
                    COALESCE('\n\n# Encounter Reason\n' || c_reas.description, '')
                    || COALESCE('\n\n# Vital Signs\n' || os.vitals_summary, '')
                    || COALESCE('\n\n# Exam Findings\n' || os.exam_summary, '')
                    || COALESCE('\n\n# Screening Scores\n' || os.screening_summary, '') END
                    AS prompt_note_text,
                n.encounter,
                n.note_id, n.note_type, n.authored_at, n.author, n.transfer_id,
                e.encounterclass,
                e.provider AS provider_id,
                pr.npi AS provider_npi,
                o.name AS organization_name
            FROM notes n
            LEFT JOIN encounters e ON n.encounter = e.id
            LEFT JOIN providers pr ON e.provider = pr.id
            LEFT JOIN organizations o ON pr.organization = o.id
            LEFT JOIN codes c_reas ON e.reasoncode = c_reas.code AND c_reas.vocab = 'SNOMED'
            LEFT JOIN obs_summary os ON n.encounter = os.encounter
        """)
    else:
        con.execute("""
            CREATE TABLE notes_enriched AS
            SELECT
                patient,
                date,
                note_text,
                note_text AS prompt_note_text,
                encounter,
                note_id, note_type, authored_at, author, transfer_id,
                NULL::VARCHAR AS encounterclass,
                NULL::VARCHAR AS provider_id,
                NULL::VARCHAR AS provider_npi,
                NULL::VARCHAR AS organization_name
            FROM notes
        """)

    if output_file.exists():
        print(f"\nLoading existing LLM notes from {output_file}...")
        con.execute(f"""
            CREATE TABLE notes_llm AS
            SELECT * FROM read_parquet('{output_file}')
        """)
        con.execute("ALTER TABLE notes_llm ALTER COLUMN date TYPE TIMESTAMPTZ")
        _ensure_note_identity(con, "notes_llm", legacy=True)
        existing_count = con.execute("SELECT COUNT(*) FROM notes_llm").fetchone()[0]
        print(f"✓ Found {existing_count:,} existing LLM notes")

        con.execute("""
            CREATE TABLE notes_to_process AS
            SELECT ne.*
            FROM notes_enriched ne
            LEFT JOIN notes_llm llm
              ON ne.note_id = llm.note_id
             AND ne.note_text = llm.note_text
            WHERE llm.patient IS NULL
        """)
    else:
        print("\nNo existing LLM notes found, will process from start")
        con.execute("""
            CREATE TABLE notes_to_process AS
            SELECT * FROM notes_enriched
        """)

    # Assign 100-patient cohorts so complete patients finish every ~100 patients
    # while still grouping by (provider_id, encounterclass) within each cohort for KV prefix caching
    con.execute("""
        CREATE TABLE notes_cohorted AS
        SELECT
            *,
            (DENSE_RANK() OVER (ORDER BY patient) - 1) // 100 AS patient_cohort
        FROM notes_to_process
    """)
    con.execute("DROP TABLE notes_to_process")
    con.execute("ALTER TABLE notes_cohorted RENAME TO notes_to_process")

    if limit:
        con.execute(f"""
            CREATE TABLE notes_to_process_limited AS
            SELECT * FROM notes_to_process
            ORDER BY patient_cohort, provider_id, encounterclass, patient, date
            LIMIT {limit}
        """)
        con.execute("DROP TABLE notes_to_process")
        con.execute("ALTER TABLE notes_to_process_limited RENAME TO notes_to_process")

    pending_count = con.execute("SELECT COUNT(*) FROM notes_to_process").fetchone()[0]
    print(f"\nNotes to process: {pending_count:,}")

    if pending_count == 0:
        print("✓ All requested notes already processed")
        con.close()
        return {
            "total_notes": total_notes,
            "pending_notes": 0,
            "transformed": 0
        }

    notes_to_transform = con.execute("""
        SELECT
            patient,
            CAST(date AS VARCHAR) AS date_str,
            note_text,
            prompt_note_text,
            encounter,
            encounterclass,
            provider_id,
            provider_npi,
            organization_name,
            note_id, note_type, authored_at, author, transfer_id
        FROM notes_to_process
        ORDER BY patient_cohort, provider_id, encounterclass, patient, date
    """).fetchall()

    print(f"\nTransforming {len(notes_to_transform):,} notes...")
    print(f"Endpoint:    {base_url}")
    print(f"Model:       {model}")
    print(f"Concurrency: {concurrency} parallel requests")
    print(f"Temperature: {temperature}")

    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=concurrency * 2,
        pool_maxsize=concurrency * 2
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    wall_start = time.time()
    total_transformed = 0
    total_failed = 0
    total_input_tokens = 0
    total_output_tokens = 0
    sum_request_latency = 0.0
    transformed_records = []

    def _worker(idx_and_row):
        idx, row = idx_and_row
        patient, date_str, note_text, prompt_note_text, encounter, encounterclass, provider_id, provider_npi, org_name, *identity = row
        t_path, v_path, system_prompt = style_resolver.resolve(
            encounterclass=encounterclass,
            provider_id=provider_id,
            provider_npi=provider_npi,
            org_name=org_name
        )
        natural_text, metadata = transform_note_with_llm(
            session=session,
            base_url=base_url,
            api_key=api_key,
            note_text=prompt_note_text,
            system_prompt=system_prompt,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            template_path=t_path,
            voice_path=v_path,
            encounterclass=encounterclass,
            provider_id=provider_id
        )
        return idx, patient, date_str, note_text, encounter, natural_text, metadata, identity

    def _handle_result(patient, date_str, note_text, encounter, natural_text, metadata, identity):
        nonlocal total_transformed, total_failed, total_input_tokens, total_output_tokens, sum_request_latency, transformed_records
        wall_elapsed = time.time() - wall_start

        if natural_text and metadata and "error" not in metadata:
            transformed_records.append((
                patient,
                date_str,
                encounter,
                note_text,
                natural_text,
                json.dumps(metadata),
                *identity
            ))
            total_transformed += 1
            in_tok = metadata.get("prompt_tokens_actual", 0)
            cached_tok = metadata.get("cached_prompt_tokens", 0)
            out_tok = metadata.get("output_tokens_actual", 0)
            req_time = metadata.get("elapsed_time_seconds", 0.0)
            ec = metadata.get("encounterclass") or "unknown"
            v_used = Path(metadata["voice_used"]).stem if metadata.get("voice_used") else "default"
            total_input_tokens += in_tok
            total_output_tokens += out_tok
            sum_request_latency += req_time

            eff_out_tps = total_output_tokens / wall_elapsed if wall_elapsed > 0 else 0.0
            eff_notes_ps = total_transformed / wall_elapsed if wall_elapsed > 0 else 0.0
            cache_str = f" (cached {cached_tok})" if cached_tok else ""
            print(
                f"  [{total_transformed + total_failed}/{len(notes_to_transform)}] "
                f"{patient[:8]}... ({ec}/{v_used}) ✓ "
                f"{out_tok} out_tok{cache_str} in {req_time:.2f}s | "
                f"Net: {eff_out_tps:.1f} out_tok/s ({eff_notes_ps:.2f} notes/s)"
            )
        else:
            total_failed += 1
            err_msg = metadata.get("error", "Unknown error") if metadata else "Unknown error"
            print(f"  [{total_transformed + total_failed}/{len(notes_to_transform)}] {patient[:8]}... ✗ ({err_msg})")

    # Process in chunks of batch_size to keep memory bounded on 700k+ note datasets
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        for chunk_start in range(0, len(notes_to_transform), batch_size):
            chunk = notes_to_transform[chunk_start:chunk_start + batch_size]
            if not chunk:
                break

            # Prime prefix cache on first note of first chunk if running concurrent workers
            if chunk_start == 0 and len(chunk) > 1 and concurrency > 1:
                _, patient, date_str, note_text, encounter, natural_text, metadata, identity = _worker((1, chunk[0]))
                _handle_result(patient, date_str, note_text, encounter, natural_text, metadata, identity)
                chunk = chunk[1:]
                base_idx = 2
            else:
                base_idx = chunk_start + 1

            futures = [
                executor.submit(_worker, (i, row))
                for i, row in enumerate(chunk, base_idx)
            ]
            for future in as_completed(futures):
                _, patient, date_str, note_text, encounter, natural_text, metadata, identity = future.result()
                _handle_result(patient, date_str, note_text, encounter, natural_text, metadata, identity)

            if transformed_records:
                _write_batch_to_parquet(con, transformed_records, output_file)
                transformed_records = []

    wall_total = time.time() - wall_start
    con.close()

    return {
        "total_notes": total_notes,
        "pending_notes": pending_count,
        "transformed": total_transformed,
        "failed": total_failed,
        "total_input_tokens": total_input_tokens,
        "total_output_tokens": total_output_tokens,
        "wall_elapsed_time": wall_total,
        "sum_request_latency": sum_request_latency
    }


def _ensure_note_identity(con, table: str, legacy: bool = False):
    """Keep multiple notes per encounter distinct, including same-day transfer notes."""
    fields = {"note_id": "VARCHAR", "note_type": "VARCHAR", "authored_at": "TIMESTAMPTZ",
              "author": "VARCHAR", "transfer_id": "VARCHAR"}
    for name, kind in fields.items():
        con.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {name} {kind}")
    if legacy:
        # Old rewritten dates were truncated to DATE. Reuse only a unique exact-text
        # match; ambiguous/stale rows must be rewritten rather than hide a new note.
        con.execute(f"""
            UPDATE {table} l SET note_id = n.note_id, date = n.date,
                note_type = n.note_type, authored_at = n.authored_at, author = n.author,
                transfer_id = n.transfer_id
            FROM (SELECT *, count(*) OVER (PARTITION BY patient, encounter, note_text) AS matches
                  FROM notes) n
            WHERE l.note_id IS NULL AND n.matches = 1 AND l.patient = n.patient
              AND l.encounter = n.encounter AND l.note_text = n.note_text
        """)
    else:
        assign_note_ids(con, table)
        con.execute(f"""UPDATE {table} SET
            note_type = COALESCE(note_type, 'source'), authored_at = COALESCE(authored_at, date)""")


def _write_batch_to_parquet(con, records: List[Tuple], output_file: Path):
    """Atomically checkpoint by note identity without discarding timestamp precision."""
    if not records:
        return
    con.execute("DROP TABLE IF EXISTS temp_batch")
    con.execute("""CREATE TABLE temp_batch (
        patient VARCHAR, date TIMESTAMPTZ, encounter VARCHAR, note_text VARCHAR,
        natural_note_text VARCHAR, llm_metadata VARCHAR, note_id VARCHAR, note_type VARCHAR,
        authored_at TIMESTAMPTZ, author VARCHAR, transfer_id VARCHAR)""")
    con.executemany("INSERT INTO temp_batch VALUES (?, CAST(? AS TIMESTAMPTZ), ?, ?, ?, ?, ?, ?, ?, ?, ?)", records)
    if output_file.exists():
        con.execute("CREATE OR REPLACE TABLE existing_notes AS SELECT * FROM read_parquet(?)", [str(output_file)])
        con.execute("ALTER TABLE existing_notes ALTER COLUMN date TYPE TIMESTAMPTZ")
        _ensure_note_identity(con, "existing_notes", legacy=True)
        con.execute("""INSERT INTO temp_batch BY NAME
            SELECT e.* FROM existing_notes e
            WHERE e.note_id NOT IN (SELECT note_id FROM temp_batch)
              AND EXISTS (SELECT 1 FROM notes n WHERE n.note_id = e.note_id AND n.note_text = e.note_text)""")
        con.execute("DROP TABLE existing_notes")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_file = output_file.parent / f".tmp_{output_file.name}"
    escaped = str(tmp_file).replace("'", "''")
    con.execute(f"COPY (SELECT * FROM temp_batch ORDER BY patient, date, note_id) TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    tmp_file.replace(output_file)
    total = con.execute("SELECT count(*) FROM temp_batch").fetchone()[0]
    print(f"    Checkpointed {len(records)} notes; {total} current notes in {output_file.name}", flush=True)


def main():
    parser = ArgumentParser(
        prog='process_notes.py',
        description='Process Synthea notes in two passes: parse → transform (oMLX / OpenAI-compatible API)'
    )

    parser.add_argument(
        'mode',
        choices=['parse', 'transform'],
        help='parse: Convert text files to notes.parquet | transform: Rewrite notes via LLM into notes_llm.parquet'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='parse: limit patient files | transform: limit notes to process'
    )

    # Parse mode arguments
    parser.add_argument(
        '--notes_dir',
        default='./output/notes',
        help='[parse] Directory containing patient note text files (default: ./output/notes)'
    )
    parser.add_argument(
        '--parquet_dir',
        default='./output/parquet',
        help='[parse] Directory containing Parquet files with encounters (default: ./output/parquet)'
    )
    parser.add_argument(
        '--output',
        default='./output/parquet/notes.parquet',
        help='[parse] Output path for notes.parquet (default: ./output/parquet/notes.parquet)'
    )

    # Transform mode arguments
    parser.add_argument(
        '--notes_parquet',
        default='./output/parquet/notes.parquet',
        help='[transform] Input notes.parquet file (default: ./output/parquet/notes.parquet)'
    )
    parser.add_argument(
        '--output_parquet',
        default='./output/parquet/notes_llm.parquet',
        help='[transform] Output notes_llm.parquet file (default: ./output/parquet/notes_llm.parquet)'
    )
    parser.add_argument(
        '--base_url',
        default=os.environ.get('OMLX_BASE_URL', 'http://localhost:8081/v1'),
        help='[transform] OpenAI-compatible API base URL (default: http://localhost:8081/v1 for oMLX)'
    )
    parser.add_argument(
        '--api_key',
        default=None,
        help='[transform] API key (auto-detected from ~/.omlx/settings.json or OMLX_API_KEY / OPENAI_API_KEY)'
    )
    parser.add_argument(
        '--model',
        default='Qwen3-4B-Instruct-2507-4bit',
        help='[transform] Model ID served by oMLX / remote server (default: Qwen3-4B-Instruct-2507-4bit)'
    )
    parser.add_argument(
        '--concurrency', '-c',
        type=int,
        default=8,
        help='[transform] Number of concurrent requests for continuous batching (default: 8)'
    )
    parser.add_argument(
        '--temperature',
        type=float,
        default=0.3,
        help='[transform] LLM temperature 0.0-1.0 (default: 0.3)'
    )
    parser.add_argument(
        '--max_tokens',
        type=int,
        default=1200,
        help='[transform] Max completion tokens per note (default: 1200)'
    )
    parser.add_argument(
        '--style_map',
        default='scripts/templates/style_mapping.json',
        help='[transform] JSON file mapping encounterclass -> template and provider -> voice (default: scripts/templates/style_mapping.json)'
    )
    parser.add_argument(
        '--template',
        default=None,
        help='[transform] Optional override: use a single note template file for all notes'
    )
    parser.add_argument(
        '--voice',
        default=None,
        help='[transform] Optional override: use a single voice example file for all notes'
    )
    parser.add_argument(
        '--batch_size',
        type=int,
        default=100,
        help='[transform] Number of notes to process before checkpointing to Parquet (default: 100)'
    )

    args = parser.parse_args()

    if args.mode == 'parse':
        results = parse_notes_pass1(
            notes_dir=args.notes_dir,
            parquet_dir=args.parquet_dir,
            output_path=args.output,
            limit=args.limit
        )
        if "error" not in results:
            print(f"\n{'='*60}")
            print("SUMMARY")
            print(f"{'='*60}")
            print(f"Patient files processed: {results['total_files']}")
            print(f"Total notes parsed:      {results['total_notes']}")
            print(f"Matched to encounters:   {results['matched_encounters']}")
            print(f"Unmatched:               {results['unmatched_encounters']}")
            print(f"Output:                  {args.output}")

    elif args.mode == 'transform':
        if args.template and not Path(args.template).exists():
            print(f"✗ Template file not found: {args.template}")
            return
        if args.voice and not Path(args.voice).exists():
            print(f"✗ Voice file not found: {args.voice}")
            return

        style_resolver = StyleResolver(
            style_map_path=args.style_map,
            override_template_path=args.template,
            override_voice_path=args.voice
        )
        if args.style_map and Path(args.style_map).exists() and not (args.template and args.voice):
            print(f"✓ Loaded encounter-template & provider-voice mapping: {args.style_map}")
        if args.template:
            print(f"✓ Template override: {args.template}")
        if args.voice:
            print(f"✓ Voice override:    {args.voice}")

        api_key = resolve_api_key(args.api_key)
        print(f"\nChecking LLM server availability at {args.base_url}...")
        if not check_llm_server_available(args.base_url, args.model, api_key):
            return

        results = transform_notes_pass2(
            notes_parquet=args.notes_parquet,
            output_parquet=args.output_parquet,
            base_url=args.base_url,
            api_key=api_key,
            model=args.model,
            temperature=args.temperature,
            style_resolver=style_resolver,
            limit=args.limit,
            batch_size=args.batch_size,
            concurrency=args.concurrency,
            max_tokens=args.max_tokens
        )

        if "error" not in results:
            print(f"\n{'='*60}")
            print("SUMMARY")
            print(f"{'='*60}")
            print(f"Total notes in source: {results['total_notes']:,}")
            print(f"Pending notes:         {results['pending_notes']:,}")
            print(f"Transformed:           {results['transformed']:,}")
            if results.get("failed"):
                print(f"Failed:                {results['failed']:,}")

            if results['transformed'] > 0:
                wall_time = results['wall_elapsed_time']
                in_tok = results['total_input_tokens']
                out_tok = results['total_output_tokens']
                tot_tok = in_tok + out_tok
                print(f"Total input tokens:    {in_tok:,}")
                print(f"Total output tokens:   {out_tok:,}")
                print(f"Total tokens:          {tot_tok:,}")
                print(f"Wall-clock time:       {wall_time:.2f}s")
                print(f"Effective time/note:   {wall_time / results['transformed']:.2f}s")
                print(f"Throughput (notes/s):  {results['transformed'] / wall_time:.2f} notes/s")
                print(f"Throughput (out tok):  {out_tok / wall_time:.1f} tok/s")
                print(f"Throughput (tot tok):  {tot_tok / wall_time:.1f} tok/s")

            print(f"Output:                {args.output_parquet}")


if __name__ == '__main__':
    main()
