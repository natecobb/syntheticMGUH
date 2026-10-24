#!/usr/bin/env python3
"""
Process Synthea clinical notes in two passes:

Pass 1 (parse):
  - Parse text files into structured format
  - Associate notes with encounters using patient_id and date
  - Output to notes.parquet
  (Note: 3_convert_to_parquet.py now performs Pass 1 automatically during Parquet conversion.)

Pass 2 (transform):
  - Read notes.parquet
  - Incrementally transform notes with an OpenAI-compatible LLM server (oMLX, vLLM, SGLang, Ollama)
  - Uses system-prompt prefix caching and concurrent request batching for high throughput
  - Only processes notes not already in notes_llm.parquet
  - Appends batches to notes_llm.parquet
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
    Build the static system prompt once so oMLX / vLLM can KV-cache the prefix across all notes.
    """
    parts = [
        "Transform the structured clinical note provided by the user into a natural physician's note. "
        "Write ONLY the clinical note, with no preamble, commentary, or markdown code fences."
    ]

    if template:
        parts.append(f"\nUse this note template/structure:\n{template.strip()}")

    if voice_example:
        parts.append(f"\nMatch the style and voice of this physician note:\n{voice_example.strip()}")

    parts.append(
        "\nGuidelines:\n"
        "- Maintain ONLY the clinical facts explicitly provided; do NOT invent exam findings, vitals, or history\n"
        "- Remove markdown headers (# symbols)\n"
        "- Strictly match the length, brevity, and abbreviation density of the voice example (if provided)\n"
        "- Add the occasional realistic clinical abbreviation or minor typo if consistent with the voice"
    )
    return "\n".join(parts)


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
    voice_path: str = None
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

        # Strip <think>...</think> blocks if any model emitted reasoning inline
        if "<think>" in response_text and "</think>" in response_text:
            response_text = response_text.split("</think>")[-1].strip()

        if not response_text:
            return None, {"error": "Empty content after stripping thinking"}

        usage = result.get("usage", {})
        actual_prompt_tokens = usage.get("prompt_tokens", prompt_tokens_est)
        actual_output_tokens = usage.get("completion_tokens", estimate_tokens(response_text))
        cached_tokens = usage.get("prompt_tokens_details", {}).get("cached_tokens", 0)

        metadata = {
            "model": model,
            "temperature": temperature,
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
    template: str = None,
    voice_example: str = None,
    template_path: str = None,
    voice_path: str = None,
    limit: int = None,
    batch_size: int = 100,
    concurrency: int = 8,
    max_tokens: int = 1200
) -> Dict[str, Any]:
    """
    Pass 2: Incrementally transform notes with LLM using concurrent batching and prefix caching.
    """
    notes_file = Path(notes_parquet)
    output_file = Path(output_parquet)

    if not notes_file.exists():
        print(f"✗ Notes file not found: {notes_parquet}")
        return {"error": "notes file not found"}

    print(f"\n{'='*60}")
    print("PASS 2: TRANSFORMING NOTES WITH LLM")
    print(f"{'='*60}")

    con = duckdb.connect(':memory:')

    print(f"\nLoading notes from {notes_file}...")
    con.execute(f"""
        CREATE TABLE notes AS
        SELECT * FROM read_parquet('{notes_file}')
    """)
    total_notes = con.execute("SELECT COUNT(*) FROM notes").fetchone()[0]
    print(f"✓ Loaded {total_notes:,} notes")

    if output_file.exists():
        print(f"\nLoading existing LLM notes from {output_file}...")
        con.execute(f"""
            CREATE TABLE notes_llm AS
            SELECT * FROM read_parquet('{output_file}')
        """)
        existing_count = con.execute("SELECT COUNT(*) FROM notes_llm").fetchone()[0]
        print(f"✓ Found {existing_count:,} existing LLM notes")

        con.execute("""
            CREATE TABLE notes_to_process AS
            SELECT n.*
            FROM notes n
            LEFT JOIN notes_llm llm
              ON n.encounter = llm.encounter
             AND n.patient = llm.patient
            WHERE llm.patient IS NULL
        """)
    else:
        print("\nNo existing LLM notes found, will process from start")
        con.execute("""
            CREATE TABLE notes_to_process AS
            SELECT * FROM notes
        """)

    if limit:
        con.execute(f"""
            CREATE TABLE notes_to_process_limited AS
            SELECT * FROM notes_to_process
            ORDER BY patient, date
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
        SELECT patient, CAST(date AS VARCHAR) AS date_str, note_text, encounter
        FROM notes_to_process
        ORDER BY patient, date
    """).fetchall()

    system_prompt = build_system_prompt(template=template, voice_example=voice_example)

    print(f"\nTransforming {len(notes_to_transform):,} notes...")
    print(f"Endpoint:    {base_url}")
    print(f"Model:       {model}")
    print(f"Concurrency: {concurrency} parallel requests")
    print(f"Temperature: {temperature}")
    if template_path:
        print(f"Template:    {template_path}")
    if voice_path:
        print(f"Voice:       {voice_path}")

    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=concurrency * 2,
        pool_maxsize=concurrency * 2
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Warm up prefix cache with the first note if running concurrent workers
    wall_start = time.time()
    total_transformed = 0
    total_failed = 0
    total_input_tokens = 0
    total_output_tokens = 0
    sum_request_latency = 0.0
    transformed_records = []

    def _worker(idx_and_row):
        idx, (patient, date_str, note_text, encounter) = idx_and_row
        natural_text, metadata = transform_note_with_llm(
            session=session,
            base_url=base_url,
            api_key=api_key,
            note_text=note_text,
            system_prompt=system_prompt,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            template_path=template_path,
            voice_path=voice_path
        )
        return idx, patient, date_str, note_text, encounter, natural_text, metadata

    def _handle_result(patient, date_str, note_text, encounter, natural_text, metadata):
        nonlocal total_transformed, total_failed, total_input_tokens, total_output_tokens, sum_request_latency, transformed_records
        wall_elapsed = time.time() - wall_start

        if natural_text and metadata and "error" not in metadata:
            transformed_records.append((
                patient,
                date_str,
                encounter,
                note_text,
                natural_text,
                json.dumps(metadata)
            ))
            total_transformed += 1
            in_tok = metadata.get("prompt_tokens_actual", 0)
            cached_tok = metadata.get("cached_prompt_tokens", 0)
            out_tok = metadata.get("output_tokens_actual", 0)
            req_time = metadata.get("elapsed_time_seconds", 0.0)
            total_input_tokens += in_tok
            total_output_tokens += out_tok
            sum_request_latency += req_time

            eff_out_tps = total_output_tokens / wall_elapsed if wall_elapsed > 0 else 0.0
            eff_notes_ps = total_transformed / wall_elapsed if wall_elapsed > 0 else 0.0
            cache_str = f" (cached {cached_tok})" if cached_tok else ""
            print(
                f"  [{total_transformed + total_failed}/{len(notes_to_transform)}] "
                f"{patient[:8]}... {date_str} ✓ "
                f"{out_tok} out_tok{cache_str} in {req_time:.2f}s | "
                f"Net: {eff_out_tps:.1f} out_tok/s ({eff_notes_ps:.2f} notes/s)"
            )

            if len(transformed_records) >= batch_size:
                _write_batch_to_parquet(con, transformed_records, output_file)
                transformed_records = []
        else:
            total_failed += 1
            err_msg = metadata.get("error", "Unknown error") if metadata else "Unknown error"
            print(f"  [{total_transformed + total_failed}/{len(notes_to_transform)}] {patient[:8]}... ✗ ({err_msg})")

    # Prime the KV prefix cache with the first note before fanning out concurrent workers
    remaining_notes = notes_to_transform
    if len(notes_to_transform) > 1 and concurrency > 1:
        _, patient, date_str, note_text, encounter, natural_text, metadata = _worker((1, notes_to_transform[0]))
        _handle_result(patient, date_str, note_text, encounter, natural_text, metadata)
        remaining_notes = notes_to_transform[1:]

    if remaining_notes:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [
                executor.submit(_worker, (i, row))
                for i, row in enumerate(remaining_notes, 2 if len(notes_to_transform) > len(remaining_notes) else 1)
            ]

            for future in as_completed(futures):
                _, patient, date_str, note_text, encounter, natural_text, metadata = future.result()
                _handle_result(patient, date_str, note_text, encounter, natural_text, metadata)

    if transformed_records:
        _write_batch_to_parquet(con, transformed_records, output_file)

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


def _write_batch_to_parquet(con, records: List[Tuple], output_file: Path):
    """Vectorized append of transformed note records to Parquet."""
    if not records:
        return

    con.execute("DROP TABLE IF EXISTS temp_batch")
    con.execute("""
        CREATE TABLE temp_batch (
            patient VARCHAR,
            date DATE,
            encounter VARCHAR,
            note_text VARCHAR,
            natural_note_text VARCHAR,
            llm_metadata VARCHAR
        )
    """)

    con.executemany(
        "INSERT INTO temp_batch VALUES (?, CAST(? AS DATE), ?, ?, ?, ?)",
        records
    )

    if output_file.exists():
        con.execute(f"""
            CREATE TABLE existing_notes AS
            SELECT
                patient,
                CAST(date AS DATE) AS date,
                encounter,
                note_text,
                natural_note_text,
                llm_metadata
            FROM read_parquet('{output_file}')
        """)
        con.execute(f"""
            COPY (
                SELECT * FROM existing_notes
                UNION ALL
                SELECT * FROM temp_batch
                ORDER BY patient, date
            )
            TO '{output_file}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        con.execute("DROP TABLE existing_notes")
    else:
        con.execute(f"""
            COPY (SELECT * FROM temp_batch ORDER BY patient, date)
            TO '{output_file}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

    print(f"    ✓ Checkpointed batch of {len(records)} notes to {output_file.name}")
    con.execute("DROP TABLE temp_batch")


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
        default='Qwen3.6-35B-A3B-oQ6-mtp',
        help='[transform] Model ID served by oMLX / remote server (default: Qwen3.6-35B-A3B-oQ6-mtp)'
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
        '--template',
        default=None,
        help='[transform] Path to note template file (e.g. scripts/templates/template_soap.txt)'
    )
    parser.add_argument(
        '--voice',
        default=None,
        help='[transform] Path to voice example file (e.g. scripts/templates/voice_terse.txt)'
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
        template = None
        if args.template:
            template_path = Path(args.template)
            if not template_path.exists():
                print(f"✗ Template file not found: {args.template}")
                return
            template = template_path.read_text(encoding='utf-8')
            print(f"✓ Loaded template: {args.template}")

        voice_example = None
        if args.voice:
            voice_path = Path(args.voice)
            if not voice_path.exists():
                print(f"✗ Voice file not found: {args.voice}")
                return
            voice_example = voice_path.read_text(encoding='utf-8')
            print(f"✓ Loaded voice example: {args.voice}")

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
            template=template,
            voice_example=voice_example,
            template_path=args.template,
            voice_path=args.voice,
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
