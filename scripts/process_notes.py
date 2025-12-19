#!/usr/bin/env python3
"""
Process Synthea clinical notes in two passes:

Pass 1 (parse):
  - Parse text files into structured format
  - Associate notes with encounters using patient_id and date
  - Output to notes.parquet

Pass 2 (transform):
  - Read notes.parquet
  - Incrementally transform notes with LLM
  - Only process notes not already in notes_llm.parquet
  - Append to notes_llm.parquet
"""

import os
import re
import json
import requests
import time
import duckdb
import multiprocessing
from pathlib import Path
from argparse import ArgumentParser
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timezone
from dateutil import parser as date_parser
from concurrent.futures import ProcessPoolExecutor, as_completed


def estimate_tokens(text: str) -> int:
    """
    Estimate token count for text.
    Uses a simple heuristic: ~4 characters per token (common for English text).
    This is a rough approximation - actual tokenization varies by model.
    """
    if not text:
        return 0
    # Average of ~4 chars per token for English text
    # Account for whitespace and punctuation
    return len(text) // 4


def check_ollama_available(model: str) -> bool:
    """Check if Ollama is running and model is available."""
    try:
        response = requests.get("http://localhost:11434/api/tags")
        if response.status_code == 200:
            models = response.json().get("models", [])
            model_names = [m.get("name") for m in models]

            if model in model_names:
                print(f"✓ Model '{model}' is available")
                return True
            else:
                print(f"✗ Model '{model}' not found")
                print(f"  Run: ollama pull {model}")
                return False
        return False
    except requests.exceptions.ConnectionError:
        print("✗ Ollama is not running")
        print("  Start Ollama service first")
        return False


def parse_notes_pass1(
    notes_dir: str,
    parquet_dir: str,
    output_path: str,
    limit: int = None
) -> Dict[str, Any]:
    """
    Pass 1: Parse all note text files and create notes.parquet with encounter associations.

    Args:
        notes_dir: Directory containing patient note text files
        parquet_dir: Directory containing Parquet files (for encounters)
        output_path: Output path for notes.parquet
        limit: Limit number of patient files to process

    Returns:
        Dictionary with statistics
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

    # Get all .txt files
    note_files = sorted(notes_path.glob("*.txt"))

    if not note_files:
        print(f"✗ No .txt files found in {notes_dir}")
        return {"error": "no files found"}

    # Apply limit if specified
    if limit:
        note_files = note_files[:limit]

    # Detect CPU cores
    cpu_count = multiprocessing.cpu_count()

    print(f"\n{'='*60}")
    print("PASS 1: PARSING NOTES AND ASSOCIATING WITH ENCOUNTERS")
    print(f"{'='*60}")
    print(f"Processing {len(note_files)} patient files")
    print(f"Using {cpu_count} CPU cores for parallel processing")

    # Connect to DuckDB
    con = duckdb.connect(':memory:')

    # Load encounters table for matching
    encounters_file = parquet_path / "encounters.parquet"
    if not encounters_file.exists():
        print(f"✗ encounters.parquet not found in {parquet_dir}")
        return {"error": "encounters.parquet not found"}

    print(f"\nLoading encounters from {encounters_file}...")
    con.execute(f"""
        CREATE TABLE encounters AS
        SELECT * FROM read_parquet('{encounters_file}')
    """)

    encounter_count = con.execute("SELECT COUNT(*) FROM encounters").fetchone()[0]
    print(f"✓ Loaded {encounter_count:,} encounters")

    # Create notes table with timezone-aware timestamp
    con.execute("""
        CREATE TABLE notes (
            patient VARCHAR,
            date TIMESTAMPTZ,
            note_text VARCHAR,
            encounter VARCHAR
        )
    """)

    print(f"\nParsing notes from text files (parallel processing)...")
    total_notes = 0
    processed_count = 0

    # Parse files in parallel using ProcessPoolExecutor
    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        # Submit all parsing tasks
        future_to_file = {executor.submit(parse_notes_file_wrapper, note_file): note_file
                          for note_file in note_files}

        # Process completed tasks as they finish
        for future in as_completed(future_to_file):
            processed_count += 1
            note_file = future_to_file[future]

            try:
                filename, note_records = future.result()

                if note_records:
                    # Prepare batch data with timezone-aware timestamps
                    batch_data = []
                    for record in note_records:
                        # Parse the date string and format for DuckDB
                        note_date = date_parser.parse(record['date'])
                        note_date_str = note_date.strftime('%Y-%m-%d %H:%M:%S')
                        batch_data.append([
                            record['patient_id'],
                            note_date_str,
                            record['note_text']
                        ])

                    # Batch insert all records from this file
                    con.executemany("""
                        INSERT INTO notes (patient, date, note_text, encounter)
                        VALUES (?, ?::TIMESTAMP AT TIME ZONE 'America/New_York', ?, NULL)
                    """, batch_data)

                    total_notes += len(note_records)
                    print(f"  [{processed_count}/{len(note_files)}] {filename}... ✓ {len(note_records)} notes")
                else:
                    print(f"  [{processed_count}/{len(note_files)}] {filename}... ✗ No notes")

            except Exception as e:
                print(f"  [{processed_count}/{len(note_files)}] {filename}... ✗ Error: {str(e)}")

    print(f"\nTotal notes parsed: {total_notes:,}")

    # Associate notes with encounters based on patient_id and date (DATE only, not timestamp)
    # Match notes to encounters that span the note date (multi-day encounters)
    print("\nAssociating notes with encounters (matching by DATE, BETWEEN inclusive)...")
    con.execute("""
        UPDATE notes
        SET encounter = (
            SELECT e.id
            FROM encounters e
            WHERE e.patient = notes.patient
              AND CAST(notes.date AS DATE) BETWEEN CAST(e.start AS DATE) AND COALESCE(CAST(e.stop AS DATE), CAST(notes.date AS DATE))
            ORDER BY e.start DESC
            LIMIT 1
        )
    """)

    unmatched = con.execute("SELECT COUNT(*) FROM notes WHERE encounter IS NULL").fetchone()[0]
    matched = total_notes - unmatched
    print(f"  Deleting unmatched: {unmatched:,}")
    con.execute("DELETE FROM notes WHERE encounter IS NULL")

    # Export to Parquet
    print(f"\nExporting to {output_file}...")
    con.execute(f"""
        COPY notes
        TO '{output_file}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    file_size_mb = output_file.stat().st_size / (1024 * 1024)
    print(f"✓ Exported {matched:,} notes → {file_size_mb:.2f} MB")

    con.close()

    return {
        "total_files": len(note_files),
        "total_notes": total_notes,
        "matched_encounters": matched,
        "unmatched_encounters": unmatched
    }


def parse_notes_file(file_path: Path) -> List[Dict[str, str]]:
    """
    Parse a single patient notes file into individual note records.

    Returns:
        List of dictionaries with patient_id, patient_name, date, and note_text
    """
    # Extract patient ID from filename
    regex_id = r"(^.*_)(.*?)\.txt"
    file_name = os.path.basename(file_path)
    match = re.search(regex_id, file_name)

    if not match:
        print(f"Warning: Could not extract patient ID from {file_name}")
        return []

    patient_id = match.group(2)
    patient_name = match.group(1).rstrip('_').replace('_', ' ')

    # Read the entire file
    with open(file_path, 'r', encoding='utf-8') as f:
        all_notes = f.read()

    # Extract individual notes using regex
    regex_notes = r"([1-2][0-9]{3}-[0-9]{2}-[0-9]{2}.*?)\n\n([\s\S]*?)(?=\n\n\n\n|\Z)"
    notes = re.findall(regex_notes, all_notes)

    # Build list of note records
    note_records = []
    for date_str, note_text in notes:
        date_str = date_str.strip()
        note_text = note_text.strip()

        if note_text:
            note_records.append({
                "patient_id": patient_id,
                #"patient_name": patient_name,
                "date": date_str,
                "note_text": note_text
            })

    return note_records


def parse_notes_file_wrapper(note_file: Path) -> Tuple[str, List[Dict[str, str]]]:
    """
    Wrapper for parse_notes_file() that can be used with ProcessPoolExecutor.
    Returns tuple of (filename, note_records) for progress tracking.
    """
    records = parse_notes_file(note_file)
    return (note_file.name, records)


def transform_note_with_llm(
    note_text: str,
    model: str,
    temperature: float,
    template: str = None,
    voice_example: str = None,
    template_path: str = None,
    voice_path: str = None
) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """
    Transform structured note into natural clinical note using LLM.

    Returns:
        Tuple of (transformed_text, metadata_dict) or (None, None) on failure
    """
    # Build the prompt
    prompt_parts = ["Transform this structured clinical note into a natural physician's note. Write ONLY the clinical note."]

    if template:
        prompt_parts.append(f"\nUse this note template/structure:\n{template}")

    prompt_parts.append(f"\nMatch the style of this physician note:\n{voice_example}")

    prompt_parts.append("""
        Guidelines:
        - Maintain all clinical facts exactly as provided
        - Remove markdown headers (# symbols)
        - Add the occasional typo
        """)

    prompt_parts.append(f"\nStructured Note:\n{note_text}")
    prompt_parts.append("\nClinical Note:")

    prompt = "\n".join(prompt_parts)

    # Track input metrics
    input_chars = len(note_text)
    input_tokens = estimate_tokens(note_text)
    prompt_chars = len(prompt)
    prompt_tokens = estimate_tokens(prompt)

    # Start wall-clock timer
    start_time = time.time()

    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": temperature,
                    "num_predict": 2000
                }
            },
            timeout=120
        )

        if response.status_code == 200:
            # Calculate wall-clock elapsed time
            elapsed_time = time.time() - start_time

            result = response.json()
            response_text = result.get("response", "").strip()

            # Remove thinking tags if present
            if "<think>" in response_text and "</think>" in response_text:
                parts = response_text.split("</think>")
                if len(parts) > 1:
                    response_text = parts[-1].strip()

            # Track output metrics
            output_chars = len(response_text)
            output_tokens = estimate_tokens(response_text)

            # Get actual token counts from Ollama response if available
            actual_prompt_tokens = result.get("prompt_eval_count", prompt_tokens)
            actual_output_tokens = result.get("eval_count", output_tokens)

            # Build metadata
            metadata = {
                "model": model,
                "temperature": temperature,
                "template_used": template_path if template_path else None,
                "voice_used": voice_path if voice_path else None,
                "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                "input_chars": input_chars,
                "input_tokens_estimate": input_tokens,
                "prompt_chars": prompt_chars,
                "prompt_tokens_estimate": prompt_tokens,
                "prompt_tokens_actual": actual_prompt_tokens,
                "output_chars": output_chars,
                "output_tokens_estimate": output_tokens,
                "output_tokens_actual": actual_output_tokens,
                "elapsed_time_seconds": round(elapsed_time, 2),
                "total_duration_ms": result.get("total_duration", 0) // 1000000,  # Convert ns to ms
                "load_duration_ms": result.get("load_duration", 0) // 1000000,
                "eval_duration_ms": result.get("eval_duration", 0) // 1000000
            }

            return response_text, metadata
        else:
            return None, None

    except requests.exceptions.Timeout:
        return None, None
    except Exception:
        return None, None


def transform_notes_pass2(
    notes_parquet: str,
    output_parquet: str,
    model: str,
    temperature: float,
    template: str = None,
    voice_example: str = None,
    template_path: str = None,
    voice_path: str = None,
    limit: int = None,
    batch_size: int = 100
) -> Dict[str, Any]:
    """
    Pass 2: Incrementally transform notes with LLM.

    Reads notes.parquet and only processes notes not already in notes_llm.parquet.
    Appends results to notes_llm.parquet.

    Args:
        notes_parquet: Path to notes.parquet (input)
        output_parquet: Path to notes_llm.parquet (output)
        model: Ollama model name
        temperature: LLM temperature
        template: Optional note template
        voice_example: Optional voice example
        template_path: Path to template file (for metadata)
        voice_path: Path to voice file (for metadata)
        limit: Limit number of notes to transform
        batch_size: Number of notes to process before writing to Parquet

    Returns:
        Dictionary with statistics
    """
    notes_file = Path(notes_parquet)
    output_file = Path(output_parquet)

    if not notes_file.exists():
        print(f"✗ Notes file not found: {notes_parquet}")
        return {"error": "notes file not found"}

    print(f"\n{'='*60}")
    print("PASS 2: TRANSFORMING NOTES WITH LLM")
    print(f"{'='*60}")

    # Connect to DuckDB
    con = duckdb.connect(':memory:')

    # Load source notes
    print(f"\nLoading notes from {notes_file}...")
    con.execute(f"""
        CREATE TABLE notes AS
        SELECT * FROM read_parquet('{notes_file}')
    """)

    total_notes = con.execute("SELECT COUNT(*) FROM notes").fetchone()[0]
    print(f"✓ Loaded {total_notes:,} notes")

    # Load existing LLM notes if they exist
    if output_file.exists():
        print(f"\nLoading existing LLM notes from {output_file}...")
        con.execute(f"""
            CREATE TABLE notes_llm AS
            SELECT * FROM read_parquet('{output_file}')
        """)
        existing_count = con.execute("SELECT COUNT(*) FROM notes_llm").fetchone()[0]
        print(f"✓ Found {existing_count:,} existing LLM notes")

        # Filter to only unprocessed notes
        con.execute("""
            CREATE TABLE notes_to_process AS
            SELECT n.*
            FROM notes n
            LEFT JOIN notes_llm llm ON n.patient = llm.patient AND n.date = llm.date
            WHERE llm.patient IS NULL
        """)
    else:
        print("\nNo existing LLM notes found, will process all")
        con.execute("""
            CREATE TABLE notes_to_process AS
            SELECT * FROM notes
        """)

    # Apply limit if specified
    if limit:
        con.execute(f"""
            CREATE TABLE notes_to_process_limited AS
            SELECT * FROM notes_to_process
            LIMIT {limit}
        """)
        con.execute("DROP TABLE notes_to_process")
        con.execute("ALTER TABLE notes_to_process_limited RENAME TO notes_to_process")

    pending_count = con.execute("SELECT COUNT(*) FROM notes_to_process").fetchone()[0]
    print(f"\nNotes to process: {pending_count:,}")

    if pending_count == 0:
        print("✓ All notes already processed")
        con.close()
        return {
            "total_notes": total_notes,
            "pending_notes": 0,
            "transformed": 0
        }

    # Fetch notes to process
    notes_to_transform = con.execute("""
        SELECT patient, date, note_text, encounter
        FROM notes_to_process
        ORDER BY patient, date
    """).fetchall()

    print(f"\nTransforming {len(notes_to_transform)} notes...")
    print(f"Model: {model}, Temperature: {temperature}")
    if template_path:
        print(f"Template: {template_path}")
    if voice_path:
        print(f"Voice: {voice_path}")

    # Transform notes
    total_transformed = 0
    total_input_tokens = 0
    total_output_tokens = 0
    total_elapsed_time = 0.0
    transformed_records = []

    for i, (patient, date, note_text, encounter) in enumerate(notes_to_transform, 1):
        print(f"  [{i}/{len(notes_to_transform)}] {patient[:8]}... {date}...", end=" ")

        natural_text, metadata = transform_note_with_llm(
            note_text,
            model,
            temperature,
            template,
            voice_example,
            template_path,
            voice_path
        )

        if natural_text and metadata:
            # Store transformed note
            transformed_records.append({
                'patient': patient,
                'date': date,
                'encounter': encounter,
                'note_text': note_text,
                'natural_note_text': natural_text,
                'llm_metadata': json.dumps(metadata)
            })

            total_transformed += 1
            total_input_tokens += metadata.get('prompt_tokens_actual', 0)
            total_output_tokens += metadata.get('output_tokens_actual', 0)
            total_elapsed_time += metadata.get('elapsed_time_seconds', 0.0)
            print(f"✓ ({metadata.get('elapsed_time_seconds', 0):.1f}s)")

            # Write batch to Parquet
            if len(transformed_records) >= batch_size:
                _write_batch_to_parquet(con, transformed_records, output_file)
                transformed_records = []
        else:
            print("✗")

    # Write remaining records
    if transformed_records:
        _write_batch_to_parquet(con, transformed_records, output_file)

    con.close()

    return {
        "total_notes": total_notes,
        "pending_notes": pending_count,
        "transformed": total_transformed,
        "total_input_tokens": total_input_tokens,
        "total_output_tokens": total_output_tokens,
        "total_elapsed_time": total_elapsed_time
    }


def _write_batch_to_parquet(con, records: List[Dict], output_file: Path):
    """Helper to append records to Parquet file."""
    if not records:
        return

    # Create temporary table with new records
    con.execute("DROP TABLE IF EXISTS temp_batch")
    con.execute("""
        CREATE TABLE temp_batch (
            patient VARCHAR,
            date TIMESTAMP,
            encounter VARCHAR,
            note_text VARCHAR,
            natural_note_text VARCHAR,
            llm_metadata VARCHAR
        )
    """)

    # Insert records
    for record in records:
        con.execute("""
            INSERT INTO temp_batch VALUES (?, ?, ?, ?, ?, ?)
        """, [
            record['patient'],
            record['date'],
            record['encounter'],
            record['note_text'],
            record['natural_note_text'],
            record['llm_metadata']
        ])

    # Append or create Parquet file
    if output_file.exists():
        # Read existing and combine
        con.execute(f"""
            CREATE TABLE existing_notes AS
            SELECT * FROM read_parquet('{output_file}')
        """)
        con.execute("""
            CREATE TABLE combined AS
            SELECT * FROM existing_notes
            UNION ALL
            SELECT * FROM temp_batch
        """)
        con.execute(f"""
            COPY combined
            TO '{output_file}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        con.execute("DROP TABLE existing_notes")
        con.execute("DROP TABLE combined")
    else:
        # Create new file
        con.execute(f"""
            COPY temp_batch
            TO '{output_file}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

    print(f"    ✓ Wrote batch of {len(records)} notes to {output_file.name}")
    con.execute("DROP TABLE temp_batch")


def main():
    parser = ArgumentParser(
        prog='process_notes.py',
        description='Process Synthea notes in two passes: parse → transform'
    )

    # Mode selection
    parser.add_argument(
        'mode',
        choices=['parse', 'transform'],
        help='parse: Convert text files to notes.parquet with encounter associations | transform: Apply LLM to notes incrementally'
    )

    # Common arguments
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='parse: limit patient files | transform: limit notes to process'
    )

    # Parse mode arguments
    parser.add_argument(
        '--notes_dir',
        default='../output/notes',
        help='[parse] Directory containing patient note text files (default: ../output/notes)'
    )
    parser.add_argument(
        '--parquet_dir',
        default='../output/parquet',
        help='[parse] Directory containing Parquet files with encounters (default: ../output/parquet)'
    )
    parser.add_argument(
        '--output',
        default='../output/parquet/notes.parquet',
        help='[parse] Output path for notes.parquet (default: ../output/parquet/notes.parquet)'
    )

    # Transform mode arguments
    parser.add_argument(
        '--notes_parquet',
        default='../output/parquet/notes.parquet',
        help='[transform] Input notes.parquet file (default: ../output/parquet/notes.parquet)'
    )
    parser.add_argument(
        '--output_parquet',
        default='../output/parquet/notes_llm.parquet',
        help='[transform] Output notes_llm.parquet file (default: ../output/parquet/notes_llm.parquet)'
    )
    parser.add_argument(
        '--model',
        default='qwen2.5:32b',
        help='[transform] Ollama model to use (default: qwen2.5:32b)'
    )
    parser.add_argument(
        '--temperature',
        type=float,
        default=0.3,
        help='[transform] LLM temperature 0.0-1.0 (default: 0.3)'
    )
    parser.add_argument(
        '--template',
        default=None,
        help='[transform] Path to note template file (structure/format to follow)'
    )
    parser.add_argument(
        '--voice',
        default=None,
        help='[transform] Path to voice example file (provider writing style)'
    )
    parser.add_argument(
        '--batch_size',
        type=int,
        default=100,
        help='[transform] Number of notes to process before writing to Parquet (default: 100)'
    )

    args = parser.parse_args()

    # Execute appropriate mode
    if args.mode == 'parse':
        # Parse mode: convert text files to Parquet
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
            print(f"Total notes parsed: {results['total_notes']}")
            print(f"Matched to encounters: {results['matched_encounters']}")
            print(f"Unmatched: {results['unmatched_encounters']}")
            print(f"Output: {args.output}")

    elif args.mode == 'transform':
        # Load template if provided
        template = None
        if args.template:
            template_path = Path(args.template)
            if template_path.exists():
                with open(template_path, 'r', encoding='utf-8') as f:
                    template = f.read()
                print(f"✓ Loaded template: {args.template}")
            else:
                print(f"✗ Template file not found: {args.template}")
                return

        # Load voice example if provided
        voice_example = None
        if args.voice:
            voice_path = Path(args.voice)
            if voice_path.exists():
                with open(voice_path, 'r', encoding='utf-8') as f:
                    voice_example = f.read()
                print(f"✓ Loaded voice example: {args.voice}")
            else:
                print(f"✗ Voice file not found: {args.voice}")
                return

        # Check Ollama availability
        print("\nChecking Ollama availability...")
        if not check_ollama_available(args.model):
            return

        # Transform mode: apply LLM incrementally
        results = transform_notes_pass2(
            notes_parquet=args.notes_parquet,
            output_parquet=args.output_parquet,
            model=args.model,
            temperature=args.temperature,
            template=template,
            voice_example=voice_example,
            template_path=args.template,
            voice_path=args.voice,
            limit=args.limit,
            batch_size=args.batch_size
        )

        if "error" not in results:
            print(f"\n{'='*60}")
            print("SUMMARY")
            print(f"{'='*60}")
            print(f"Total notes in source: {results['total_notes']}")
            print(f"Pending notes: {results['pending_notes']}")
            print(f"Transformed: {results['transformed']}")

            if results['transformed'] > 0:
                print(f"Total input tokens: {results['total_input_tokens']:,}")
                print(f"Total output tokens: {results['total_output_tokens']:,}")
                print(f"Total tokens: {results['total_input_tokens'] + results['total_output_tokens']:,}")

                # Calculate and display timing statistics
                total_time = results['total_elapsed_time']
                avg_time = total_time / results['transformed']
                print(f"Total LLM time: {total_time:.1f}s")
                print(f"Average time per note: {avg_time:.2f}s")

            print(f"Output: {args.output_parquet}")


if __name__ == '__main__':
    main()
