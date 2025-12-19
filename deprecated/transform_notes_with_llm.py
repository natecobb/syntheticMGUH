#!/usr/bin/env python3
"""
Transform structured Synthea clinical notes into natural-looking clinical notes using a local LLM.

Uses Ollama with qwen3:latest to rewrite structured markdown notes into more realistic,
narrative clinical documentation.
"""

import json
import requests
from pathlib import Path
from argparse import ArgumentParser
from typing import Dict, Any


def check_ollama_available(model: str = "qwen2.5:latest") -> bool:
    """
    Check if Ollama is running and the specified model is available.

    Args:
        model: Model name to check

    Returns:
        True if Ollama is available and model exists
    """
    try:
        response = requests.get("http://localhost:11434/api/tags")
        if response.status_code == 200:
            models = response.json().get("models", [])
            model_names = [m.get("name") for m in models]
            print(f"Available models: {', '.join(model_names)}")

            # Check if requested model exists
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


def transform_note_with_llm(
    note_text: str,
    model: str = "qwen2.5:latest",
    temperature: float = 0.3,
    template: str = None,
    voice_example: str = None
) -> str:
    """
    Transform structured note into natural clinical note using LLM.

    Args:
        note_text: Structured clinical note text
        model: Ollama model to use
        temperature: LLM temperature (0.0-1.0, lower = more conservative)
        template: Optional note template/structure to follow
        voice_example: Optional example showing provider's writing style
                      (terseness, language level, typo frequency, etc.)

    Returns:
        Transformed clinical note
    """
    # Build the prompt with optional template and voice
    prompt_parts = ["Transform this structured clinical note into a natural physician's note. Write ONLY the clinical note."]

    if template:
        prompt_parts.append(f"\nUse this note template/structure:\n{template}")

    if voice_example:
        prompt_parts.append(f"\nMatch this provider's writing style exactly - pay attention to terseness, language complexity, abbreviations, and writing patterns including any typos or informal elements:\n{voice_example}")

    prompt_parts.append("""
Guidelines:
- Maintain all clinical facts exactly as provided
- Remove markdown headers (# symbols)
- If voice example provided, match that style precisely (terse vs verbose, language level, typo patterns)
- If no voice provided, use standard professional medical narrative style""")

    prompt_parts.append(f"\nStructured Note:\n{note_text}")
    prompt_parts.append("\nClinical Note:")

    prompt = "\n".join(prompt_parts)

    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": temperature,
                    "num_predict": 2000  # Max tokens for response
                }
            },
            timeout=120
        )

        if response.status_code == 200:
            result = response.json()
            response_text = result.get("response", "").strip()

            # Remove thinking tags if present (some models output reasoning)
            if "<think>" in response_text and "</think>" in response_text:
                # Extract content after </think> tag
                parts = response_text.split("</think>")
                if len(parts) > 1:
                    response_text = parts[-1].strip()

            return response_text
        else:
            print(f"  Error: HTTP {response.status_code}")
            return None

    except requests.exceptions.Timeout:
        print("  Error: Request timed out")
        return None
    except Exception as e:
        print(f"  Error: {str(e)}")
        return None


def process_jsonl_file(
    jsonl_path: Path,
    model: str = "qwen2.5:latest",
    temperature: float = 0.3,
    limit: int = None,
    skip_existing: bool = True,
    template: str = None,
    voice_example: str = None
) -> Dict[str, Any]:
    """
    Process a single JSONL file, transforming all notes.

    Args:
        jsonl_path: Path to JSONL file
        model: Ollama model to use
        temperature: LLM temperature
        limit: Limit number of notes to process (None = all)
        skip_existing: Skip notes that already have natural_note_text
        template: Optional note template
        voice_example: Optional voice example

    Returns:
        Dictionary with processing statistics
    """
    print(f"\nProcessing: {jsonl_path.name}")

    # Read all notes from file
    notes = []
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            notes.append(json.loads(line))

    total_notes = len(notes)
    processed = 0
    skipped = 0
    errors = 0

    # Process each note
    transformed_notes = []
    for i, note in enumerate(notes):
        if limit and i >= limit:
            skipped += (total_notes - i)
            break

        # Skip if already processed and skip_existing is True
        if skip_existing and "natural_note_text" in note:
            transformed_notes.append(note)
            skipped += 1
            continue

        print(f"  [{i+1}/{total_notes}] Transforming note from {note['date']}...", end=" ")

        # Transform the note
        natural_text = transform_note_with_llm(note['note_text'], model, temperature, template, voice_example)

        if natural_text:
            note['natural_note_text'] = natural_text
            note['llm_model'] = model
            note['llm_temperature'] = temperature
            transformed_notes.append(note)
            processed += 1
            print("✓")
        else:
            # Keep original note if transformation fails
            transformed_notes.append(note)
            errors += 1
            print("✗")

    # Write back to file
    with open(jsonl_path, 'w', encoding='utf-8') as f:
        for note in transformed_notes:
            json.dump(note, f, ensure_ascii=False)
            f.write('\n')

    return {
        "file": jsonl_path.name,
        "total": total_notes,
        "processed": processed,
        "skipped": skipped,
        "errors": errors
    }


def main():
    parser = ArgumentParser(
        prog='transform_notes_with_llm.py',
        description='Transform structured clinical notes to natural format using local LLM via Ollama'
    )
    parser.add_argument(
        '--notes_dir',
        default='../output/notes',
        help='Directory containing patient JSONL files (default: ../output/notes)'
    )
    parser.add_argument(
        '--model',
        default='qwen2.5:latest',
        help='Ollama model to use (default: qwen2.5:latest)'
    )
    parser.add_argument(
        '--temperature',
        type=float,
        default=0.3,
        help='LLM temperature 0.0-1.0, lower = more conservative (default: 0.3)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Limit number of notes to process per file (default: all)'
    )
    parser.add_argument(
        '--patient',
        default=None,
        help='Process only specific patient file (provide filename)'
    )
    parser.add_argument(
        '--no-skip',
        action='store_true',
        help='Reprocess notes that already have natural_note_text'
    )
    parser.add_argument(
        '--template',
        default=None,
        help='Path to note template file (structure/format to follow)'
    )
    parser.add_argument(
        '--voice',
        default=None,
        help='Path to voice example file (shows provider writing style: terse vs verbose, language level, typo patterns)'
    )

    args = parser.parse_args()

    # Load template if provided
    template = None
    if args.template:
        template_path = Path(args.template)
        if template_path.exists():
            with open(template_path, 'r', encoding='utf-8') as f:
                template = f.read()
            print(f"✓ Loaded template from: {args.template}")
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
            print(f"✓ Loaded voice example from: {args.voice}")
        else:
            print(f"✗ Voice file not found: {args.voice}")
            return

    # Check if Ollama is available
    print("Checking Ollama availability...")
    if not check_ollama_available(args.model):
        return

    notes_path = Path(args.notes_dir)

    if not notes_path.exists():
        print(f"Error: Notes directory not found: {args.notes_dir}")
        return

    # Get JSONL files to process
    if args.patient:
        jsonl_files = [notes_path / args.patient]
        if not jsonl_files[0].exists():
            print(f"Error: Patient file not found: {args.patient}")
            return
    else:
        jsonl_files = sorted(notes_path.glob("*.jsonl"))

    if not jsonl_files:
        print(f"Warning: No .jsonl files found in {args.notes_dir}")
        return

    print(f"\nFound {len(jsonl_files)} patient file(s) to process")
    print(f"Model: {args.model}")
    print(f"Temperature: {args.temperature}")
    if args.limit:
        print(f"Limit: {args.limit} notes per patient")
    if template:
        print(f"Using custom template")
    if voice_example:
        print(f"Using voice example")

    # Process all files
    results = []
    for jsonl_file in jsonl_files:
        result = process_jsonl_file(
            jsonl_file,
            model=args.model,
            temperature=args.temperature,
            limit=args.limit,
            skip_existing=not args.no_skip,
            template=template,
            voice_example=voice_example
        )
        results.append(result)

    # Print summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)

    total_processed = sum(r['processed'] for r in results)
    total_skipped = sum(r['skipped'] for r in results)
    total_errors = sum(r['errors'] for r in results)
    total_notes = sum(r['total'] for r in results)

    print(f"Files processed: {len(results)}")
    print(f"Total notes: {total_notes}")
    print(f"Transformed: {total_processed}")
    print(f"Skipped: {total_skipped}")
    print(f"Errors: {total_errors}")

    if results:
        print("\nPer-file breakdown:")
        for result in results:
            print(f"  {result['file']}: {result['processed']}/{result['total']} "
                  f"(skipped: {result['skipped']}, errors: {result['errors']})")


if __name__ == '__main__':
    main()
