#!/usr/bin/env python3
"""
Convert Synthea clinical notes from individual text files to JSONL format.

Based on the parsing logic from load_notes_to_rdbms.py, this script:
1. Reads each patient's note file (PatientName_PatientID.txt)
2. Extracts individual notes separated by date
3. Outputs structured JSONL with patient_id, date, and note_text
"""

import os
import re
import json
from argparse import ArgumentParser
from pathlib import Path


def parse_notes_file(file_path):
    """
    Parse a single patient notes file into individual note records.

    Args:
        file_path: Path to the patient notes text file

    Returns:
        List of dictionaries with patient_id, patient_name, date, and note_text
    """
    # Extract patient ID from filename using regex from load_notes_to_rdbms.py
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

    # Extract individual notes using regex from load_notes_to_rdbms.py
    # Pattern: date (YYYY-MM-DD) followed by note text, separated by 4 newlines
    regex_notes = r"([1-2][0-9]{3}-[0-9]{2}-[0-9]{2}.*?)\n\n([\s\S]*?)(?=\n\n\n\n|\Z)"
    notes = re.findall(regex_notes, all_notes)

    # Build list of note records
    note_records = []
    for date_str, note_text in notes:
        # Clean up the date string (remove any trailing whitespace)
        date_str = date_str.strip()
        # Clean up the note text (remove trailing whitespace)
        note_text = note_text.strip()

        if note_text:  # Only include non-empty notes
            note_records.append({
                "patient_id": patient_id,
                "patient_name": patient_name,
                "date": date_str,
                "note_text": note_text
            })

    return note_records


def convert_notes_to_jsonl(notes_dir):
    """
    Convert all notes files to JSONL format.
    Creates one JSONL file per patient in the same directory with the same name.
    Deletes each text file after successful conversion.

    Args:
        notes_dir: Directory containing note text files
    """
    notes_path = Path(notes_dir)

    if not notes_path.exists():
        print(f"Error: Notes directory not found: {notes_dir}")
        return

    # Get all .txt files in the notes directory
    note_files = sorted(notes_path.glob("*.txt"))

    if not note_files:
        print(f"Warning: No .txt files found in {notes_dir}")
        return

    total_notes = 0
    deleted_files = 0

    print(f"Converting {len(note_files)} patient note files to JSONL in {notes_dir}")

    for note_file in note_files:
        note_records = parse_notes_file(note_file)

        if note_records:
            # Create output filename in same directory with same base name
            jsonl_path = note_file.with_suffix('.jsonl')

            with open(jsonl_path, 'w', encoding='utf-8') as out_f:
                for record in note_records:
                    json.dump(record, out_f, ensure_ascii=False)
                    out_f.write('\n')

            total_notes += len(note_records)
            print(f"  {note_file.name}: {len(note_records)} notes -> {jsonl_path.name}")
            # Delete the text file after successful conversion
            note_file.unlink()
            deleted_files += 1

    print(f"\nTotal: {total_notes} notes written to {deleted_files} JSONL files")
    print(f"Deleted {deleted_files} source text files")


def main():
    parser = ArgumentParser(
        prog='convert_notes_to_jsonl.py',
        description='Convert Synthea clinical notes to JSONL format. Creates one JSONL file per patient in the same directory.'
    )
    parser.add_argument(
        '--notes_dir',
        default='../output/notes',
        help='Directory containing patient note text files (default: ../output/notes)'
    )

    args = parser.parse_args()

    convert_notes_to_jsonl(notes_dir=args.notes_dir)


if __name__ == '__main__':
    main()
