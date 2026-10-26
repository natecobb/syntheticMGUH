#!/usr/bin/env python3
"""
Iterative, crash-recoverable Synthea data generation and Parquet conversion pipeline.

Generates patients in chunks of `--batch_size` (every X patients):
1. Runs Synthea for batch i using a deterministic batch seed (`seed + i * 1000003`)
   and a fixed clinician seed (`-cs seed`) + reference date (`-r YYYYMMDD`) so all
   batches share identical organizations and clinician providers.
2. Converts the batch CSVs, symptoms, and clinical notes into DuckDB and atomically
   merges them into `./output/parquet/*.parquet`.
3. Immediately deletes raw `./output/csv/`, `./output/notes/`, `./output/symptoms/`,
   and `./output/metadata/` so disk usage stays bounded to a single batch.
4. Saves checkpoint progress to `./output/parquet/_checkpoint.json` after each batch
   so interrupted runs resume automatically from the last unfinished batch.
"""

import importlib.util
import json
import math
import shutil
import subprocess
import sys
import time
from argparse import ArgumentParser
from datetime import datetime, timezone
from pathlib import Path
from dateutil import parser as date_parser
import duckdb


def _load_convert_module():
    """Import 3_convert_to_parquet.py dynamically."""
    script_path = Path(__file__).resolve().parent / "3_convert_to_parquet.py"
    spec = importlib.util.spec_from_file_location("convert_to_parquet", script_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _clean_raw_dirs(output_dir: Path):
    """Remove any leftover raw human-readable directories before starting a batch."""
    for subdir in ["csv", "notes", "symptoms", "metadata"]:
        target = output_dir / subdir
        if target.exists():
            shutil.rmtree(target, ignore_errors=True)


def _save_checkpoint(checkpoint_file: Path, state: dict):
    """Atomically write checkpoint state JSON."""
    tmp_file = checkpoint_file.parent / ".tmp_checkpoint.json"
    tmp_file.write_text(json.dumps(state, indent=2), encoding="utf-8")
    tmp_file.replace(checkpoint_file)


def _batch_requests(total: int, batch_size: int, completed: dict) -> dict[int, int]:
    """Extend a population without replaying a previously completed partial batch."""
    if total <= 0 or batch_size <= 0:
        raise ValueError("Population and batch size must be positive")
    requests = {int(key): info["patients_requested"] for key, info in completed.items()}
    remaining = max(0, total - sum(requests.values()))
    next_index = max(requests, default=-1) + 1
    while remaining:
        count = min(remaining, batch_size)
        requests[next_index] = count
        next_index += 1
        remaining -= count
    return dict(sorted(requests.items()))


def _update_latest_symlinks(base_output_root: Path, seed_dir: Path, parquet_path: Path):
    """Point ./output/latest and ./output/parquet at the active seed directory."""
    try:
        latest_link = base_output_root / "latest"
        if latest_link.is_symlink() or latest_link.exists():
            if latest_link.is_dir() and not latest_link.is_symlink():
                shutil.rmtree(latest_link, ignore_errors=True)
            else:
                latest_link.unlink()
        latest_link.symlink_to(seed_dir.name)

        pq_link = base_output_root / "parquet"
        if pq_link.resolve() != parquet_path.resolve():
            if pq_link.is_symlink() or pq_link.exists():
                if pq_link.is_dir() and not pq_link.is_symlink():
                    shutil.rmtree(pq_link, ignore_errors=True)
                else:
                    pq_link.unlink()
            pq_link.symlink_to(Path(seed_dir.name) / "parquet")
    except OSError:
        pass


def main(
    total_patients: int = 500,
    batch_size: int = 100,
    seed: int = 12345,
    years_of_history: int = 14,
    output_dir: str = None,
    parquet_dir: str = None,
    ehr_launch_date: datetime = date_parser.parse("2010-01-01 00:00:00Z"),
    include_notes: bool = True,
    keep_descriptions: bool = False,
    no_claims: bool = False,
    threads: int = 10,
    reset: bool = False,
    transfers_output: str = None,
) -> int:
    repo_root = Path(__file__).resolve().parent
    base_output_root = repo_root / "output"
    base_output_root.mkdir(parents=True, exist_ok=True)

    # Default to seed-isolated directory: ./output/seed_<seed>/
    output_path = Path(output_dir) if output_dir else (base_output_root / f"seed_{seed}")
    parquet_path = Path(parquet_dir) if parquet_dir else (output_path / "parquet")
    checkpoint_file = parquet_path / "_checkpoint.json"

    # Ensure Synthea JAR and provider files are ready
    jar_path = repo_root / "synthea" / "bin" / "synthea-with-dependencies.jar"
    if not jar_path.exists():
        print("Synthea JAR not found in synthea/bin/. Running ./0_setup_synthea.sh...")
        subprocess.run(["./0_setup_synthea.sh"], cwd=repo_root, check=True)

    providers_dir = repo_root / "synthea" / "providers" / "syntheticMGUH"
    if not (providers_dir / "hospitals.csv").exists():
        print("MedStar provider files not found. Running 1_build_providers.py...")
        subprocess.run([sys.executable, "1_build_providers.py"], cwd=repo_root, check=True)

    if reset and output_path.exists():
        print(f"Resetting existing output directory: {output_path}")
        shutil.rmtree(output_path, ignore_errors=True)

    output_path.mkdir(parents=True, exist_ok=True)
    parquet_path.mkdir(parents=True, exist_ok=True)
    if output_path.parent.resolve() == base_output_root.resolve():
        _update_latest_symlinks(base_output_root, output_path, parquet_path)

    num_batches = math.ceil(total_patients / batch_size)

    # Load or initialize checkpoint state
    if checkpoint_file.exists() and not reset:
        state = json.loads(checkpoint_file.read_text(encoding="utf-8"))
        saved_seed = state.get("base_seed", seed)
        saved_batch_size = state.get("batch_size", batch_size)
        if saved_seed != seed or saved_batch_size != batch_size:
            print(
                f"⚠ Note: Using checkpoint's locked seed={saved_seed} and batch_size={saved_batch_size} "
                "(pass --reset to start a new build with different seed/batch_size)."
            )
            seed = saved_seed
            batch_size = saved_batch_size
            num_batches = math.ceil(total_patients / batch_size)
        clinician_seed = state.get("clinician_seed", seed)
        reference_date = state.get("reference_date", datetime.now().strftime("%Y%m%d"))
        completed_batches = state.get("completed_batches", {})
        print(f"\n{'='*60}")
        print("RESUMING INCREMENTAL BUILD FROM CHECKPOINT")
        print(f"{'='*60}")
        print(f"Completed batches: {len(completed_batches)}/{num_batches}")
        print(f"Locked base seed: {seed} | Clinician seed: {clinician_seed} | Reference date: {reference_date}")
    else:
        clinician_seed = seed
        reference_date = datetime.now().strftime("%Y%m%d")
        completed_batches = {}
        state = {
            "base_seed": seed,
            "clinician_seed": clinician_seed,
            "reference_date": reference_date,
            "batch_size": batch_size,
            "total_target": total_patients,
            "years_of_history": years_of_history,
            "include_notes": include_notes,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "completed_batches": completed_batches,
        }

    state["total_target"] = total_patients
    state["batch_size"] = batch_size
    batch_requests = _batch_requests(total_patients, batch_size, completed_batches)
    num_batches = len(batch_requests)

    convert_mod = _load_convert_module()
    pipeline_start = time.time()

    print(f"\n{'='*60}")
    print("ITERATIVE SYNTHEA → PARQUET PIPELINE")
    print(f"{'='*60}")
    print(f"Target population: {total_patients:,} patients ({num_batches} batches of up to {batch_size:,})")
    print(f"Base seed:         {seed} (Clinician seed: {clinician_seed})")
    print(f"CPU threads:       {threads} (Synthea + DuckDB)")
    print(f"Years of history:  {years_of_history} (EHR launch filter: {ehr_launch_date.date()})")
    print(f"Clinical notes:    {'enabled' if include_notes else 'disabled'}")
    print(f"Parquet output:    {parquet_path.resolve()}")

    for batch_idx, batch_patients in batch_requests.items():
        batch_key = str(batch_idx)
        batch_seed = seed + batch_idx * 1000003

        if batch_key in completed_batches:
            info = completed_batches[batch_key]
            print(
                f"\n[Batch {batch_idx + 1}/{num_batches}] ✓ Already completed "
                f"({info.get('patients_requested')} requested, seed={info.get('batch_seed')}) — skipping"
            )
            continue

        batch_start = time.time()
        print(f"\n{'#'*60}")
        print(
            f"BATCH {batch_idx + 1}/{num_batches}: Generating {batch_patients:,} patients "
            f"(seed={batch_seed}, clinician_seed={clinician_seed})"
        )
        print(f"{'#'*60}")

        # 1. Clean any leftover raw files from an interrupted run
        _clean_raw_dirs(output_path)

        # 2. Run Synthea for this batch
        # NOTE: -c ./synthea/synthea.properties MUST come before -p/-s/-cs/-r so CLI flags override properties!
        classpath = f"synthea:synthea/bin/resources:{jar_path}"
        excluded_csvs = (
            "patient_expenses.csv,claims.csv,claims_transactions.csv"
            if no_claims
            else "patient_expenses.csv"
        )
        cmd = [
            "java",
            "-Xmx8g",
            "-cp",
            classpath,
            "App",
            "District of Columbia",
            "Washington",
            "-c",
            "./synthea/synthea.properties",
            "-d",
            "./synthea/modules",
            "-p",
            str(batch_patients),
            "-s",
            str(batch_seed),
            "-cs",
            str(clinician_seed),
            "-r",
            reference_date,
            f"--generate.thread_pool_size={threads}",
            f"--exporter.baseDirectory={output_path}/",
            f"--exporter.years_of_history={years_of_history}",
            f"--exporter.clinical_note.export={'true' if include_notes else 'false'}",
            f"--exporter.csv.excluded_files={excluded_csvs}",
        ]

        subprocess.run(cmd, cwd=repo_root, check=True)

        # 3. Convert batch CSVs + notes to Parquet, merge with prior batches, and delete raw CSV/notes
        has_prior_batches = len(completed_batches) > 0 and (parquet_path / "patients.parquet").exists()
        convert_mod.main(
            synthea_data_path=str(output_path),
            output_path=str(parquet_path),
            ehr_launch_date=ehr_launch_date,
            include_notes=include_notes,
            keep_descriptions=keep_descriptions,
            append=has_prior_batches,
            cleanup=True,
            no_claims=no_claims,
            threads=threads,
        )

        # 4. Record batch checkpoint atomically
        con = duckdb.connect(":memory:")
        cumulative_patients = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{parquet_path / 'patients.parquet'}')"
        ).fetchone()[0]
        con.close()

        batch_elapsed = round(time.time() - batch_start, 2)
        completed_batches[batch_key] = {
            "batch_index": batch_idx,
            "batch_seed": batch_seed,
            "patients_requested": batch_patients,
            "cumulative_patients_in_parquet": cumulative_patients,
            "elapsed_seconds": batch_elapsed,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        state["completed_batches"] = completed_batches
        _save_checkpoint(checkpoint_file, state)

        pq_size_mb = sum(f.stat().st_size for f in parquet_path.glob("*.parquet")) / (1024 * 1024)
        print(
            f"\n✓ Batch {batch_idx + 1}/{num_batches} checkpointed in {batch_elapsed:.1f}s | "
            f"Cumulative patients: {cumulative_patients:,} | "
            f"Parquet total: {pq_size_mb:.2f} MB | Raw CSV/notes on disk: 0.00 MB"
        )

    total_elapsed = time.time() - pipeline_start
    pq_size_mb = sum(f.stat().st_size for f in parquet_path.glob("*.parquet")) / (1024 * 1024)
    con = duckdb.connect(":memory:")
    final_patients = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{parquet_path / 'patients.parquet'}')"
    ).fetchone()[0]
    final_encounters = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{parquet_path / 'encounters.parquet'}')"
    ).fetchone()[0]
    con.close()

    print(f"\n{'='*60}")
    print("✓ INCREMENTAL PIPELINE COMPLETE")
    print(f"{'='*60}")
    print(f"Batches completed:  {len(completed_batches)}/{num_batches}")
    print(f"Total patients:     {final_patients:,} (including deceased replacements)")
    print(f"Total encounters:   {final_encounters:,}")
    print(f"Total Parquet size: {pq_size_mb:.2f} MB")
    print(f"Raw files on disk:  0.00 MB (cleaned after every batch)")
    print(f"Elapsed time:       {total_elapsed:.1f}s")
    print(f"Checkpoint file:    {checkpoint_file}")
    if transfers_output:
        from scripts.process_transfers import run as process_transfers

        print(json.dumps(process_transfers(parquet_path, Path(transfers_output)), indent=2))
    return 0


if __name__ == "__main__":
    parser = ArgumentParser(
        prog="2_generate_incremental.py",
        description="Iterative, crash-recoverable Synthea generation + Parquet conversion (loads and deletes raw files every X patients)",
    )
    parser.add_argument(
        "-p", "--total",
        type=int,
        default=500,
        help="Total target number of patients to generate across all batches (default: 500)",
    )
    parser.add_argument(
        "-b", "--batch_size",
        type=int,
        default=100,
        help="Number of patients per batch before converting to Parquet and deleting raw CSV/notes (default: 100)",
    )
    parser.add_argument(
        "-s", "--seed",
        type=int,
        default=12345,
        help="Base random seed for population and clinicians (default: 12345)",
    )
    parser.add_argument(
        "-y", "--years",
        type=int,
        default=14,
        help="Years of history to keep in Synthea exporter (default: 14)",
    )
    parser.add_argument(
        "--path",
        default=None,
        help="Base Synthea output folder (default: ./output/seed_<seed>)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Parquet output folder (default: ./output/seed_<seed>/parquet)",
    )
    parser.add_argument(
        "--start",
        default="2010-01-01 00:00:00Z",
        type=lambda s: date_parser.parse(s),
        help="EHR launch date for filtering operational tables (default: 2010-01-01)",
    )
    parser.add_argument(
        "--no_notes",
        action="store_true",
        default=False,
        help="Disable clinical notes generation and parsing",
    )
    parser.add_argument(
        "--keep_descriptions",
        action="store_true",
        default=False,
        help="Keep redundant text description columns in Parquet tables",
    )
    parser.add_argument(
        "--no_claims",
        action="store_true",
        default=False,
        help="Exclude claims.csv and claims_transactions.csv (~66%% smaller raw CSV, ~78%% smaller Parquet)",
    )
    parser.add_argument(
        "-t", "--threads",
        type=int,
        default=10,
        help="Max CPU threads/cores to use for Synthea and DuckDB (default: 10)",
    )
    parser.add_argument(
        "--transfers-output",
        help="Publish an incremental hospital-transfer dataset at this separate path after generation",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        default=False,
        help="Ignore existing checkpoints and start a fresh build from batch 1",
    )

    args = parser.parse_args()
    sys.exit(
        main(
            total_patients=args.total,
            batch_size=args.batch_size,
            seed=args.seed,
            years_of_history=args.years,
            output_dir=args.path,
            parquet_dir=args.output,
            ehr_launch_date=args.start,
            include_notes=not args.no_notes,
            keep_descriptions=args.keep_descriptions,
            no_claims=args.no_claims,
            threads=args.threads,
            reset=args.reset,
            transfers_output=args.transfers_output,
        )
    )
