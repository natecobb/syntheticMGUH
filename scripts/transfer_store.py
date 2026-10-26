"""Immutable Parquet snapshots for the incremental hospital-transfer projection."""

from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
from uuid import uuid4

import duckdb


def identifier(value: str) -> str:
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", value):
        raise ValueError(f"Unsupported table/column name: {value!r}")
    return '"' + value + '"'


def literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def columns(con: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    return [r[0] for r in con.execute(f"DESCRIBE {identifier(table)}").fetchall()]


def tables(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [r[0] for r in con.execute("SHOW TABLES").fetchall()]


def patient_column(name: str, cols: list[str]) -> str | None:
    if name == "patients":
        return "id"
    return next((c for c in ("patient", "patientid") if c in cols), None)


def file_hash(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def inventory(path: Path) -> dict[str, str]:
    return {
        p.stem: file_hash(p)
        for p in sorted(path.glob("*.parquet"))
        if not p.name.startswith(".") and p.stem != "notes_llm"
    }


def load_source(
    con: duckdb.DuckDBPyConnection, source: Path, files: dict[str, str]
) -> None:
    for name in files:
        con.execute(
            f"CREATE TABLE {identifier(name)} AS SELECT * FROM read_parquet(?)",
            [str(source / f"{name}.parquet")],
        )


def patient_hashes(con: duckdb.DuckDBPyConnection) -> dict[str, str]:
    """Content hashes ignore row order, but preserve duplicate rows and all source fields."""
    digests = {
        str(p[0]): hashlib.sha256()
        for p in con.execute("SELECT id FROM patients").fetchall()
    }
    for name in tables(con):
        col = patient_column(name, columns(con, name))
        if col is None:
            continue
        cursor = con.execute(f"""
            SELECT CAST({identifier(col)} AS VARCHAR), sha256(to_json(t)) AS digest
            FROM {identifier(name)} t ORDER BY 1, 2
        """)
        while rows := cursor.fetchmany(10000):
            for patient, digest in rows:
                if patient not in digests:
                    raise ValueError(f"{name} references unknown patient {patient}")
                digests[patient].update((name + ":" + digest + "\n").encode())
    return {patient: digest.hexdigest() for patient, digest in digests.items()}


@contextmanager
def output_lock(output: Path):
    output.parent.mkdir(parents=True, exist_ok=True)
    with (output.parent / f".{output.name}.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError(f"Another writer is publishing {output}") from exc
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def publish(con: duckdb.DuckDBPyConnection, output: Path, manifest: dict) -> Path:
    """All tables become visible together through one atomic symlink replacement."""
    root = output.parent / f".{output.name}.snapshots"
    root.mkdir(exist_ok=True)
    snapshot = root / uuid4().hex
    snapshot.mkdir()
    pointer = output.parent / f".{output.name}.{uuid4().hex}.tmp"
    try:
        for name in tables(con):
            con.execute(
                f"COPY (SELECT * FROM {identifier(name)} ORDER BY ALL) TO "
                f"{literal(snapshot / (name + '.parquet'))} "
                "(FORMAT PARQUET, COMPRESSION ZSTD)"
            )
        (snapshot / "_transfers.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n"
        )
        for path in snapshot.iterdir():
            with path.open("rb") as stream:
                os.fsync(stream.fileno())
        descriptor = os.open(snapshot, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        pointer.symlink_to(
            snapshot.relative_to(output.parent), target_is_directory=True
        )
        os.replace(pointer, output)
        descriptor = os.open(output.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return snapshot
    except BaseException:
        pointer.unlink(missing_ok=True)
        # Never remove a snapshot after its pointer became visible.
        if not output.is_symlink() or output.resolve() != snapshot.resolve():
            shutil.rmtree(snapshot)
        raise


def merge_previous(
    con: duckdb.DuckDBPyConnection, previous: Path, old_patients: set[str]
) -> None:
    """Replace old patients with their already transformed rows; new patients stay raw."""
    con.execute("CREATE TEMP TABLE _old_patients(id VARCHAR)")
    if old_patients:
        con.executemany(
            "INSERT INTO _old_patients VALUES (?)", [(p,) for p in sorted(old_patients)]
        )
    existing = {p.stem: p for p in previous.glob("*.parquet")}
    names = set(tables(con)) | set(existing)
    for name in sorted(names - {"_old_patients"}):
        if name not in tables(con):
            con.execute(
                f"CREATE TABLE {identifier(name)} AS SELECT * FROM read_parquet(?)",
                [str(existing[name])],
            )
            continue
        col = patient_column(name, columns(con, name))
        if col is None:
            if name == "codes" and name in existing:
                # A previous ED split may have added the receiving admission code.
                # Keep that vocabulary even when no new patient transfers this run.
                con.execute(
                    """CREATE OR REPLACE TABLE codes AS
                    SELECT * FROM codes UNION ALL BY NAME
                    SELECT prior.* FROM read_parquet(?) prior
                    WHERE NOT EXISTS (SELECT 1 FROM codes current
                        WHERE current.vocab = prior.vocab
                          AND CAST(current.code AS VARCHAR) = CAST(prior.code AS VARCHAR))""",
                    [str(existing[name])],
                )
            continue
        con.execute(
            f"DELETE FROM {identifier(name)} WHERE {identifier(col)} IN (SELECT id FROM _old_patients)"
        )
        if name in existing:
            con.execute(
                f"CREATE OR REPLACE TABLE {identifier(name)} AS "
                f"SELECT * FROM {identifier(name)} UNION ALL BY NAME "
                f"SELECT * FROM read_parquet(?)",
                [str(existing[name])],
            )
    con.execute("DROP TABLE _old_patients")
