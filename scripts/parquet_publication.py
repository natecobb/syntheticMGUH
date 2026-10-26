"""Recover an interrupted source conversion before reading its append inputs."""

import hashlib
import json
from pathlib import Path

JOURNAL = ".parquet-publication.json"


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def recover(directory: Path) -> None:
    journal = directory / JOURNAL
    if not journal.exists():
        return
    entries = json.loads(journal.read_text())
    for entry in entries:
        if any(Path(entry[key]).name != entry[key] for key in ("staged", "final")):
            raise ValueError("Invalid Parquet publication journal path")
        staged, final = directory / entry["staged"], directory / entry["final"]
        candidate = staged if staged.exists() else final
        if not candidate.exists() or digest(candidate) != entry["sha256"]:
            raise ValueError(
                f"Cannot recover incomplete Parquet publication: {candidate}"
            )
    for entry in entries:
        staged = directory / entry["staged"]
        if staged.exists():
            staged.replace(directory / entry["final"])
    journal.unlink()


def publish(directory: Path, files: list[tuple[str, Path, Path]]) -> None:
    journal = directory / JOURNAL
    if journal.exists():
        raise ValueError(
            "Recover the pending Parquet publication before staging another"
        )
    entries = [
        dict(staged=staged.name, final=final.name, sha256=digest(staged))
        for _, staged, final in files
    ]
    temporary = journal.with_suffix(".tmp")
    temporary.write_text(json.dumps(entries))
    temporary.replace(journal)
    recover(directory)
