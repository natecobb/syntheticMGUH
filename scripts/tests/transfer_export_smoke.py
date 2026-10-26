"""Exercise actual Synthea onset export, conversion and transfer publication.

Runs only in disposable directories. The generated clinical course is retained;
the fixture sets its starting hospital and supplies synthetic directory rows so
the test does not depend on Synthea's randomized provider choice.
"""

from pathlib import Path
from contextlib import redirect_stdout
import importlib.util
import io
import sys
import tempfile
from uuid import NAMESPACE_URL, uuid5

import duckdb

from liver_care_export_smoke import run_case

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.process_transfers import run  # noqa: E402
from scripts.transfer_store import literal, inventory  # noqa: E402


def directory_fixture(path: Path) -> None:
    with duckdb.connect() as con:
        for table in ("organizations", "providers", "encounters"):
            con.execute(
                f"CREATE TABLE {table} AS SELECT * FROM read_parquet(?)",
                [str(path / f"{table}.parquet")],
            )
        source_provider = con.execute(
            "SELECT provider FROM encounters WHERE encounterclass='inpatient' ORDER BY start LIMIT 1"
        ).fetchone()[0]
        source_org = con.execute(
            "SELECT organization FROM providers WHERE id=?", [source_provider]
        ).fetchone()[0]
        sites = {}
        for name, npi in (("MGUH", "1427145176"), ("MWHC", "1548378235")):
            organization = str(uuid5(NAMESPACE_URL, "transfer-fixture/" + name))
            provider = str(
                uuid5(NAMESPACE_URL, "transfer-fixture/" + name + "/clinician")
            )
            # Remove NPI ambiguity only inside this disposable directory fixture.
            con.execute(
                "UPDATE organizations SET npi=NULL WHERE CAST(npi AS VARCHAR)=?", [npi]
            )
            con.execute(
                "INSERT INTO organizations SELECT * REPLACE (? AS id, ? AS npi, ? AS name, 0 AS utilization, 0 AS revenue) FROM organizations WHERE id=?",
                [organization, npi, name, source_org],
            )
            con.execute(
                "INSERT INTO providers SELECT * REPLACE (? AS id, ? AS organization, 0 AS encounters, 0 AS procedures) FROM providers WHERE id=?",
                [provider, organization, source_provider],
            )
            sites[name] = organization, provider
        con.execute(
            "UPDATE encounters SET organization=?, provider=? WHERE encounterclass IN ('emergency','inpatient')",
            list(sites["MWHC"]),
        )
        for table in ("organizations", "providers", "encounters"):
            con.execute(
                f"COPY {table} TO {literal(path / (table + '.parquet'))} (FORMAT PARQUET)"
            )


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="transfer-export-") as temp:
        root = Path(temp)
        for scenario in ("recovery", "transplant_survival"):
            case = root / scenario
            case.mkdir()
            run_case(case, "acetaminophen", scenario)
            parquet = case / "parquet"
            before_retry = inventory(parquet)
            spec = importlib.util.spec_from_file_location(
                "convert",
                Path(__file__).resolve().parents[2] / "3_convert_to_parquet.py",
            )
            converter = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(converter)
            with redirect_stdout(io.StringIO()):
                converter.main(
                    synthea_data_path=str(case / "raw"),
                    output_path=str(parquet),
                    include_notes=True,
                    no_claims=True,
                    append=True,
                    threads=1,
                )
            assert inventory(parquet) == before_retry, (
                "Replayed source batch must not change files or counters"
            )
            with duckdb.connect() as con:
                signals = con.execute(
                    "SELECT recognized_at FROM read_parquet(?) WHERE policy='liver'",
                    [str(parquet / "transfer_signals.parquet")],
                ).fetchall()
                assert signals, (
                    "Actual condition timestamps must survive CSV/notes conversion"
                )
                leaked = con.execute(
                    "SELECT count(*) FROM read_parquet(?) WHERE note_text LIKE '%synthea-transfer-v1%'",
                    [str(parquet / "notes.parquet")],
                ).fetchone()[0]
                assert leaked == 0, "Export metadata must not leak into note prose"
            directory_fixture(parquet)
            result = run(parquet, case / "transfers")
            assert result["transfers"] == 1, result
            assert run(parquet, case / "transfers")["status"] == "unchanged"
            print(
                f"PASS: actual onset export → conversion → {scenario} transfer → unchanged rerun",
                flush=True,
            )


if __name__ == "__main__":
    main()
