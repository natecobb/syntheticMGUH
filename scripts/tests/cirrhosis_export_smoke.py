"""Exercise the integrated cirrhosis parent through generation, notes, CSV, and Parquet."""
import json
import re
import subprocess
import tempfile
from pathlib import Path

import duckdb
import pandas as pd

from cirrhosis_integration_fixtures import ROOT, write_case

CASES = ("ward_recovery", "ward_deterioration", "transplant_survival", "icu_death", "ward_death", "recurrence")
CARE_CODES = {"50849002", "305351004", "305342007", "439740005"}


def check_legacy_and_empty_notes(folder, raw, encounters):
    """Old headers and an empty notes directory must keep the four-column schema."""
    for mode in ("legacy", "empty"):
        source = folder / mode
        notes = source / "notes"
        notes.mkdir(parents=True)
        (source / "csv").symlink_to(raw / "csv", target_is_directory=True)
        if mode == "legacy":
            for original in (raw / "notes").glob("*.txt"):
                old_headers = re.sub(r"(?m)^(\d{4}-\d{2}-\d{2})T[^\n]*$", r"\1", original.read_text())
                (notes / original.name).write_text(old_headers)
        output = folder / f"{mode}_parquet"
        with (folder / f"{mode}_conversion.log").open("w") as log:
            subprocess.run(["uv", "run", "python", "3_convert_to_parquet.py", "--path", str(source),
                "--output", str(output), "--no_claims", "--include_notes", "--threads", "1"],
                cwd=ROOT, check=True, stdout=log, stderr=subprocess.STDOUT, timeout=120)
        with duckdb.connect() as con:
            exported = con.read_parquet(str(output / "notes.parquet")).df()
        assert list(exported.columns) == ["patient", "date", "note_text", "encounter"]
        if mode == "legacy":
            assert len(exported) > 0 and exported.encounter.isin(encounters.id).all()
            assert (exported.date.dt.tz_convert("America/New_York").dt.hour == 0).all()
        else:
            assert exported.empty
        print(f"PASS: {mode} notes retain schema and conversion behavior", flush=True)


def run_case(folder, scenario):
    modules = folder / "modules"
    write_case(modules, scenario, generator=True)
    # Use the complete parent definition, with only the documented fixture entry/outcomes changed.
    raw, parquet = folder / "raw", folder / "parquet"
    fatal = scenario in ("icu_death", "ward_death")
    command = ["java", "-Xmx2g", "-cp", "synthea:synthea/bin/resources:synthea/bin/synthea-with-dependencies.jar",
        "App", "District of Columbia", "Washington", "-c", "synthea/synthea.properties",
        "-d", "synthea/modules", "-d", str(modules), "-m", scenario,
        "-p", "1", "-a", "55-56", "-s", "3003", "-cs", "3003", "-r", "20261025",
        "--generate.thread_pool_size=1", "--lifecycle.death_by_natural_causes=false",
        f"--generate.only_dead_patients={str(fatal).lower()}", "--exporter.clinical_note.export=true",
        "--exporter.csv.excluded_files=patient_expenses.csv,claims.csv,claims_transactions.csv",
        f"--exporter.baseDirectory={raw}"]
    with (folder / "generation.log").open("w") as log:
        subprocess.run(command, cwd=ROOT, check=True, stdout=log, stderr=subprocess.STDOUT, timeout=180)
    with (folder / "conversion.log").open("w") as log:
        subprocess.run(["uv", "run", "python", "3_convert_to_parquet.py", "--path", str(raw),
            "--output", str(parquet), "--no_claims", "--include_notes", "--threads", "1"],
            cwd=ROOT, check=True, stdout=log, stderr=subprocess.STDOUT, timeout=120)
    data = {}
    with duckdb.connect() as con:
        for name in ("patients", "encounters", "conditions", "observations", "procedures", "medications", "notes", "codes"):
            path = parquet / (name + ".parquet")
            data[name] = con.read_parquet(str(path)).df() if path.exists() else pd.DataFrame(columns=["patient", "encounter", "code", "start", "stop"])
    encounters = data["encounters"]
    care = encounters[encounters.code.astype(str).isin(CARE_CODES)].sort_values("start")
    ed = care[care.code.astype(str) == "50849002"]
    icu = care[care.code.astype(str) == "305351004"]
    expected_episodes = 2 if scenario == "recurrence" else 1
    expected_icu = scenario in ("icu_death", "ward_deterioration", "transplant_survival")
    assert len(ed) == expected_episodes and set(ed.encounterclass) == {"emergency"}
    assert len(icu) == int(expected_icu)
    assert (ed.stop - ed.start == pd.Timedelta(hours=1)).all(), "Explicit ED interval survives export"
    assert (care.stop >= care.start).all()
    assert all(care.stop.iloc[i-1] <= care.start.iloc[i] for i in range(1, len(care))), "Sequential acute care never overlaps"
    patient = ed.iloc[0].patient
    care_ids = set(care.id)
    for name in ("conditions", "observations", "procedures", "medications", "notes"):
        rows = data[name][data[name].encounter.isin(care_ids)]
        assert set(rows.patient) <= {patient}, f"{name} patient linkage"
        assert rows.encounter.isin(encounters.id).all(), f"{name} encounter linkage"
    assert care_ids <= set(data["notes"].encounter), "Both same-day ED and inpatient encounters retain linked notes"
    linked = data["notes"][data["notes"].encounter.isin(care_ids)].merge(
        care[["id", "start"]], left_on="encounter", right_on="id")
    assert (linked.date == linked.start).all(), "Precise note headers preserve encounter-start timestamps"
    conditions = data["conditions"]
    cirrhosis = conditions[conditions.code.astype(str) == "19943007"]
    assert len(cirrhosis) == 1, "One persistent native cirrhosis history across recurrence"
    assert not conditions.code.astype(str).eq("197270009").any(), "No de novo ALF label on chronic disease"
    procedures = data["procedures"]
    surgery = procedures[procedures.code.astype(str) == "18027006"]
    transplanted = scenario == "transplant_survival"
    assert len(surgery) == int(transplanted)
    if transplanted:
        assert cirrhosis.stop.notna().all()
        assert surgery.iloc[0].encounter == icu.iloc[0].id
        assert len(care[care.code.astype(str) == "439740005"]) == 5
        for code in ("313096", "313988", "856519", "391937"):
            orders = data["medications"][data["medications"].code.astype(str) == code]
            assert orders.stop.notna().all() and (orders.stop <= surgery.iloc[0].start).all(), "No native-liver prescriptions after graft"
    elif not fatal:
        assert cirrhosis.stop.isna().all(), "Medical recovery preserves cirrhosis"
        for code in ("13920009", "716203000"):
            rows = conditions[conditions.code.astype(str) == code]
            assert len(rows) == expected_episodes and rows.stop.notna().all(), "Acute disease closes between recurrences"
        assert data["medications"][data["medications"].code.astype(str) == "313096"].stop.isna().any(), "Chronic care resumes"
    if fatal:
        assert data["patients"][data["patients"].id == patient].deathdate.notna().all()
        endpoint = care.stop.max()
        assert not care.code.astype(str).eq("439740005").any()
        assert (data["medications"].stop.dropna() <= endpoint).all()
    labs = {item["code"]: item["unit"] for item in json.loads((ROOT / "synthea/liver_model.json").read_text())["labs"].values()}
    observed = data["observations"]
    observed = observed[observed.encounter.isin(care_ids) & observed.code.astype(str).isin(labs)].copy()
    panels = observed.groupby(["patient", "date"]).code.nunique()
    assert (panels == 9).all(), "Every acute assessment exports nine correlated labs"
    times = sorted(observed.date.unique())
    if expected_episodes == 1 and not transplanted:
        assert all(times[i] - times[i-1] == pd.Timedelta(days=1) for i in range(1, len(times))), "ED-to-ward/ICU handoff preserves the daily lab clock"
    for code, unit in labs.items():
        rows = observed[observed.code.astype(str) == code]
        assert set(rows.units) == {unit} and set(rows.type) == {"numeric"}
    csv = pd.read_csv(raw / "csv/observations.csv", dtype=str)
    csv.columns = csv.columns.str.lower()
    csv = csv[csv.encounter.isin(care_ids) & csv.code.isin(labs)]
    def normalize(frame):
        frame = frame[["patient", "encounter", "date", "code", "value", "units"]].copy()
        frame["date"] = pd.to_datetime(frame.date, utc=True)
        frame["code"] = frame.code.astype(str)
        frame["value"] = frame.value.astype(float)
        return frame.sort_values(["patient", "date", "code"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(normalize(csv), normalize(observed), check_dtype=False)
    print(f"PASS: integrated cirrhosis CSV/Parquet/notes {scenario}", flush=True)
    if scenario == "ward_recovery":
        check_legacy_and_empty_notes(folder, raw, encounters)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="cirrhosis-export-") as directory:
        for scenario in CASES:
            folder = Path(directory) / scenario
            folder.mkdir()
            run_case(folder, scenario)
