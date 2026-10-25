"""Generate seven deterministic liver test episodes and verify the real CSV/Parquet workflow.

Uses an isolated temporary directory; no production output, symlinks, or checkpoints change.
The fixture intentionally exercises several unrelated episodes in one patient; it is not an
incidence model. Requires the configured stock Synthea JAR and normal project dependencies.
"""
import json
import subprocess
import tempfile
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SCENARIOS = [(etiology, course) for etiology in
             ("alcohol_cirrhosis", "acetaminophen", "non_acetaminophen")
             for course in ("recovery", "deterioration")] + [("alcohol_cirrhosis", "postoperative")]


def fixture():
    states = {
        "Initial": {"type": "Initial", "direct_transition": "Adult"},
        "Adult": {"type": "Delay", "exact": {"quantity": 50, "unit": "years"},
                  "direct_transition": "Episode_0_0"},
        "Terminal": {"type": "Terminal"},
    }
    for i, (etiology, course) in enumerate(SCENARIOS):
        inputs = {"etiology": etiology, "trajectory": course, "initial_severity": 0.8,
                  "preexisting_cirrhosis": etiology == "alcohol_cirrhosis", "renal_baseline": 1.1,
                  "transplant_status": "recipient" if course == "postoperative" else "none",
                  "alcohol_use": "unknown" if etiology == "alcohol_cirrhosis" else "not_applicable"}
        episode = [{"type": "Encounter", "encounter_class": "inpatient", "codes": [
            {"system": "SNOMED-CT", "code": "183452005", "display": "Inpatient stay"}]}]
        episode += [{"type": "SetAttribute", "attribute": "liver_" + key, "value": value}
                    for key, value in inputs.items()]
        episode += [{"type": "CallSubmodule", "submodule": "liver/initialize"}]
        for day in range(4):
            episode.append({"type": "CallSubmodule", "submodule": "liver/advance"})
        episode += [{"type": "CallSubmodule", "submodule": "liver/finish"}, {"type": "EncounterEnd"}]
        for j, state in enumerate(episode):
            target = f"Episode_{i}_{j + 1}" if j + 1 < len(episode) else (
                f"Episode_{i + 1}_0" if i + 1 < len(SCENARIOS) else "Terminal")
            states[f"Episode_{i}_{j}"] = {**state, "direct_transition": target}
    return {"name": "Liver export fixture", "gmf_version": 2, "states": states}


def run_test(work):
    modules = work / "modules"
    modules.mkdir()
    (modules / "liver_export_fixture.json").write_text(json.dumps(fixture()))
    raw = work / "raw"
    parquet = work / "parquet"
    classpath = "synthea:synthea/bin/resources:synthea/bin/synthea-with-dependencies.jar"
    command = ["java", "-Xmx2g", "-cp", classpath, "App", "District of Columbia", "Washington",
               "-c", "synthea/synthea.properties", "-d", "synthea/modules", "-d", str(modules),
               "-m", "liver_export_fixture", "-p", "1", "-a", "55-56", "-s", "3003", "-cs", "3003",
               "-r", "20261024", "--generate.thread_pool_size=1",
               "--lifecycle.death_by_natural_causes=false", "--exporter.clinical_note.export=false",
               "--exporter.csv.excluded_files=patient_expenses.csv,claims.csv,claims_transactions.csv",
               f"--exporter.baseDirectory={raw}"]
    subprocess.run(command, cwd=ROOT, check=True, stdout=subprocess.DEVNULL)
    subprocess.run(["uv", "run", "python", "3_convert_to_parquet.py", "--path", str(raw),
                    "--output", str(parquet), "--no_claims", "--threads", "1"],
                   cwd=ROOT, check=True, stdout=subprocess.DEVNULL)

    config = json.loads((ROOT / "synthea/liver_model.json").read_text())
    codes = [lab["code"] for lab in config["labs"].values()]
    csv = pd.read_csv(raw / "csv/observations.csv", dtype=str)
    csv.columns = csv.columns.str.lower()
    csv = csv[csv.code.isin(codes)].copy()
    assert len(csv) == 7 * 5 * 9, f"Missing/duplicate fixture observations: {len(csv)}"
    encounters = pd.read_csv(raw / "csv/encounters.csv", dtype=str)
    encounters.columns = encounters.columns.str.lower()
    ordered = encounters[encounters.id.isin(csv.encounter)].sort_values("start")
    assert len(ordered) == len(SCENARIOS), "One distinct encounter per episode"
    for encounter, (etiology, course) in zip(ordered.id, SCENARIOS):
        panel = csv[csv.encounter == encounter].pivot(index="date", columns="code", values="value").astype(float)
        assert panel.shape == (5, 9), f"Five complete daily panels for {etiology}/{course}"
        times = pd.to_datetime(panel.index)
        assert all((times[1:] - times[:-1]) == pd.Timedelta(days=1)), "Actual 24-hour lab intervals"
        inr = panel[config["labs"]["inr"]["code"]]
        ast = panel[config["labs"]["ast"]["code"]]
        assert (inr.iloc[-1] > inr.iloc[0]) == (course == "deterioration"), "Exported function trajectory"
        assert ast.iloc[-1] < ast.iloc[0], "Falling AST includes worsening function"

    with duckdb.connect() as con:
        observed = con.execute("SELECT * FROM read_parquet(?)", [str(parquet / "observations.parquet")]).df()
        dictionary = con.execute("SELECT * FROM read_parquet(?) WHERE vocab='LOINC'",
                                 [str(parquet / "codes.parquet")]).df()
    observed = observed[observed.code.astype(str).isin(codes)].copy()
    for lab in config["labs"].values():
        before = csv[csv.code == lab["code"]]
        after = observed[observed.code.astype(str) == lab["code"]]
        assert set(before.units) == {lab["unit"]} == set(after.units), f"Units: {lab['code']}"
        assert set(before.type) == {"numeric"} == set(after.type), "Numeric export type"
        entry = dictionary[dictionary.code.astype(str) == lab["code"]]
        assert len(entry) == 1 and pd.notna(entry.iloc[0].omop_concept_id), f"Athena mapping: {lab['code']}"
        assert entry.iloc[0].omop_domain_id == "Measurement", "Measurement terminology domain"
    # Compare patient/encounter/date/code/value/units together so equal values in wrong encounters
    # or dates cannot conceal an export regression.
    fields = ["patient", "encounter", "date", "code", "value", "units"]
    def normalize(frame):
        frame = frame[fields].copy()
        frame["date"] = pd.to_datetime(frame.date, utc=True)
        frame["code"] = frame.code.astype(str)
        frame["value"] = frame.value.astype(float)
        return frame.sort_values(["patient", "encounter", "date", "code"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(normalize(csv), normalize(observed), check_dtype=False)
    print("PASS: 7 seeded episodes, 315 observations, 9 LOINC/unit/OMOP mappings, CSV→Parquet equality")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="synthea-liver-") as directory:
        run_test(Path(directory))
