"""Verify ICU/transplant clinical events and notes through actual Synthea CSV and Parquet export."""
import json
import subprocess
import tempfile
from pathlib import Path

import duckdb
import pandas as pd

from liver_care_fixtures import ROOT, driver, write_modules

CASES = [("alcohol_cirrhosis", "recovery"), ("alcohol_cirrhosis", "transplant_survival"),
         ("acetaminophen", "wait_death"), ("acetaminophen", "postoperative_death"),
         ("non_acetaminophen", "rejection"), ("non_acetaminophen", "not_candidate"),
         ("alcohol_cirrhosis", "no_support")]
CARE_CODES = {"305351004", "305342007", "439740005"}


def run_case(folder, etiology, scenario):
    modules = folder / "modules"
    write_modules(modules, scenario)
    (modules / "liver_care_fixture.json").write_text(json.dumps(driver(etiology, scenario)))
    raw, parquet = folder / "raw", folder / "parquet"
    fatal = scenario in ("wait_death", "postoperative_death", "death")
    command = ["java", "-Xmx2g", "-cp", "synthea:synthea/bin/resources:synthea/bin/synthea-with-dependencies.jar",
        "App", "District of Columbia", "Washington", "-c", "synthea/synthea.properties",
        "-d", "synthea/modules", "-d", str(modules), "-m", "liver_care_fixture",
        "-p", "1", "-a", "55-56", "-s", "3003", "-cs", "3003", "-r", "20261025",
        "--generate.thread_pool_size=1", "--lifecycle.death_by_natural_causes=false",
        f"--generate.only_dead_patients={str(fatal).lower()}", "--exporter.clinical_note.export=true",
        "--exporter.csv.excluded_files=patient_expenses.csv,claims.csv,claims_transactions.csv",
        f"--exporter.baseDirectory={raw}"]
    with (folder / "generation.log").open("w") as log:
        subprocess.run(command, cwd=ROOT, check=True, stdout=log, stderr=subprocess.STDOUT, timeout=120)
    with (folder / "conversion.log").open("w") as log:
        subprocess.run(["uv", "run", "python", "3_convert_to_parquet.py", "--path", str(raw), "--output", str(parquet),
            "--no_claims", "--include_notes", "--threads", "1"], cwd=ROOT, check=True, stdout=log, stderr=subprocess.STDOUT, timeout=120)
    with duckdb.connect() as con:
        data = {}
        for name in ("patients", "encounters", "observations", "conditions", "procedures", "careplans", "devices", "medications", "notes", "codes"):
            path = parquet / (name + ".parquet")
            if path.exists():
                data[name] = con.read_parquet(str(path)).df()
            else:
                # The repository converter omits tables whose source CSV has only a header.
                assert name in {"procedures", "careplans", "devices", "medications"}
                source = raw / "csv" / (name + ".csv")
                empty = pd.read_csv(source, dtype=str) if source.exists() else pd.DataFrame(
                    columns=["patient", "encounter", "code", "start", "stop"])
                assert empty.empty, f"Nonempty {name} was lost during conversion"
                empty.columns = empty.columns.str.lower()
                data[name] = empty
    enc = data["encounters"]
    care = enc[enc.code.astype(str).isin(CARE_CODES)]
    icu = care[care.code.astype(str) == "305351004"]
    assert len(icu) == 1 and set(icu.encounterclass) == {"inpatient"}, "Observable ICU admission"
    patient = icu.iloc[0].patient
    assert patient in set(data["patients"].id), "Patient identity"
    care_ids = set(care.id)
    for table in ("observations", "conditions", "procedures", "careplans", "devices", "medications", "notes"):
        rows = data[table][data[table].encounter.isin(care_ids)]
        assert set(rows.patient) <= {patient}, f"{table} patient/encounter linkage"
        assert rows.encounter.isin(enc.id).all(), f"{table} encounter references"
    notes = data["notes"]
    assert care_ids <= set(notes.encounter), "Each care encounter retains its clinical note"
    assert notes.encounter.notna().all(), "No unlinked notes"
    procedures = data["procedures"]
    surgery = procedures[procedures.code.astype(str) == "18027006"]
    expected_surgery = scenario in ("transplant_survival", "postoperative_death", "rejection")
    assert len(surgery) == int(expected_surgery), "One transplant at most"
    if expected_surgery:
        assert surgery.iloc[0].encounter == icu.iloc[0].id, "Surgery in the ICU inpatient encounter"
        evaluation = procedures[procedures.code.astype(str) == "1076381000000104"]
        assert evaluation.iloc[0].start < surgery.iloc[0].start < surgery.iloc[0].stop <= icu.iloc[0].stop
    if not fatal:
        ward = care[care.code.astype(str) == "305342007"]
        assert len(ward) == 1 and ward.iloc[0].start >= icu.iloc[0].stop, "ICU precedes ward care"
        if expected_surgery:
            followups = care[care.code.astype(str) == "439740005"].sort_values("start")
            assert len(followups) == 5 and set(followups.encounterclass) == {"ambulatory"}, "Recipient follow-up"
            for days, date in zip((7, 14, 30, 90, 180), followups.start):
                assert date - ward.iloc[0].stop == pd.Timedelta(days=days), "Follow-up schedule"
            for med in ("198377", "199058"):
                orders = data["medications"][data["medications"].code.astype(str) == med]
                assert orders.stop.isna().any(), "Ongoing immunosuppression survives export"
            levels = data["observations"][data["observations"].code.astype(str) == "11253-2"]
            assert len(levels) == 5 and set(levels.units) == {"ng/mL"}, "Drug level monitoring"
    else:
        assert not data["patients"][data["patients"].id == patient].deathdate.isna().any(), "Death exported"
        assert set(care.code.astype(str)) == {"305351004"}, "No ward care or follow-up after death"
        end = icu.iloc[0].stop
        for table in ("observations", "procedures", "devices", "medications"):
            rows = data[table][data[table].encounter.isin(care_ids)]
            time_col = "date" if table == "observations" else "start"
            assert (rows[time_col] <= end).all(), "No clinical events after terminal ICU endpoint"
            if table != "observations":
                assert (rows.stop.dropna() <= end).all(), "No treatment extends beyond death"
    for table, codes in (("careplans", {"53950000", "385971003"}), ("medications", {"373112"})):
        rows = data[table][data[table].code.astype(str).isin(codes)]
        assert rows.stop.notna().all() and (rows.stop >= rows.start).all(), "Support has explicit stop times"
        if scenario == "no_support":
            assert rows.empty, "Unsupported complications do not acquire support"
    devices = data["devices"][data["devices"].code.astype(str).isin({"449071006", "463140004"})]
    assert devices.stop.notna().all() and (devices.stop > devices.start).all(), "Exact support intervals"
    assert (devices.start >= icu.iloc[0].start).all() and (devices.stop <= icu.iloc[0].stop).all(), "Support contained in ICU"
    assert devices.empty == (scenario == "no_support"), "Devices only when support is indicated"
    if scenario == "rejection":
        rejection = data["conditions"][data["conditions"].code.astype(str) == "431222008"]
        assert len(rejection) == 1 and rejection.iloc[0].stop > rejection.iloc[0].start, "Bounded rejection"
    # Compare all clinical values/IDs in the course, not just counts; descriptions normalize into codes.
    for table in ("observations", "procedures", "careplans", "devices", "medications", "conditions"):
        source = raw / "csv" / (table + ".csv")
        if not source.exists():
            assert data[table].empty, f"{table} has records without a source CSV"
            continue
        csv = pd.read_csv(source, dtype=str)
        csv.columns = csv.columns.str.lower()
        before = csv[csv.encounter.isin(care_ids)].copy()
        after = data[table][data[table].encounter.isin(care_ids)].copy()
        fields = ["patient", "encounter", "code", "date"] if table == "observations" else ["patient", "encounter", "code", "start", "stop"]
        if table == "observations": fields += ["value", "units", "type"]
        def normalized(frame):
            result = frame[fields].copy()
            for field in fields:
                if field in ("start", "stop", "date"):
                    parsed = pd.to_datetime(result[field])
                    # Stock care-plan/condition CSVs have dates, which the existing converter
                    # interprets at New York midnight. Device intervals preserve precise UTC times.
                    if table in ("careplans", "conditions") and parsed.dt.tz is None:
                        parsed = parsed.dt.tz_localize("America/New_York")
                    result[field] = pd.to_datetime(parsed, utc=True).astype("datetime64[ns, UTC]")
                else:
                    result[field] = result[field].fillna("").astype(str)
            return result.sort_values(fields).reset_index(drop=True)
        pd.testing.assert_frame_equal(normalized(before), normalized(after), check_dtype=False)
    print(f"PASS: CSV/Parquet/notes {etiology}/{scenario}", flush=True)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="liver-care-export-") as directory:
        for etiology, scenario in CASES:
            folder = Path(directory) / (etiology + "_" + scenario)
            folder.mkdir()
            run_case(folder, etiology, scenario)
