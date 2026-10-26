"""Deterministic transfer workflows, incremental publication and note checkpoints."""

from datetime import datetime, timedelta, timezone
import json
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import duckdb

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts import process_notes, process_transfers as transfers, transfer_store  # noqa: E402
from scripts.patient_identity import allocate_ssns  # noqa: E402
from scripts.transfer_signals import extract_signals  # noqa: E402
from scripts.note_identity import filter_notes_before_launch  # noqa: E402
from scripts import parquet_publication  # noqa: E402

BASE = datetime(2026, 1, 1, 10, tzinfo=timezone.utc)


def fixture() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    schemas = {
        "patients": "id VARCHAR, deathdate TIMESTAMPTZ, first_encounter_id VARCHAR",
        "encounters": "id VARCHAR, patient VARCHAR, start TIMESTAMPTZ, stop TIMESTAMPTZ, organization VARCHAR, provider VARCHAR, encounterclass VARCHAR, code VARCHAR, reasoncode VARCHAR, base_encounter_cost DOUBLE, total_claim_cost DOUBLE, payer_coverage DOUBLE",
        "organizations": "id VARCHAR, npi VARCHAR, name VARCHAR, utilization DOUBLE, revenue DOUBLE",
        "providers": "id VARCHAR, organization VARCHAR, npi VARCHAR, encounters DOUBLE, procedures DOUBLE",
        "conditions": "patient VARCHAR, encounter VARCHAR, start TIMESTAMPTZ, stop TIMESTAMPTZ, code VARCHAR",
        "procedures": "patient VARCHAR, encounter VARCHAR, start TIMESTAMPTZ, stop TIMESTAMPTZ, code VARCHAR, base_cost DOUBLE",
        "observations": "patient VARCHAR, encounter VARCHAR, date TIMESTAMPTZ, code VARCHAR, value VARCHAR, units VARCHAR, category VARCHAR",
        "medications": "patient VARCHAR, encounter VARCHAR, start TIMESTAMPTZ, stop TIMESTAMPTZ, code VARCHAR, totalcost DOUBLE",
        "devices": "patient VARCHAR, encounter VARCHAR, start TIMESTAMPTZ, stop TIMESTAMPTZ, code VARCHAR, udi VARCHAR",
        "careplans": "id VARCHAR, patient VARCHAR, encounter VARCHAR, start TIMESTAMPTZ, stop TIMESTAMPTZ, code VARCHAR",
        "notes": "patient VARCHAR, encounter VARCHAR, date TIMESTAMPTZ, note_text VARCHAR",
        "claims": "id VARCHAR, patientid VARCHAR, appointmentid VARCHAR, providerid VARCHAR, supervisingproviderid VARCHAR, outstanding1 DOUBLE",
        "claims_transactions": "id VARCHAR, claimid VARCHAR, patientid VARCHAR, appointmentid VARCHAR, providerid VARCHAR, placeofservice VARCHAR, type VARCHAR, amount DOUBLE",
        "codes": "vocab VARCHAR, code VARCHAR, description VARCHAR",
    }
    for table, schema in schemas.items():
        con.execute(f"CREATE TABLE {table} ({schema})")
    for org, npi in (("g", "1427145176"), ("w", "1548378235")):
        con.execute(
            "INSERT INTO organizations VALUES (?, ?, ?, 999, 999)", [org, npi, org]
        )
        con.execute(
            "INSERT INTO providers VALUES (?, ?, '1234567890', 999, 999)",
            [org + "doc", org],
        )
    return con


def add_patient(
    con,
    patient="p1",
    policy="cardiac",
    setting="emergency",
    trigger=True,
    surgery=False,
    death=None,
    followon=True,
):
    source = "g" if policy == "cardiac" else "w"
    enc = patient + "e"
    con.execute("INSERT INTO patients VALUES (?, ?, ?)", [patient, death, enc])
    con.execute(
        "INSERT INTO encounters VALUES (?, ?, ?, ?, ?, ?, ?, '50849002', NULL, 100, 1000, 700)",
        [enc, patient, BASE, BASE + timedelta(days=2), source, source + "doc", setting],
    )
    if trigger:
        if policy == "cardiac":
            con.execute(
                "INSERT INTO conditions VALUES (?, ?, ?, NULL, '22298006')",
                [patient, enc, BASE + timedelta(hours=2)],
            )
        else:
            con.execute(
                "INSERT INTO procedures VALUES (?, ?, ?, ?, '1076381000000104', 50)",
                [patient, enc, BASE + timedelta(hours=2), BASE + timedelta(hours=2)],
            )
    if surgery:
        con.execute(
            "INSERT INTO procedures VALUES (?, ?, ?, ?, ?, 500)",
            [
                patient,
                enc,
                BASE + timedelta(days=1),
                BASE + timedelta(days=1, hours=4),
                "18027006" if policy == "liver" else "232717009",
            ],
        )
    con.execute(
        "INSERT INTO observations VALUES (?, ?, ?, 'lab', 'known', 'mg/dL', 'laboratory'), (?, ?, ?, 'lab', 'FUTURE_VALUE', 'mg/dL', 'laboratory')",
        [
            patient,
            enc,
            BASE + timedelta(hours=1),
            patient,
            enc,
            BASE + timedelta(hours=20),
        ],
    )
    con.execute(
        "INSERT INTO medications VALUES (?, ?, ?, ?, 'pressor', 75)",
        [patient, enc, BASE, BASE + timedelta(days=2)],
    )
    con.execute(
        "INSERT INTO devices VALUES (?, ?, ?, ?, 'ventilator', ?)",
        [patient, enc, BASE, BASE + timedelta(days=2), patient + "device"],
    )
    con.execute(
        "INSERT INTO careplans VALUES (?, ?, ?, ?, ?, 'support')",
        [patient + "cp", patient, enc, BASE, BASE + timedelta(days=2)],
    )
    con.execute(
        "INSERT INTO notes VALUES (?, ?, ?, 'FUTURE_OUTCOME: transplant or death')",
        [patient, enc, BASE],
    )
    con.execute(
        "INSERT INTO claims VALUES (?, ?, ?, ?, ?, 1000)",
        [patient + "claim", patient, enc, source + "doc", source + "doc"],
    )
    con.execute(
        "INSERT INTO claims_transactions VALUES (?, ?, ?, ?, ?, ?, 'TRANSFEROUT', 123)",
        [
            patient + "transaction",
            patient + "claim",
            patient,
            enc,
            source + "doc",
            source,
        ],
    )
    if followon:
        for suffix, kind, start, stop in (
            ("ward", "inpatient", 49, 72),
            ("clinic", "ambulatory", 96, 97),
        ):
            con.execute(
                "INSERT INTO encounters VALUES (?, ?, ?, ?, ?, ?, ?, '305342007', NULL, 10, 100, 70)",
                [
                    patient + suffix,
                    patient,
                    BASE + timedelta(hours=start),
                    BASE + timedelta(hours=stop),
                    source,
                    source + "doc",
                    kind,
                ],
            )


def save(con, path):
    path.mkdir(exist_ok=True)
    for name in transfer_store.tables(con):
        con.execute(
            f"COPY {name} TO {transfer_store.literal(path / (name + '.parquet'))} (FORMAT PARQUET)"
        )


def read(path, table):
    with duckdb.connect() as con:
        return con.execute(
            "SELECT * FROM read_parquet(?) ORDER BY ALL",
            [str(path / (table + ".parquet"))],
        ).fetchall()


class TransferTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.source, self.output = self.root / "source", self.root / "transfers"
        self.con = fixture()

    def tearDown(self):
        self.con.close()
        self.tmp.cleanup()

    def test_note_launch_boundary_uses_new_york_after_utc_parsing(self):
        for session_zone in ("UTC", "America/New_York"):
            self.con.execute(f"SET TimeZone='{session_zone}'")
            self.con.execute("DELETE FROM notes")
            self.con.execute("""INSERT INTO notes VALUES
                ('p','e','2010-01-01T04:59:59Z','prelaunch'),
                ('p','e','2010-01-01T05:00:00Z','at launch'),
                ('p','e','2010-01-02T00:00:00Z','after launch')""")
            self.assertEqual(filter_notes_before_launch(self.con, "2010-01-01"), 1)
            self.assertEqual(
                self.con.execute(
                    "SELECT note_text FROM notes ORDER BY date"
                ).fetchall(),
                [("at launch",), ("after launch",)],
            )

    def run_pipeline(self):
        save(self.con, self.source)
        return transfers.run(self.source, self.output)

    def load_output(self):
        con = duckdb.connect()
        transfer_store.load_source(
            con, self.output.resolve(), transfer_store.inventory(self.output)
        )
        return con

    def test_population_growth_preserves_completed_partial_batches(self):
        spec = importlib.util.spec_from_file_location(
            "generation", ROOT / "2_generate_incremental.py"
        )
        generation = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(generation)
        completed = {"0": {"patients_requested": 50}}
        self.assertEqual(
            generation._batch_requests(100, 100, completed), {0: 50, 1: 50}
        )
        completed["1"] = {"patients_requested": 50}
        self.assertEqual(
            generation._batch_requests(250, 100, completed),
            {0: 50, 1: 50, 2: 100, 3: 50},
        )
        self.assertEqual(
            generation._batch_requests(100, 100, completed), {0: 50, 1: 50}
        )

    def test_source_conversion_publication_recovers_before_retry(self):
        self.source.mkdir()
        staged = []
        for name in ("a", "b"):
            temporary = self.source / f".tmp_{name}.parquet"
            final = self.source / f"{name}.parquet"
            temporary.write_bytes(("new-" + name).encode())
            final.write_bytes(("old-" + name).encode())
            staged.append((name, temporary, final))
        original_replace = Path.replace

        def interrupted(path, target):
            if path.name == ".tmp_b.parquet":
                raise OSError("interrupted publication")
            return original_replace(path, target)

        with patch.object(Path, "replace", interrupted):
            with self.assertRaises(OSError):
                parquet_publication.publish(self.source, staged)
        parquet_publication.recover(self.source)
        self.assertEqual((self.source / "a.parquet").read_bytes(), b"new-a")
        self.assertEqual((self.source / "b.parquet").read_bytes(), b"new-b")
        self.assertFalse((self.source / parquet_publication.JOURNAL).exists())

    def test_routing_and_outcomes(self):
        for policy in ("cardiac", "liver"):
            for setting in ("emergency", "inpatient"):
                add_patient(self.con, policy + setting, policy, setting, surgery=True)
        add_patient(self.con, "romi", trigger=False)
        add_patient(self.con, "resolving", "liver", trigger=False)
        add_patient(self.con, "recovery", "liver", surgery=False)
        add_patient(
            self.con, "death", "liver", death=BASE + timedelta(days=3), followon=False
        )
        result = self.run_pipeline()
        self.assertEqual(result["transfers"], 6)
        with self.load_output() as out:
            self.assertEqual(
                out.execute(
                    "SELECT count(*) FROM transfers WHERE patient IN ('romi','resolving')"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                out.execute(
                    "SELECT count(*) FROM encounters WHERE id LIKE '%clinic' AND organization != CASE WHEN patient LIKE 'cardiac%' OR patient='romi' THEN 'g' ELSE 'w' END"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                out.execute(
                    "SELECT count(*) FROM encounters e JOIN transfers t ON e.id = t.destination_encounter WHERE e.encounterclass = 'inpatient'"
                ).fetchone()[0],
                6,
            )
            self.assertEqual(
                out.execute(
                    "SELECT count(*) FROM encounters WHERE id='cardiacemergencye' AND encounterclass='emergency'"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                out.execute("SELECT count(*) FROM procedures").fetchone()[0],
                self.con.execute("SELECT count(*) FROM procedures").fetchone()[0],
            )
            self.assertEqual(
                out.execute(
                    "SELECT deathdate FROM patients WHERE id='death'"
                ).fetchone(),
                self.con.execute(
                    "SELECT deathdate FROM patients WHERE id='death'"
                ).fetchone(),
            )

    def test_idempotent_and_append_preserves_existing_patient_rows(self):
        add_patient(self.con)
        self.run_pipeline()
        original = self.output.resolve()
        before = {p.stem: read(self.output, p.stem) for p in original.glob("*.parquet")}
        self.assertEqual(transfers.run(self.source, self.output)["status"], "unchanged")
        self.assertEqual(original, self.output.resolve())
        add_patient(self.con, "p2", "liver")
        # New provider cannot change an old patient's accepting clinician.
        self.con.execute("INSERT INTO providers VALUES ('newdoc','w','1234567891',0,0)")
        result = self.run_pipeline()
        self.assertEqual(result["new_patients"], 1)
        with self.load_output() as out:
            for table in before:
                col = transfer_store.patient_column(
                    table, transfer_store.columns(out, table)
                )
                if col:
                    old_rows = out.execute(
                        f"SELECT * FROM {table} WHERE {col}='p1' ORDER BY ALL"
                    ).fetchall()
                    self.assertEqual(old_rows, before[table], table)
        self.assertEqual(transfers.run(self.source, self.output)["status"], "unchanged")

    def test_reordered_input_does_not_change_patient_effect(self):
        add_patient(self.con)
        add_patient(self.con, "p2", "liver")
        self.run_pipeline()
        before = read(self.output, "transfers")
        self.con.execute(
            "CREATE OR REPLACE TABLE encounters AS SELECT * FROM encounters ORDER BY id DESC"
        )
        result = self.run_pipeline()
        self.assertEqual(result["new_patients"], 0)
        self.assertEqual(read(self.output, "transfers"), before)

    def test_receiving_admission_code_survives_nontransfer_patient_addition(self):
        add_patient(self.con)
        self.run_pipeline()
        add_patient(self.con, "local", trigger=False)
        self.run_pipeline()
        with self.load_output() as out:
            missing = out.execute("""SELECT count(*) FROM transfers t
                JOIN encounters e ON t.destination_encounter=e.id
                LEFT JOIN codes c ON c.vocab='SNOMED' AND CAST(c.code AS VARCHAR)=CAST(e.code AS VARCHAR)
                WHERE c.code IS NULL""").fetchone()[0]
            self.assertEqual(missing, 0)

    def test_changed_history_and_policy_fail_without_publishing(self):
        add_patient(self.con)
        self.run_pipeline()
        prior = self.output.resolve()
        self.con.execute("UPDATE observations SET value='changed'")
        with self.assertRaisesRegex(ValueError, "patient histories changed"):
            self.run_pipeline()
        self.assertEqual(self.output.resolve(), prior)
        config = json.loads(transfers.DEFAULT_POLICY.read_text())
        config["transport_minutes"] += 1
        policy = self.root / "policy.json"
        policy.write_text(json.dumps(config))
        with self.assertRaisesRegex(ValueError, "policy/version changed"):
            transfers.run(self.source, self.output, policy)

    def test_failure_before_atomic_commit_is_restart_safe(self):
        add_patient(self.con)
        self.run_pipeline()
        prior = self.output.resolve()
        add_patient(self.con, "p2", "liver")
        with patch.object(
            transfer_store.os, "replace", side_effect=OSError("injected interruption")
        ):
            with self.assertRaisesRegex(OSError, "interruption"):
                self.run_pipeline()
        self.assertEqual(self.output.resolve(), prior)
        self.assertEqual(transfers.run(self.source, self.output)["transfers"], 2)

    def test_transport_keeps_continuous_support_and_financial_totals(self):
        add_patient(self.con, surgery=True)
        before = self.con.execute(
            "SELECT sum(total_claim_cost), sum(payer_coverage), sum(base_encounter_cost) FROM encounters"
        ).fetchone()
        claims = self.con.execute("SELECT * FROM claims_transactions").fetchall()
        self.run_pipeline()
        with self.load_output() as out:
            self.assertEqual(
                out.execute(
                    "SELECT sum(total_claim_cost), sum(payer_coverage), sum(base_encounter_cost) FROM encounters"
                ).fetchone(),
                before,
            )
            self.assertEqual(
                out.execute("SELECT * FROM claims_transactions").fetchall(), claims
            )
            self.assertEqual(
                out.execute("SELECT count(*) FROM medications").fetchone()[0], 1
            )
            self.assertEqual(
                out.execute("SELECT count(*) FROM devices").fetchone()[0], 1
            )
            self.assertEqual(
                out.execute(
                    "SELECT count(*) FROM transfer_orders WHERE prior_order_id IS NOT NULL"
                ).fetchone()[0],
                3,
            )
            self.assertEqual(
                out.execute("SELECT start, stop FROM medications").fetchone(),
                (BASE, BASE + timedelta(days=2)),
            )
            self.assertEqual(
                out.execute("SELECT sum(utilization) FROM organizations").fetchone()[0],
                out.execute("SELECT count(*) FROM encounters").fetchone()[0],
            )
            self.assertEqual(
                out.execute("SELECT sum(revenue) FROM organizations").fetchone()[0],
                before[0],
            )

    def test_notes_do_not_leak_future_outcomes(self):
        add_patient(self.con, policy="liver", surgery=True)
        self.run_pipeline()
        with self.load_output() as out:
            notes = out.execute(
                "SELECT note_type, note_text, date, authored_at FROM notes"
            ).fetchall()
            for kind, text, date, authored in notes:
                self.assertEqual(date, authored)
                self.assertNotIn("FUTURE_OUTCOME", text)
                if kind in {"transfer_summary", "admission"}:
                    self.assertNotIn("FUTURE_VALUE", text)
                    self.assertNotIn("18027006", text)
            self.assertEqual(
                out.execute("SELECT count(*) FROM transfer_source_notes").fetchone()[0],
                1,
            )

    def test_ed_recognition_survives_overlapping_icu_admission(self):
        add_patient(self.con, "overlap", followon=False)
        self.con.execute("UPDATE conditions SET start=?", [BASE])
        self.con.execute("UPDATE encounters SET stop=?", [BASE + timedelta(hours=1)])
        self.con.execute(
            """INSERT INTO encounters SELECT 'icu', patient, ?, ?, organization,
            provider, 'inpatient', '32485007', reasoncode, base_encounter_cost,
            total_claim_cost, payer_coverage FROM encounters""",
            [BASE + timedelta(minutes=55), BASE + timedelta(days=2)],
        )
        self.con.execute(
            """INSERT INTO procedures VALUES
            ('overlap','overlape',?,?,'stabilization',10),
            ('overlap','icu',?,?,'admission',10),
            ('overlap','icu',?,?,'415070008',500)""",
            [
                BASE,
                BASE + timedelta(minutes=55),
                BASE + timedelta(minutes=55),
                BASE + timedelta(minutes=85),
                BASE + timedelta(days=1),
                BASE + timedelta(days=1, hours=2),
            ],
        )
        self.assertEqual(self.run_pipeline()["transfers"], 1)
        with self.load_output() as out:
            row = out.execute(
                "SELECT recognition_encounter, source_encounter, recognized_at, departed_at, arrived_at FROM transfers"
            ).fetchone()
            self.assertEqual(
                row,
                (
                    "overlape",
                    "icu",
                    BASE,
                    BASE + timedelta(minutes=85),
                    BASE + timedelta(minutes=115),
                ),
            )
            self.assertEqual(
                out.execute(
                    "SELECT organization, stop FROM encounters WHERE id='overlape'"
                ).fetchone(),
                ("g", BASE + timedelta(hours=1)),
            )
            self.assertEqual(
                out.execute(
                    "SELECT start, stop FROM procedures ORDER BY start"
                ).fetchall(),
                self.con.execute(
                    "SELECT start, stop FROM procedures ORDER BY start"
                ).fetchall(),
            )
            self.assertEqual(
                out.execute(
                    "SELECT count(*) FROM procedures p JOIN encounters e ON p.encounter=e.id WHERE p.code='415070008' AND e.organization='w'"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                out.execute(
                    "SELECT count(*) FROM transfer_orders WHERE order_type='reconcile_continuing_treatment' AND code='pressor'"
                ).fetchone()[0],
                1,
            )
        self.assertEqual(self.run_pipeline()["status"], "unchanged")

    def test_destination_treatment_in_followon_stay_blocks_late_transfer(self):
        add_patient(self.con, "treated", followon=False)
        self.con.execute("UPDATE conditions SET start=?", [BASE])
        self.con.execute("UPDATE encounters SET stop=?", [BASE + timedelta(hours=1)])
        self.con.execute(
            """INSERT INTO encounters SELECT 'icu', patient, ?, ?, organization,
            provider, 'inpatient', '32485007', reasoncode, base_encounter_cost,
            total_claim_cost, payer_coverage FROM encounters""",
            [BASE + timedelta(minutes=55), BASE + timedelta(days=2)],
        )
        self.con.execute(
            "INSERT INTO procedures VALUES ('treated','icu',?,?,'415070008',500)",
            [BASE + timedelta(minutes=55), BASE + timedelta(minutes=85)],
        )
        self.assertEqual(self.run_pipeline()["transfers"], 0)
        with self.load_output() as out:
            self.assertIn(
                ("destination_treatment_precedes_possible_arrival",),
                out.execute("SELECT reason FROM transfer_audit").fetchall(),
            )

    def test_stabilization_wait_is_bounded(self):
        add_patient(self.con, "busy", followon=False)
        self.con.execute(
            "INSERT INTO procedures VALUES ('busy','busye',?,?,'stabilization',10)",
            [BASE + timedelta(hours=2), BASE + timedelta(hours=12)],
        )
        self.assertEqual(self.run_pipeline()["transfers"], 0)
        with self.load_output() as out:
            self.assertIn(
                ("procedure_overlaps_transport",),
                out.execute("SELECT reason FROM transfer_audit").fetchall(),
            )

    def test_first_encounter_validation_excludes_only_prelaunch_deaths(self):
        from scripts.test_parquet_load import TestResults, test_first_encounter_id

        self.con.execute("INSERT INTO patients VALUES ('old', '2009-12-28', NULL)")
        results = TestResults()
        test_first_encounter_id(
            self.con,
            {"patients", "encounters"},
            results,
            datetime(2010, 1, 1, tzinfo=timezone.utc),
        )
        self.assertFalse(results.failed)
        for patient, death in [("living", None), ("recent", "2020-01-01")]:
            with self.subTest(patient=patient):
                self.con.execute(
                    "INSERT INTO patients VALUES (?, ?, NULL)", [patient, death]
                )
                results = TestResults()
                test_first_encounter_id(
                    self.con,
                    {"patients", "encounters"},
                    results,
                    datetime(2010, 1, 1, tzinfo=timezone.utc),
                )
                self.assertEqual(len(results.failed), 1)
                self.con.execute("DELETE FROM patients WHERE id=?", [patient])

    def test_synthetic_ssns_reserve_all_values_and_preserve_existing(self):
        with duckdb.connect() as con:
            con.execute("CREATE TABLE patients(id VARCHAR, ssn VARCHAR)")
            con.execute("CREATE TABLE _existing_patients(id VARCHAR, ssn VARCHAR)")
            con.execute("INSERT INTO _existing_patients VALUES ('old','duplicate')")
            original = [
                ("b", "duplicate"),
                ("a", "duplicate"),
                ("c", "999-10-1000"),
                ("unknown", None),
            ]
            outputs = []
            for rows in (original, original[::-1]):
                con.execute("DELETE FROM patients")
                con.executemany("INSERT INTO patients VALUES (?,?)", rows)
                with patch("scripts.patient_identity.hashlib.sha256") as digest:
                    digest.return_value.hexdigest.return_value = "0"
                    changes = allocate_ssns(con, "_existing_patients")
                self.assertEqual(len(changes), 2)
                outputs.append(
                    con.execute("SELECT * FROM patients ORDER BY id").fetchall()
                )
                self.assertEqual(allocate_ssns(con, "_existing_patients"), [])
            self.assertEqual(outputs[0], outputs[1])
            self.assertEqual(
                outputs[0],
                [
                    ("a", "999-10-1001"),
                    ("b", "999-10-1002"),
                    ("c", "999-10-1000"),
                    ("unknown", None),
                ],
            )
            self.assertEqual(
                con.execute("SELECT * FROM _existing_patients").fetchall(),
                [("old", "duplicate")],
            )
            con.execute("DELETE FROM patients")
            con.execute(
                "INSERT INTO patients VALUES ('second','same'),('first','same')"
            )
            self.assertEqual(allocate_ssns(con)[0][:2], ("second", "same"))
            self.assertEqual(allocate_ssns(con), [])

    def test_ambiguous_and_impossible_cases_are_reported(self):
        add_patient(self.con, "date")
        self.con.execute(
            "UPDATE conditions SET start = '2026-01-01 05:00:00+00' WHERE patient='date'"
        )
        add_patient(self.con, "short")
        self.con.execute(
            "UPDATE encounters SET stop = ? WHERE id='shorte'",
            [BASE + timedelta(hours=2, minutes=30)],
        )
        add_patient(self.con, "fatal", death=BASE + timedelta(hours=3), followon=False)
        add_patient(self.con, "procedure", surgery=True)
        self.con.execute(
            "UPDATE procedures SET start=?, stop=? WHERE patient='procedure'",
            [BASE + timedelta(hours=2, minutes=40), BASE + timedelta(hours=4)],
        )
        self.assertEqual(self.run_pipeline()["transfers"], 0)
        with self.load_output() as out:
            reasons = {
                r[0]
                for r in out.execute("SELECT reason FROM transfer_audit").fetchall()
            }
            self.assertIn("date_only_diagnosis", reasons)
            self.assertIn("insufficient_or_invalid_encounter_time", reasons)
            self.assertIn("death_before_arrival_or_ambiguous_death_date", reasons)
            self.assertIn("destination_treatment_precedes_possible_arrival", reasons)

    def test_no_transplant_outcome_required_for_liver_transfer(self):
        add_patient(self.con, "recovered", "liver")
        add_patient(self.con, "transplanted", "liver", surgery=True)
        add_patient(
            self.con, "died", "liver", death=BASE + timedelta(days=3), followon=False
        )
        self.assertEqual(self.run_pipeline()["transfers"], 3)

    def test_explicit_signal_and_historical_mi(self):
        add_patient(self.con, "past")
        self.con.execute("UPDATE conditions SET start = ?", [BASE - timedelta(days=10)])
        add_patient(self.con, "explicit", trigger=False)
        self.con.execute(
            "CREATE TABLE transfer_signals(id VARCHAR, patient VARCHAR, encounter VARCHAR, policy VARCHAR, recognized_at TIMESTAMPTZ)"
        )
        self.con.execute(
            "INSERT INTO transfer_signals VALUES ('s','explicit','explicite','cardiac',?)",
            [BASE + timedelta(hours=2)],
        )
        self.assertEqual(self.run_pipeline()["transfers"], 1)

    def test_missing_destination_and_nested_output_fail_safely(self):
        add_patient(self.con)
        self.con.execute("DELETE FROM providers WHERE organization='w'")
        self.assertEqual(self.run_pipeline()["transfers"], 0)
        with self.assertRaisesRegex(ValueError, "separate"):
            transfers.run(self.source, self.source / "derived")

    def test_llm_checkpoint_multiple_notes_same_encounter_and_timestamp_precision(self):
        add_patient(self.con, followon=False)
        self.run_pipeline()
        with self.load_output() as out:
            rows = out.execute(
                "SELECT patient, CAST(date AS VARCHAR), encounter, note_text, 'rewritten', '{}', note_id, note_type, authored_at, author, transfer_id FROM notes ORDER BY date"
            ).fetchall()
            output = self.root / "notes_llm.parquet"
            process_notes._write_batch_to_parquet(out, rows[:2], output)
            process_notes._write_batch_to_parquet(out, rows[2:], output)
            process_notes._write_batch_to_parquet(out, rows[:2], output)
            self.assertEqual(
                out.execute(
                    "SELECT count(*) FROM read_parquet(?)", [str(output)]
                ).fetchone()[0],
                3,
            )
            self.assertEqual(
                out.execute(
                    "SELECT count(DISTINCT date) FROM read_parquet(?)", [str(output)]
                ).fetchone()[0],
                3,
            )
            self.assertEqual(
                out.execute(
                    "SELECT count(DISTINCT encounter) FROM read_parquet(?)",
                    [str(output)],
                ).fetchone()[0],
                2,
            )

    def test_identical_source_notes_keep_distinct_repeatable_ids(self):
        add_patient(self.con, trigger=False)
        self.con.execute("INSERT INTO notes SELECT * FROM notes")
        self.run_pipeline()
        with self.load_output() as out:
            self.assertEqual(
                out.execute("SELECT count(DISTINCT note_id) FROM notes").fetchone()[0],
                2,
            )
        self.assertEqual(transfers.run(self.source, self.output)["status"], "unchanged")

    def test_exact_export_markers_are_removed_from_prose(self):
        text = "Clinical facts\n<!-- synthea-transfer-v1 policy=cardiac recognized_at=2026-01-01T12:34:56Z code=22298006 -->\nNext section"
        clean, signals = extract_signals(text)
        self.assertNotIn("synthea-transfer", clean)
        self.assertIn("Next section", clean)
        self.assertEqual(
            signals,
            [
                dict(
                    policy="cardiac",
                    recognized_at="2026-01-01T12:34:56Z",
                    source_code="22298006",
                )
            ],
        )

    def test_legacy_rewrite_adopts_only_unambiguous_source_identity(self):
        add_patient(self.con, trigger=False)
        self.run_pipeline()
        with self.load_output() as out:
            out.execute(
                "CREATE TABLE existing_notes AS SELECT patient, encounter, CAST(date AS DATE) AS date, note_text, 'old rewritten' AS natural_note_text, '{}' AS llm_metadata FROM notes"
            )
            out.execute("ALTER TABLE existing_notes ALTER COLUMN date TYPE TIMESTAMPTZ")
            process_notes._ensure_note_identity(out, "existing_notes", legacy=True)
            self.assertEqual(
                out.execute(
                    "SELECT note_id, authored_at FROM existing_notes"
                ).fetchall(),
                out.execute("SELECT note_id, authored_at FROM notes").fetchall(),
            )

    def test_llm_enrichment_does_not_reintroduce_future_facts(self):
        add_patient(self.con, followon=False)
        self.run_pipeline()
        output = self.root / "llm.parquet"
        prompts = []

        def fake_llm(**kwargs):
            prompts.append(kwargs["note_text"])
            return kwargs["note_text"], {
                "prompt_tokens_actual": 1,
                "completion_tokens": 1,
                "elapsed_time_seconds": 0,
            }

        with patch.object(
            process_notes, "transform_note_with_llm", side_effect=fake_llm
        ):
            result = process_notes.transform_notes_pass2(
                str(self.output / "notes.parquet"),
                str(output),
                base_url="http://example.invalid",
                model="mock",
                api_key="mock",
                concurrency=1,
                temperature=0,
                style_resolver=process_notes.StyleResolver(),
            )
            again = process_notes.transform_notes_pass2(
                str(self.output / "notes.parquet"),
                str(output),
                base_url="http://example.invalid",
                model="mock",
                api_key="mock",
                concurrency=1,
                temperature=0,
                style_resolver=process_notes.StyleResolver(),
            )
        self.assertEqual(result["transformed"], 3)
        self.assertEqual(again["pending_notes"], 0)
        self.assertNotIn("FUTURE_VALUE", prompts[0])
        self.assertNotIn("FUTURE_VALUE", prompts[1])


if __name__ == "__main__":
    unittest.main()
