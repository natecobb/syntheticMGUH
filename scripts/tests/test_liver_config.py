"""Reject unsafe model configurations and stale generated GMF artifacts."""
import copy
import importlib.util
import json
import unittest
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
spec = importlib.util.spec_from_file_location("liver_builder", ROOT / "scripts/build_liver_modules.py")
builder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(builder)


class LiverConfigTest(unittest.TestCase):
    def setUp(self):
        self.config = json.loads((ROOT / "synthea/liver_model.json").read_text())

    def test_generated_modules_are_current_and_loadable_graphs(self):
        builder.validate(self.config)
        for name, expected in builder.build(self.config).items():
            with self.subTest(module=name):
                actual = json.loads((ROOT / f"synthea/modules/liver/{name}.json").read_text())
                self.assertEqual(expected, actual)
                states = actual["states"]
                for state in states.values():
                    targets = [state["direct_transition"]] if "direct_transition" in state else []
                    for key in ("conditional_transition", "distributed_transition"):
                        targets += [branch["transition"] for branch in state.get(key, [])]
                    distribution = [branch["distribution"] for branch in state.get("distributed_transition", [])]
                    if distribution and all(isinstance(p, (int, float)) for p in distribution):
                        self.assertAlmostEqual(sum(distribution), 1, places=12)
                        self.assertTrue(all(0 <= p <= 1 for p in distribution))
                    for target in targets:
                        self.assertIn(target, states)

    def test_probability_gaps_excess_and_nonfinite_values_rejected(self):
        for value in (0, 1.2, -0.1, float("nan"), float("inf")):
            with self.subTest(value=value):
                config = copy.deepcopy(self.config)
                config["profiles"]["alcohol_cirrhosis"]["outcomes"]["severe"]["continue"] = value
                with self.assertRaises(ValueError):
                    builder.validate(config)

    def test_invalid_physiology_or_intervals_rejected(self):
        edits = [
            lambda c: c.update(step_hours=168),
            lambda c: c["severity_bands"].update(moderate=0.8),
            lambda c: c["profiles"]["acetaminophen"].update(recovery_days=0),
            lambda c: c["profiles"]["acetaminophen"]["deltas"].update(sodium=-200),
            lambda c: c["profiles"]["acetaminophen"]["complications"].update(severe=[2, 0, 0]),
            lambda c: c["labs"]["lactate"].update(unit=""),
            lambda c: c["baseline"].update(creatinine=0),
        ]
        for edit in edits:
            config = copy.deepcopy(self.config)
            edit(config)
            with self.assertRaises(ValueError):
                builder.validate(config)

    def test_invalid_care_parameters_rejected(self):
        edits = [
            lambda c: c["care"].update(ward_days=0),
            lambda c: c["care"]["candidate_probability"].update(acute=1.1),
            lambda c: c["care"]["graft_probability_per_day"].update(chronic=-0.1),
            lambda c: c["care"].update(evaluation_odds_multiplier=-1),
            lambda c: c["care"].update(maximum_wait_days=14),
            lambda c: c["care"].update(followup_days_after_discharge=[7, 7]),
            lambda c: c["care"].update(followup_days_after_discharge=[7, 8]),
            lambda c: c["care"].update(surgery_hours=0),
            lambda c: c["care"].update(rejection_probability=float("nan")),
            lambda c: c["cirrhosis_integration"].update(initial_severity_range=[0, 1]),
            lambda c: c["cirrhosis_integration"].update(initial_severity_range=[0.8, 0.2]),
            lambda c: c["cirrhosis_integration"].update(initial_severity_range=[0.2, float("nan")]),
            lambda c: c["cirrhosis_integration"].update(workup_hours=0),
            lambda c: c["cirrhosis_integration"].update(workup_hours=25),
            lambda c: c["cirrhosis_integration"].update(evaluation_odds_multiplier=-0.1),
            lambda c: c["cirrhosis_calibration"]["cohort"].update(initial_severity_range=[0.75, 1]),
            lambda c: c["cirrhosis_calibration"]["cohort"].update(etiology_weights={"acetaminophen": 1.0}),
        ]
        for edit in edits:
            config = copy.deepcopy(self.config)
            edit(config)
            with self.assertRaises(ValueError):
                builder.validate(config)


if __name__ == "__main__":
    unittest.main()
