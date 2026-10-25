"""Denominator, censoring, weighting, and holdout protections for liver calibration."""
import copy
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from calibrate_liver import assess_validation, driver, fit
from liver_cohort import summarize, validate


class LiverCalibrationTest(unittest.TestCase):
    def setUp(self):
        self.model = json.loads((ROOT / "synthea/liver_model.json").read_text())
        self.scenario = self.model["calibration"]

    def row(self, **edits):
        return {"seed": 1, "etiology": "acetaminophen", "referred": True, "icu_admitted": True,
                "transplanted": False, "dead": False, "complete": True,
                "ventilation": False, "rrt": False, "vasopressors": False,
                "icu_closed": True, "icu_days": 10, **edits}

    def test_postoperative_deaths_count_and_censored_entrants_stay_in_denominator(self):
        rows = [self.row(transplanted=True, dead=True, days_to_transplant=4), self.row(),
                self.row(complete=False, icu_closed=False), self.row(referred=False, icu_admitted=False)]
        result = summarize(rows, self.scenario)
        self.assertEqual(result["referral"]["rate"], 0.75)
        self.assertEqual(result["icu_transplant"]["numerator"], 1)
        self.assertEqual(result["icu_transplant"]["denominator"], 3)
        self.assertEqual(sum(result["outcomes"].values()), 3)
        self.assertEqual(result["outcomes"]["transplanted_then_died"], 1)
        self.assertEqual(result["outcomes"]["unresolved_without_transplant"], 1)
        self.assertFalse(assess_validation(result, self.scenario)["no_unresolved_transplant_outcomes"])
        self.assertIsNone(result["annualization"])

    def test_incomplete_recipient_followup_does_not_hide_an_observed_transplant(self):
        rows = [self.row(transplanted=True, complete=False, days_to_transplant=12)] + [self.row()] * 3
        result = summarize(rows, self.scenario)
        self.assertEqual(result["censored_courses"], 1)
        self.assertEqual(result["censored_after_transplant"], 1)
        self.assertEqual(result["icu_transplant"]["rate"], 0.25)
        self.assertTrue(assess_validation(result, self.scenario)["no_unresolved_transplant_outcomes"])

    def test_annualization_applies_referral_once_then_icu_then_transplant(self):
        self.scenario["cohort"].update(regional_cases_per_year=1000, icu_fraction_among_referred=0.4)
        result = summarize([self.row() for _ in range(20)], self.scenario)["annualization"]
        self.assertEqual(result["expected_mguh_cases"], 250)
        self.assertEqual(result["expected_mguh_icu_cases"], 100)
        self.assertEqual(result["target_mguh_icu_transplants"], 25)
        self.assertEqual(result["weight_per_simulated_icu_patient_year"], 5)

    def test_empty_and_tiny_cohorts_do_not_validate(self):
        for rows in ([], [self.row(referred=False, icu_admitted=False)]):
            summary = summarize(rows, self.scenario)
            self.assertIsNone(summary["icu_transplant"]["rate"])
            self.assertFalse(all(assess_validation(summary, self.scenario).values()))
        summary = summarize([self.row(transplanted=True, days_to_transplant=3)] + [self.row()] * 3, self.scenario)
        self.assertEqual(summary["icu_transplant"]["rate"], 0.25)
        self.assertFalse(assess_validation(summary, self.scenario)["minimum_1000_icu_entrants"])

    def test_invalid_targets_and_seed_leakage_rejected(self):
        edits = [lambda c: c.update(referral_share=1.1), lambda c: c.update(icu_transplant_target=float("nan")),
                 lambda c: c["simulation"].update(validation_seeds=c["simulation"]["fit_seeds"]),
                 lambda c: c["cohort"]["etiology_weights"].update(acetaminophen=0.9),
                 lambda c: c["cohort"].update(icu_fraction_among_referred=1.5)]
        for edit in edits:
            config = copy.deepcopy(self.scenario)
            edit(config)
            with self.assertRaises(ValueError):
                validate(config)

    def test_fit_uses_only_training_seeds_and_does_not_mutate_input(self):
        self.model["care"]["evaluation_odds_multiplier"] = 1.0
        before = copy.deepcopy(self.model)
        def simulated_rate(config, seeds, folder):
            self.assertEqual(seeds, self.scenario["simulation"]["fit_seeds"])
            multiplier = config["care"]["evaluation_odds_multiplier"]
            return {"icu_transplant": {"rate": multiplier / (multiplier + 4)}}
        fitted, summary, history = fit(self.model, Path("unused"), runner=simulated_rate)
        self.assertLessEqual(abs(summary["icu_transplant"]["rate"] - 0.25), self.scenario["simulation"]["fit_tolerance"])
        self.assertGreater(len(history), 1)
        self.assertEqual(self.model, before)
        self.assertNotEqual(fitted["care"]["evaluation_odds_multiplier"], 1.0)

    def test_integrated_denominator_excludes_ward_only_cases(self):
        scenario = self.model["cirrhosis_calibration"]
        rows = [self.row(icu_entry_route="direct"), self.row(icu_entry_route="from_ward", transplanted=True,
                dead=True, days_to_transplant=12), self.row(icu_admitted=False),
                self.row(icu_admitted=False, dead=True), self.row(referred=False, icu_admitted=False)]
        result = summarize(rows, scenario)
        self.assertEqual(result["referral"]["rate"], 4 / 5)
        self.assertEqual(result["icu_among_referred"]["rate"], 2 / 4)
        self.assertEqual(result["icu_transplant"]["rate"], 1 / 2)
        self.assertEqual(result["non_icu_outcomes"], {"recovered": 1, "died": 1, "unresolved": 0})
        self.assertEqual(result["icu_entry_routes"]["from_ward"]["numerator"], 1)
        with self.assertRaises(ValueError):
            summarize(rows, self.scenario)
        with self.assertRaises(ValueError):
            summarize([self.row(referred=False)], scenario)
        unresolved = summarize([self.row(icu_admitted=False, complete=False)], scenario)
        self.assertFalse(assess_validation(unresolved, scenario)["no_unresolved_non_icu_courses"])

    def test_cirrhosis_fit_does_not_change_reference_parameter(self):
        scenario = self.model["cirrhosis_calibration"]
        before = copy.deepcopy(self.model)
        def simulated_rate(config, seeds, folder):
            self.assertEqual(seeds, scenario["simulation"]["fit_seeds"])
            self.assertEqual(config["care"], before["care"])
            multiplier = config["cirrhosis_integration"]["evaluation_odds_multiplier"]
            return {"icu_transplant": {"rate": multiplier / (multiplier + 4)}}
        fitted, summary, _ = fit(self.model, Path("unused"), runner=simulated_rate, scenario_key="cirrhosis_calibration")
        self.assertLessEqual(abs(summary["icu_transplant"]["rate"] - scenario["icu_transplant_target"]), scenario["simulation"]["fit_tolerance"])
        self.assertEqual(fitted["care"], before["care"])
        self.assertEqual(self.model, before)

    def test_integrated_driver_preserves_clinical_decisions(self):
        parent = json.loads((ROOT / "synthea/modules/cirrhosis.json").read_text())
        states = driver("cirrhosis")["states"]
        for name in ("Complication_Encounter", "Liver_Complication", "After_Liver_Complication", "Manage_Delay"):
            self.assertEqual(states[name], parent["states"][name])
        self.assertEqual(states["End_Recovery_Followup"]["direct_transition"], "Terminal")


if __name__ == "__main__":
    unittest.main()
