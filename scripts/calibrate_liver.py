#!/usr/bin/env python3
"""Fit the pooled ICU transplant fraction in the real stock Synthea engine."""
import argparse
import copy
import hashlib
import json
import subprocess
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path

import build_liver_modules as builder
from liver_cohort import summarize

ROOT = Path(__file__).resolve().parents[1]
CLASSPATH = "synthea:synthea/bin/resources:synthea/bin/synthea-with-dependencies.jar"


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")


def driver(entry_path="direct_icu"):
    if entry_path == "cirrhosis":
        parent = json.loads((ROOT / "synthea/modules/cirrhosis.json").read_text())
        parent["name"] = "Cirrhosis index decompensation calibration"
        parent["remarks"] += ["Calibration conditions on diagnosis and one decompensation. Clinical outcomes are unforced."]
        states = parent["states"]
        states["Initial"] = {"type": "Initial", "direct_transition": "Calibration_referral"}
        states["Calibration_referral"] = {"type": "CallSubmodule", "submodule": "liver/referral", "direct_transition": "Calibration_destination"}
        states["Calibration_destination"] = {"type": "Simple", "conditional_transition": [
            {"condition": builder.attr("referred_to_mguh", "==", True), "transition": "Onset_Entry"},
            {"transition": "Terminal"}]}
        states["End_Diagnosis_Encounter"]["direct_transition"] = "Calibration_present"
        states["Calibration_present"] = {"type": "Delay", "exact": {"quantity": 1, "unit": "days"},
                                         "direct_transition": "Complication_Encounter"}
        # Observe one index course. Recurrent admissions cannot enlarge its denominator.
        states["End_Recovery_Followup"]["direct_transition"] = "Terminal"
        return parent
    return {"name": "Liver referral calibration", "gmf_version": 2, "states": {
        "Initial": {"type": "Initial", "direct_transition": "Referral"},
        "Referral": {"type": "CallSubmodule", "submodule": "liver/referral", "direct_transition": "Destination"},
        "Destination": {"type": "Simple", "conditional_transition": [
            {"condition": builder.attr("referred_to_mguh", "==", True), "transition": "ICU"},
            {"transition": "Terminal"}]},
        "ICU": {"type": "CallSubmodule", "submodule": "liver/critical_care", "direct_transition": "Terminal"},
        "Terminal": {"type": "Terminal"},
    }}


def run_cohort(config, seeds, folder, scenario_key="calibration"):
    builder.validate(config)
    folder.mkdir(parents=True, exist_ok=True)
    for name, module in builder.build(config).items():
        write_json(folder / "modules/liver" / (name + ".json"), module)
    scenario = {**config[scenario_key], "seeds": seeds}
    write_json(folder / "scenario.json", scenario)
    write_json(folder / "model.json", config)
    write_json(folder / "driver.json", driver(scenario.get("entry_path", "direct_icu")))
    shards = scenario["simulation"].get("parallel_shards_per_seed", 1)
    jobs = [(seed, shard) for seed in seeds for shard in range(shards)]
    def run_seed(job):
        seed, shard = job
        log_path = folder / f"engine-{seed}-{shard}.log"
        with log_path.open("w") as log:
            result = subprocess.run(["java", "-Xmx1g", "-cp", CLASSPATH,
                "scripts/LiverCalibration.java", str(folder.resolve()), str(seed), str(shard), str(shards)], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        if result.returncode:
            raise RuntimeError(f"Synthea calibration failed; see {log_path}")
    # Each shard has its own JVM. The harness consumes the same cohort draws before
    # selecting patient indices, so worker layout cannot change the cohort definition.
    with ThreadPoolExecutor(max_workers=min(9, len(jobs))) as pool:
        list(pool.map(run_seed, jobs))
    with (folder / "episodes.jsonl").open("w") as combined:
        for seed, shard in jobs:
            with (folder / f"episodes-{seed}-{shard}.jsonl").open() as part:
                for line in part:
                    combined.write(line)
    with (folder / "episodes.jsonl").open() as stream:
        summary = summarize((json.loads(line) for line in stream), config[scenario_key])
    write_json(folder / "summary.json", summary)
    return summary


def fit(config, folder, runner=run_cohort, scenario_key="calibration"):
    """Bracket and bisect one identifiable parameter; validation seeds never enter the fit."""
    simulation = config[scenario_key]["simulation"]
    target = config[scenario_key]["icu_transplant_target"]
    section = "cirrhosis_integration" if config[scenario_key].get("entry_path") == "cirrhosis" else "care"
    multiplier = config[section]["evaluation_odds_multiplier"]
    low, high = 0.0, None
    history, best = [], None
    for iteration in range(simulation["max_fit_iterations"]):
        candidate = copy.deepcopy(config)
        candidate[section]["evaluation_odds_multiplier"] = multiplier
        summary = runner(candidate, simulation["fit_seeds"], folder / f"fit_{iteration:02d}")
        rate = summary["icu_transplant"]["rate"]
        if rate is None:
            raise ValueError("No referred ICU entrants; increase the regional cohort size")
        difference = abs(rate - target)
        history.append({"iteration": iteration, "evaluation_odds_multiplier": multiplier,
                        "summary": summary})
        print(f"Fit {iteration}: evaluation odds ×{multiplier:.5f}; ICU transplants {rate:.2%}", flush=True)
        if best is None or difference < best[0]:
            best = (difference, candidate, summary)
        if difference <= simulation["fit_tolerance"]:
            break
        if rate < target:
            low = multiplier
            if multiplier >= 64:
                break
        else:
            high = multiplier
        multiplier = (low + high) / 2 if high is not None else min(64, max(1, multiplier * 2))
    return best[1], best[2], history


def assess_validation(summary, scenario):
    rate = summary["icu_transplant"]["rate"]
    tolerance = scenario["simulation"]["validation_tolerance"]
    # A tiny fixture cannot certify the population target even if its point estimate matches.
    enough = summary["icu_transplant"]["denominator"] >= 1000
    return {
        "minimum_1000_icu_entrants": enough,
        "transplant_target_within_tolerance": rate is not None and abs(rate - scenario["icu_transplant_target"]) <= tolerance,
        "referral_target_within_tolerance": summary["referral"]["rate"] is not None and abs(summary["referral"]["rate"] - scenario["referral_share"]) <= tolerance,
        # A completed transplant is an observed endpoint even if later follow-up is censored.
        "no_unresolved_transplant_outcomes": summary["outcomes"]["unresolved_without_transplant"] == 0,
        "no_unresolved_non_icu_courses": summary.get("non_icu_outcomes", {}).get("unresolved", 0) == 0,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "synthea/liver_model.json")
    parser.add_argument("--output", type=Path, default=ROOT / "output/liver_calibration")
    parser.add_argument("--fit", action="store_true", help="Fit evaluation odds using only the fit seeds")
    parser.add_argument("--pathway", choices=("reference_icu", "cirrhosis"), default="reference_icu",
                        help="Direct-ICU reference cohort or the integrated cirrhosis ED/ward pathway")
    parser.add_argument("--apply", action="store_true", help="Write the fitted config and regenerate modules after validation passes")
    parser.add_argument("--regional-cases-per-seed", type=int, help="Override cohort size; small runs cannot certify the target")
    args = parser.parse_args()
    if args.apply and not args.fit:
        parser.error("--apply requires --fit")
    if args.output.exists() and any(args.output.iterdir()):
        parser.error("Output directory must be empty so previous evidence is not overwritten")
    config = json.loads(args.config.read_text())
    scenario_key = "cirrhosis_calibration" if args.pathway == "cirrhosis" else "calibration"
    section = "cirrhosis_integration" if args.pathway == "cirrhosis" else "care"
    if args.regional_cases_per_seed is not None:
        config[scenario_key]["simulation"]["regional_cases_per_seed"] = args.regional_cases_per_seed
    builder.validate(config)
    runner = partial(run_cohort, scenario_key=scenario_key)
    if args.fit:
        config, training, history = fit(config, args.output, runner=runner, scenario_key=scenario_key)
    else:
        training, history = None, []
    scenario = config[scenario_key]
    print("Running independent validation seeds…", flush=True)
    validation = runner(config, scenario["simulation"]["validation_seeds"], args.output / "validation")
    checks = assess_validation(validation, scenario)
    if args.fit:
        checks["training_target_within_tolerance"] = abs(training["icu_transplant"]["rate"] - scenario["icu_transplant_target"]) <= scenario["simulation"]["fit_tolerance"]
    passed = all(checks.values())
    model_json = json.dumps(config, sort_keys=True, allow_nan=False)
    report = {
        "status": "scenario_target_validated" if passed else "target_not_validated",
        "scope": "User-assumed referral and pooled ICU transplant rates only; other clinical outcomes are uncalibrated.",
        "model_sha256": hashlib.sha256(model_json.encode()).hexdigest(),
        "parent_sha256": hashlib.sha256((ROOT / "synthea/modules/cirrhosis.json").read_bytes()).hexdigest() if args.pathway == "cirrhosis" else None,
        "engine_sha256": hashlib.sha256((ROOT / "synthea/bin/synthea-with-dependencies.jar").read_bytes()).hexdigest(),
        "scenario": scenario,
        "parameter": section + ".evaluation_odds_multiplier",
        "evaluation_odds_multiplier": config[section]["evaluation_odds_multiplier"],
        "fit_history": history,
        "validation": validation,
        "checks": checks,
        "applied": bool(args.apply and passed),
    }
    write_json(args.output / "fitted_model.json", config)
    write_json(args.output / "report.json", report)
    if args.apply and passed:
        write_json(args.config, config)
        for name, module in builder.build(config).items():
            write_json(ROOT / "synthea/modules/liver" / (name + ".json"), module)
    print(json.dumps({"status": report["status"], "checks": checks,
                      "referral": validation["referral"], "icu_transplant": validation["icu_transplant"],
                      "report": str(args.output / "report.json")}, indent=2), flush=True)
    if not passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
