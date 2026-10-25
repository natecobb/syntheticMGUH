"""Test-only deterministic consumers; no forced outcomes are exposed in production modules."""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import build_liver_modules as builder

SCENARIOS = ("recovery", "death", "evaluation_recovery", "not_candidate", "wait_death",
             "waiting_recovery", "wait_deterioration", "no_graft", "transplant_survival", "postoperative_death", "rejection", "no_support",
             "prolonged_recovery", "prolonged_death", "stochastic")


def definitions(scenario):
    config = json.loads((ROOT / "synthea/liver_model.json").read_text())
    config["care"]["candidate_probability"] = dict.fromkeys(("acute", "chronic"), 0 if scenario == "not_candidate" else 1)
    config["care"]["graft_probability_per_day"] = dict.fromkeys(("acute", "chronic"), 0 if scenario in ("no_graft", "wait_death") else 1)
    config["care"]["rejection_probability"] = 1 if scenario == "rejection" else 0
    if scenario == "stochastic":
        config = json.loads((ROOT / "synthea/liver_model.json").read_text())
    builder.validate(config)
    modules = builder.build(config)
    if scenario != "stochastic":
        for state in modules["initialize"]["states"].values():
            if state.get("attribute") in ("liver_renal_draw", "liver_respiratory_draw", "liver_circulatory_draw"):
                state["distribution"] = {"kind": "EXACT", "parameters": {"value": 0.99 if scenario == "no_support" else 0.01}}
        states = modules["choose_outcome"]["states"]
        def force(name, outcome):
            # Real named-probability sampler remains in the loop; replace only its test inputs.
            for i, key in enumerate(builder.OUTCOMES):
                states[f"Test_{name}_{i}"] = {**builder.probability("p_" + key, float(key == outcome)),
                    "direct_transition": f"Test_{name}_{i+1}" if i+1 < len(builder.OUTCOMES) else "Choose"}
            return f"Test_{name}_0"
        if scenario.startswith("prolonged_"):
            target = "Test_prolonged"
            states[target] = {"type": "Simple", "conditional_transition": [
                {"condition": builder.attr("elapsed_days", ">=", 45),
                 "transition": force("late", "death" if scenario == "prolonged_death" else "recovery")},
                {"transition": force("early", "continue")}]}
        elif scenario in ("recovery", "no_support", "death"):
            outcome = "death" if scenario == "death" else "recovery"
            target = force("always", outcome)
        else:
            target = "Test_context"
            states[target] = {"type": "Simple", "conditional_transition": [
                {"condition": builder.attr("transplant_status", "==", "recipient"),
                 "transition": force("postop", "death" if scenario == "postoperative_death" else "recovery")},
                {"condition": builder.attr("transplant_status", "==", "evaluating"),
                 "transition": force("evaluation", "recovery" if scenario == "evaluation_recovery" else "continue")},
                {"condition": builder.all_of(builder.attr("transplant_status", "==", "listed"),
                     builder.attr("waiting_days", ">=", 19)),
                 "transition": force("long_wait", "recovery" if scenario == "no_graft" else "continue")},
                {"condition": builder.attr("transplant_status", "==", "listed"),
                 "transition": force("wait", {"wait_death": "death", "waiting_recovery": "recovery", "wait_deterioration": "deterioration"}.get(scenario, "continue"))},
                {"condition": builder.attr("evaluation_done", "==", True), "transition": force("declined", "recovery")},
                {"transition": force("entry", "transplant_evaluation")}]}
        states["Check_due"]["conditional_transition"][0]["transition"] = target
    return modules


def write_modules(folder, scenario, prefix="liver"):
    modules = definitions(scenario)
    for name, module in modules.items():
        text = json.dumps(module).replace('"liver/', '"' + prefix + '/')
        target = folder / prefix / (name + ".json")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)


def driver(etiology, scenario, prefix="liver", adult_delay=True):
    inputs = {"etiology": etiology, "trajectory": "stable", "initial_severity": 0.5 if scenario == "no_support" else 0.8,
              "preexisting_cirrhosis": etiology in builder.CHRONIC_ETIOLOGIES, "renal_baseline": 1.1,
              "transplant_status": "none", "alcohol_use": "active" if etiology == "alcohol_cirrhosis" else ("unknown" if etiology == "cirrhosis" else "not_applicable")}
    states = {"Initial": {"type": "Initial", "direct_transition": "Adult" if adult_delay else "Input_0"},
              "Terminal": {"type": "Terminal"}}
    if adult_delay:
        states["Adult"] = {"type": "Delay", "exact": {"quantity": 50, "unit": "years"}, "direct_transition": "Input_0"}
    for i, (key, value) in enumerate(inputs.items()):
        states[f"Input_{i}"] = {**builder.set_value(key, value),
            "direct_transition": f"Input_{i+1}" if i+1 < len(inputs) else "Care"}
    states["Care"] = {"type": "CallSubmodule", "submodule": prefix + "/critical_care", "direct_transition": "Terminal"}
    return {"name": "Liver care fixture", "gmf_version": 2, "states": states}


def write_engine_fixtures(folder):
    cases = []
    for scenario in SCENARIOS:
        prefix = "fixture_" + scenario
        write_modules(folder, scenario, prefix)
        for etiology in builder.ETIOLOGIES:
            definition = driver(etiology, scenario, prefix, adult_delay=False)
            path = folder / (scenario + "_" + etiology + ".json")
            path.write_text(json.dumps(definition))
            cases.append({"scenario": scenario, "etiology": etiology, "path": str(path)})
    (folder / "cases.json").write_text(json.dumps(cases))
