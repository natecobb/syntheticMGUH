"""Deterministic copies of the actual cirrhosis parent; production has no forced-outcome flags."""
import json
from pathlib import Path

from liver_care_fixtures import ROOT, builder, definitions

SCENARIOS = ("ward_recovery", "ward_deterioration", "ward_death", "icu_recovery", "icu_death",
             "transplant_survival", "postoperative_death", "recurrence", "alcohol_known",
             "external_medication", "external_cirrhosis", "existing_recipient", "cancel_death", "busy_owner",
             "routine_followup", "chronic_death")


def write_case(folder, scenario, generator=False):
    care_scenario = {"icu_death": "death", "ward_death": "death", "external_medication": "transplant_survival"}.get(scenario,
        scenario if scenario in ("transplant_survival", "postoperative_death") else "recovery")
    modules = definitions(care_scenario)
    severity = 0.8 if scenario in ("icu_recovery", "icu_death", "transplant_survival", "postoperative_death", "external_medication") else 0.3
    for state in modules["cirrhosis_decompensation"]["states"].values():
        if state.get("attribute") == "liver_initial_severity":
            state["distribution"] = {"kind": "EXACT", "parameters": {"value": severity}}
    if scenario == "ward_deterioration":
        states = modules["choose_outcome"]["states"]
        previous = states["Check_due"]["conditional_transition"][0]["transition"]
        for index, outcome in enumerate(builder.OUTCOMES):
            states[f"Fixture_ward_{index}"] = {**builder.probability("p_" + outcome, float(outcome == "deterioration")),
                "direct_transition": f"Fixture_ward_{index+1}" if index+1 < len(builder.OUTCOMES) else "Choose"}
        states["Fixture_context"] = {"type": "Simple", "conditional_transition": [
            {"condition": builder.attr("level_of_care", "==", "ward"), "transition": "Fixture_ward_0"},
            {"transition": previous}]}
        states["Check_due"]["conditional_transition"][0]["transition"] = "Fixture_context"
    prefix = "cirrhosis_fixture_" + scenario
    for name, module in modules.items():
        target = folder / prefix / (name + ".json")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(module).replace('"liver/', '"' + prefix + '/'))
    parent = json.loads((ROOT / "synthea/modules/cirrhosis.json").read_text())
    states = parent["states"]
    states["Initial"] = {"type": "Initial", "direct_transition": "Fixture_adult" if generator else "Fixture_cause"}
    states["Fixture_adult"] = {"type": "Delay", "exact": {"quantity": 50, "unit": "years"}, "direct_transition": "Fixture_cause"}
    states["Fixture_cause"] = {**builder.set_value("cirrhosis_etiology", "alcohol" if scenario == "alcohol_known" else "unspecified"),
        "direct_transition": "Onset_Entry"}
    states["End_Diagnosis_Encounter"]["direct_transition"] = "Disease_Death" if scenario in ("cancel_death", "chronic_death") else "Fixture_complication_delay"
    states["Fixture_complication_delay"] = {"type": "Delay", "exact": {"quantity": 1, "unit": "days"}, "direct_transition": "Complication_Encounter"}
    states["Manage_Delay"]["distributed_transition"] = [{"distribution": 1.0, "transition": "Complication_Encounter"}]
    if scenario == "routine_followup":
        states["End_Diagnosis_Encounter"]["direct_transition"] = "Manage_Delay"
        states["Manage_Delay"]["distributed_transition"] = [{"distribution": 1.0, "transition": "Followup_Encounter"}]
        # Supply a deterministic appointment instead of waiting for the Lifecycle wellness module.
        states["Routine_Followup_Encounter"].pop("wellness")
        states["Routine_Followup_Encounter"].update(encounter_class="ambulatory", codes=states["Diagnosis_Encounter"]["codes"])
    # Unit tests stop at actual return to Manage_Delay. Export fixtures have a finite course.
    if generator:
        if scenario == "recurrence":
            states["End_Recovery_Followup"]["direct_transition"] = "Fixture_count"
            states["Fixture_count"] = {"type": "Counter", "attribute": "fixture_recoveries", "action": "increment", "direct_transition": "Fixture_recur"}
            states["Fixture_recur"] = {"type": "Simple", "conditional_transition": [
                {"condition": {"condition_type": "Attribute", "attribute": "fixture_recoveries", "operator": "<", "value": 2}, "transition": "Manage_Delay"},
                {"transition": "Terminal"}]}
        else:
            states["End_Recovery_Followup"]["direct_transition"] = "Terminal"
    parent = json.loads(json.dumps(parent).replace('"liver/', '"' + prefix + '/'))
    path = folder / (scenario + ".json")
    path.write_text(json.dumps(parent))
    return {"scenario": scenario, "path": str(path), "prefix": prefix}


def write_all(folder):
    cases = [write_case(folder, scenario) for scenario in SCENARIOS]
    (folder / "cases.json").write_text(json.dumps(cases))
