#!/usr/bin/env python3
"""Compile the reviewed liver-model configuration into stock Synthea GMF submodules."""

import argparse
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ETIOLOGIES = ("alcohol_cirrhosis", "acetaminophen", "non_acetaminophen", "cirrhosis")
CHRONIC_ETIOLOGIES = ("alcohol_cirrhosis", "cirrhosis")
COURSES = ("recovery", "deterioration", "stable", "postoperative")
OUTCOMES = ("recovery", "deterioration", "death", "transplant_evaluation", "continue")
ORGANS = ("renal", "respiratory", "circulatory")


def validate(config):
    """Fail before writing modules if a configuration can silently bias transitions."""
    from liver_care import validate as validate_care
    from liver_cohort import validate as validate_cohort
    from liver_cirrhosis import validate as validate_cirrhosis
    validate_care(config)
    validate_cohort(config["calibration"])
    validate_cirrhosis(config)
    if "cirrhosis_calibration" in config:
        scenario = config["cirrhosis_calibration"]
        validate_cohort(scenario)
        if scenario.get("entry_path") != "cirrhosis" or scenario["referral_share"] != config["calibration"]["referral_share"]:
            raise ValueError("Cirrhosis calibration must use the integrated path and shared referral assumption")
        if scenario["cohort"]["initial_severity_range"] != config["cirrhosis_integration"]["initial_severity_range"]:
            raise ValueError("Cirrhosis calibration must use the production entry severity range")
    if config["step_hours"] != 24:
        raise ValueError("v1 probabilities are defined per 24-hour step")
    labs = set(config["labs"])
    if labs != {"inr", "bilirubin", "creatinine", "sodium", "albumin", "glucose", "lactate", "ast", "alt"}:
        raise ValueError("The nine required liver laboratory measurements must be configured")
    if set(config["profiles"]) != set(ETIOLOGIES) | {"postoperative"}:
        raise ValueError("All supported liver etiologies and the postoperative profile are required")
    bands = config["severity_bands"]
    if not 0 < bands["moderate"] < bands["severe"] < 1:
        raise ValueError("Severity bands must increase within (0, 1)")
    for baseline in ("baseline", "cirrhosis_baseline"):
        if set(config[baseline]) != labs or any(
            not math.isfinite(v) or v <= 0 for v in config[baseline].values()
        ):
            raise ValueError(f"Invalid {baseline}")
    for lab, metadata in config["labs"].items():
        if not all(metadata.get(key) for key in ("code", "display", "unit")):
            raise ValueError(f"Missing terminology metadata for {lab}")
    for name, profile in config["profiles"].items():
        for key in ("recovery_days", "deterioration_days", "injury_decay_days"):
            if not math.isfinite(profile[key]) or profile[key] <= 0:
                raise ValueError(f"{name}.{key} must be positive")
        if set(profile["deltas"]) != labs:
            raise ValueError(f"{name} must configure every lab delta")
        for lab, delta in profile["deltas"].items():
            if not math.isfinite(delta) or any(
                config[b][lab] + min(0, delta * 1.05) <= 0
                for b in ("baseline", "cirrhosis_baseline")
            ):
                raise ValueError(f"{name}.{lab} can produce an invalid concentration")
        for band in ("mild", "moderate", "severe"):
            probabilities = profile["outcomes"][band]
            if set(probabilities) != set(OUTCOMES) or any(
                not math.isfinite(p) or not 0 <= p <= 1 for p in probabilities.values()
            ) or not math.isclose(sum(probabilities.values()), 1, abs_tol=1e-12, rel_tol=0):
                raise ValueError(f"{name}.{band} outcomes must be exhaustive probabilities")
            complications = profile["complications"][band]
            if len(complications) != len(ORGANS) or any(
                not math.isfinite(p) or not 0 <= p <= 1 for p in complications
            ):
                raise ValueError(f"Invalid complications for {name}.{band}")


def attr(name, operator, value=None):
    condition = {"condition_type": "Attribute", "attribute": "liver_" + name, "operator": operator}
    if value is not None:
        condition["value"] = value
    return condition


def all_of(*conditions):
    return {"condition_type": "And", "conditions": list(conditions)}


def one_of(name, values):
    return {"condition_type": "Or", "conditions": [attr(name, "==", value) for value in values]}


def set_value(name, value):
    return {"type": "SetAttribute", "attribute": "liver_" + name, "value": value}


def expr(name, expression):
    return {"type": "SetAttribute", "attribute": "liver_" + name, "expression": expression}


def copy_value(name, source):
    return {"type": "SetAttribute", "attribute": "liver_" + name, "value_attribute": "liver_" + source}


def probability(name, value):
    # Synthea's named transition probabilities require Double, including zero/one.
    return {"type": "SetAttribute", "attribute": "liver_" + name,
            "distribution": {"kind": "EXACT", "parameters": {"value": value}}}


class Graph:
    def __init__(self, name, config):
        self.document = {"name": "Liver " + name, "gmf_version": 2,
                         "remarks": ["Generated by scripts/build_liver_modules.py; edit synthea/liver_model.json or the compiler.",
                                     *config["assumptions"]],
                         "states": {"Terminal": {"type": "Terminal"}}}
        self.states = self.document["states"]

    def chain(self, prefix, states, target):
        for i, state in enumerate(states):
            self.states[f"{prefix}_{i}"] = {**state, "direct_transition":
                                           f"{prefix}_{i + 1}" if i + 1 < len(states) else target}
        return prefix + "_0" if states else target

    def branch(self, name, choices, fallback):
        self.states[name] = {"type": "Simple", "conditional_transition": [
            {"condition": condition, "transition": target} for condition, target in choices
        ] + [{"transition": fallback}]}

    def error(self, message):
        return self.chain("Error_" + str(len(self.states)), [set_value("error", message)], "Terminal")

    def start(self, target):
        self.states["Initial"] = {"type": "Initial", "direct_transition": target}


def valid_inputs():
    return all_of(one_of("etiology", ETIOLOGIES), one_of("trajectory", COURSES),
                  attr("initial_severity", ">=", 0), attr("initial_severity", "<=", 1))


def valid_baseline():
    return all_of(attr("renal_baseline", ">", 0), attr("renal_baseline", "<=", 20),
                  one_of("preexisting_cirrhosis", [True, False]),
                  one_of("transplant_status", ["none", "evaluating", "listed", "recipient"]),
                  one_of("alcohol_use", ["unknown", "active", "abstinent", "not_applicable"]))


def validate_context(graph, target):
    graph.branch("Validate_postoperative", [(all_of(attr("trajectory", "==", "postoperative"),
                 attr("transplant_status", "!=", "recipient")),
                 graph.error("Postoperative course requires transplant_status=recipient"))], target)
    graph.branch("Validate_cirrhosis", [(all_of(one_of("etiology", CHRONIC_ETIOLOGIES),
                 attr("preexisting_cirrhosis", "==", False)),
                 graph.error("A cirrhosis profile requires preexisting_cirrhosis=true"))], "Validate_postoperative")
    graph.branch("Validate_baseline", [(valid_baseline(), "Validate_cirrhosis")],
                 graph.error("Invalid renal baseline, cirrhosis flag, alcohol use, or transplant status"))
    return "Validate_baseline"


def build(config):
    graphs = {}
    init = graphs["initialize"] = Graph("initialize", config)
    initialize = init.chain("Initialize", [
        {"type": "SetAttribute", "attribute": "liver_error"},
        {"type": "Counter", "attribute": "liver_episode", "action": "increment"},
        set_value("model_version", config["version"]), set_value("active", True),
        set_value("entry_path", "direct_icu"),
        set_value("elapsed_days", 0), set_value("phase_start_day", 0),
        set_value("last_assessed_day", -1), set_value("last_outcome_day", 0),
        set_value("suggested_outcome", "continue"),
        copy_value("phase_initial_severity", "initial_severity"),
        copy_value("phase_initial_injury", "initial_severity"),
        copy_value("current_trajectory", "trajectory"),
        *[{"type": "SetAttribute", "attribute": "liver_" + organ + "_draw",
           "distribution": {"kind": "UNIFORM", "parameters": {"low": 0, "high": 1}}} for organ in ORGANS],
        {"type": "SetAttribute", "attribute": "liver_lab_scale",
         "distribution": {"kind": "UNIFORM", "parameters": {"low": 0.95, "high": 1.05}}},
        {"type": "CallSubmodule", "submodule": "liver/assess"}
    ], "Terminal")
    validate_context(init, initialize)
    defaults = [
        ("preexisting_cirrhosis", expr("preexisting_cirrhosis", "#s{liver_etiology} = 'alcohol_cirrhosis' or #s{liver_etiology} = 'cirrhosis'")),
        ("renal_baseline", set_value("renal_baseline", config["baseline"]["creatinine"])),
        ("transplant_status", set_value("transplant_status", "none")),
        ("alcohol_use", expr("alcohol_use", "if #s{liver_etiology} = 'alcohol_cirrhosis' or #s{liver_etiology} = 'cirrhosis' then 'unknown' else 'not_applicable'"))
    ]
    target = "Validate_baseline"
    for name, state in reversed(defaults):
        setter = init.chain("Default_" + name, [state], target)
        init.branch("Check_" + name, [(attr(name, "is nil"), setter)], target)
        target = "Check_" + name
    init.branch("Validate_inputs", [(valid_inputs(), target)], init.error("Set a supported etiology, trajectory, and initial_severity in [0,1]"))
    init.branch("Check_active", [(attr("active", "==", True), init.error("Finish the active episode before initializing another"))], "Validate_inputs")
    init.start("Check_active")

    assess = graphs["assess"] = Graph("assessment", config)
    assessment_end = assess.chain("Record_assessment", [copy_value("last_assessed_day", "elapsed_days")], "Terminal")
    observations = []
    for lab, metadata in config["labs"].items():
        observations.append({"type": "Observation", "attribute": "liver_" + lab,
                             "category": "laboratory", "unit": metadata["unit"],
                             "codes": [{"system": "LOINC", "code": metadata["code"], "display": metadata["display"]}]})
    observe = assess.chain("Observe", observations, assessment_end)
    # Symptom states share the caller's module context and are cleared by finish.
    for symptom, field, thresholds in reversed([
        ("Confusion", "encephalopathy_grade", [(1, 0), (2, 20), (3, 40), (4, 70), (5, 90)]),
        ("Jaundice", "bilirubin", [(3, 0), (10, 25), (1000, 60)])
    ]):
        choices = []
        for cutoff, intensity in thresholds:
            target = assess.chain(f"{symptom}_{intensity}", [{"type": "Symptom", "symptom": symptom,
                                  "cause": "liver_episode", "exact": {"quantity": intensity}}], observe)
            choices.append((attr(field, "<", cutoff), target))
        assess.branch("Symptom_" + symptom, choices, target)
        observe = "Symptom_" + symptom
    laboratory_states = []
    for lab in config["labs"]:
        baseline = (copy_value("baseline_creatinine", "renal_baseline") if lab == "creatinine" else
                    expr("baseline_" + lab, f"if #b{{liver_preexisting_cirrhosis}} and #s{{liver_transplant_status}} != 'recipient' then {config['cirrhosis_baseline'][lab]} else {config['baseline'][lab]}"))
        activity = "#{liver_injury_activity}" if lab in ("ast", "alt") else "#{liver_severity}"
        if lab == "creatinine":
            activity = "(if #b{liver_renal_dysfunction} then #{liver_severity} else 0.0)"
        # CQL produces BigDecimal, which stock CSVExporter labels as text. Counter parses
        # Number and stores Double. This immediate +1/-1 pair coerces only our namespaced
        # attribute, without touching shared VitalSigns or changing the engine. Round to the
        # exporter's one-decimal precision before conversion so downstream inputs match CSV.
        laboratory_states.extend([baseline,
            expr(lab, f"Round(#{{liver_baseline_{lab}}} + #{{liver_delta_{lab}}} * {activity} * #{{liver_lab_scale}}, 1)"),
            {"type": "Counter", "attribute": "liver_" + lab, "action": "increment", "amount": 1},
            {"type": "Counter", "attribute": "liver_" + lab, "action": "decrement", "amount": 1}])
    calculate = assess.chain("Calculate_labs", laboratory_states, observe)
    flags = assess.chain("Clinical_state", [
        *[expr(organ + "_dysfunction", f"#{{liver_{organ}_draw}} < #{{liver_p_{organ}}}") for organ in ORGANS],
        expr("encephalopathy_grade", "if #{liver_severity} < 0.2 then 0 else if #{liver_severity} < 0.5 then 1 else if #{liver_severity} < 0.75 then 2 else if #{liver_severity} < 0.95 then 3 else 4"),
        expr("airway_support_required", "#{liver_encephalopathy_grade} >= 3 or #b{liver_respiratory_dysfunction}"),
        expr("renal_support_required", "#b{liver_renal_dysfunction} and #{liver_severity} >= 0.8"),
        copy_value("circulatory_support_required", "circulatory_dysfunction")
    ], calculate)
    band_choices = []
    for profile_name, profile in config["profiles"].items():
        choices = []
        for band in ("mild", "moderate", "severe"):
            probabilities = profile["outcomes"][band]
            risk_target = flags
            if profile_name != "postoperative":
                # Fit the evaluation entry odds among native-liver ICU patients only.
                # Preserve relative non-evaluation probabilities and all other consumers.
                adjusted = {}
                for entry, section in (("direct_icu", "care"), ("cirrhosis", "cirrhosis_integration")):
                    multiplier = config[section]["evaluation_odds_multiplier"]
                    total = 1 + probabilities["transplant_evaluation"] * (multiplier - 1)
                    if total <= 0:
                        raise ValueError("Cannot scale an evaluation-only outcome distribution to zero")
                    adjusted[entry] = assess.chain(f"Calibrated_{entry}_{profile_name}_{band}", [
                        probability("p_" + key, value * (multiplier if key == "transplant_evaluation" else 1) / total)
                        for key, value in probabilities.items()], flags)
                select = f"Calibration_entry_{profile_name}_{band}"
                assess.branch(select, [(attr("entry_path", "==", "cirrhosis"), adjusted["cirrhosis"])], adjusted["direct_icu"])
                risk_target = f"Calibration_{profile_name}_{band}"
                assess.branch(risk_target, [(all_of(attr("care_active", "==", True),
                    attr("level_of_care", "==", "icu"), attr("evaluation_done", "==", False),
                    attr("transplant_status", "==", "none")), select)], flags)
            target = assess.chain(f"Risk_{profile_name}_{band}", [set_value("severity_band", band),
                *[probability("p_" + outcome, profile["outcomes"][band][outcome]) for outcome in OUTCOMES],
                *[probability("p_" + organ, p) for organ, p in zip(ORGANS, profile["complications"][band])]
            ], risk_target)
            if band != "severe":
                cutoff = config["severity_bands"]["moderate" if band == "mild" else "severe"]
                choices.append((attr("severity", "<", cutoff), target))
        assess.branch("Risk_" + profile_name, choices, target)
        band_choices.append((attr("profile", "==", profile_name), "Risk_" + profile_name))
    assess.branch("Select_profile_risk", band_choices, assess.error("Unknown risk profile"))
    resolved = assess.chain("Resolved", [set_value("severity_band", "resolved"),
        *[probability("p_" + name, 1.0 if name == "recovery" else 0.0) for name in OUTCOMES],
        *[probability("p_" + organ, 0.0) for organ in ORGANS]], flags)
    assess.branch("Select_risk", [(attr("severity", "==", 0), resolved)], "Select_profile_risk")
    severity = assess.chain("Severity", [
        expr("phase_days", "#{liver_elapsed_days} - #{liver_phase_start_day}"),
        expr("raw_severity", "if #s{liver_trajectory} = 'stable' then #{liver_phase_initial_severity} else if #s{liver_trajectory} = 'deterioration' then #{liver_phase_initial_severity} + #{liver_phase_days} / #{liver_deterioration_days} else #{liver_phase_initial_severity} - #{liver_phase_days} / #{liver_recovery_days}"),
        expr("severity", "if #{liver_raw_severity} < 0 then 0.0 else if #{liver_raw_severity} > 1 then 1.0 else #{liver_raw_severity}"),
        expr("injury_activity", "if #s{liver_trajectory} = 'stable' then #{liver_phase_initial_injury} else (if #{liver_phase_initial_severity} > 0 then #{liver_phase_initial_injury} * #{liver_severity} / #{liver_phase_initial_severity} else #{liver_severity}) / (1.0 + #{liver_phase_days} / #{liver_injury_decay_days})")
    ], "Select_risk")
    profile_choices = []
    for name, profile in config["profiles"].items():
        target = assess.chain("Profile_" + name, [set_value("profile", name),
            *[set_value(key, profile[key]) for key in ("recovery_days", "deterioration_days", "injury_decay_days")],
            *[set_value("delta_" + lab, delta) for lab, delta in profile["deltas"].items()]
        ], severity)
        if name == "postoperative":
            profile_choices.insert(0, (attr("transplant_status", "==", "recipient"), target))
        else:
            profile_choices.append((attr("etiology", "==", name), target))
    assess.branch("Select_profile", profile_choices, assess.error("Unknown laboratory profile"))
    changed = assess.chain("Change_course", [copy_value("phase_initial_severity", "severity"),
        copy_value("phase_initial_injury", "injury_activity"),
        copy_value("phase_start_day", "elapsed_days"), copy_value("current_trajectory", "trajectory")], "Select_profile")
    assess.branch("Check_course", [({"condition_type": "Attribute", "attribute": "liver_course_changed", "operator": "==", "value": True}, changed)], "Select_profile")
    course = assess.chain("Course", [expr("course_changed", "#s{liver_trajectory} != #s{liver_current_trajectory}")], "Check_course")
    assess.branch("Check_duplicate", [(attr("assessment_duplicate", "==", True), "Terminal")], course)
    duplicate = assess.chain("Duplicate", [expr("assessment_duplicate", "#{liver_elapsed_days} = #{liver_last_assessed_day}")], "Check_duplicate")
    clear_error = assess.chain("Clear_error", [{"type": "SetAttribute", "attribute": "liver_error"}], duplicate)
    validate_context(assess, clear_error)
    assess.branch("Validate_inputs", [(valid_inputs(), "Validate_baseline")], assess.error("Unsupported liver inputs"))
    assess.branch("Check_active", [(attr("active", "==", True), "Validate_inputs")], assess.error("Initialize an episode before assessing"))
    assess.start("Check_active")

    advance = graphs["advance"] = Graph("advance one day", config)
    advance_target = advance.chain("Advance", [
        {"type": "Delay", "distribution": {"kind": "EXACT", "parameters": {"value": config["step_hours"]}}, "unit": "hours"},
        {"type": "Counter", "attribute": "liver_elapsed_days", "action": "increment"},
        {"type": "CallSubmodule", "submodule": "liver/assess"}], "Terminal")
    validate_context(advance, advance_target)
    advance.branch("Validate_inputs", [(valid_inputs(), "Validate_baseline")], advance.error("Unsupported liver inputs"))
    advance.branch("Check_active", [(attr("active", "==", True), "Validate_inputs")], advance.error("Initialize before advancing"))
    advance.start("Check_active")

    outcome = graphs["choose_outcome"] = Graph("choose interval outcome", config)
    recorded = outcome.chain("Record_outcome", [copy_value("last_outcome_day", "elapsed_days")], "Terminal")
    choices = []
    for name in OUTCOMES:
        target = outcome.chain("Outcome_" + name, [set_value("suggested_outcome", name)], recorded)
        choices.append({"distribution": {"attribute": "liver_p_" + name, "default": 0.0}, "transition": target})
    outcome.states["Choose"] = {"type": "Simple", "distributed_transition": choices}
    outcome.branch("Check_due", [(attr("outcome_due", "==", True), "Choose")], "Terminal")
    due = outcome.chain("Due", [expr("outcome_due", "#{liver_elapsed_days} > #{liver_last_outcome_day} and #{liver_last_assessed_day} = #{liver_elapsed_days}")], "Check_due")
    outcome.branch("Check_error", [(attr("error", "is nil"), due)], "Terminal")
    outcome.branch("Check_active", [(attr("active", "==", True), "Check_error")], outcome.error("Initialize before choosing an outcome"))
    outcome.start("Check_active")

    finish = graphs["finish"] = Graph("finish episode", config)
    finish.start(finish.chain("Finish", [set_value("active", False),
        *[{"type": "Symptom", "symptom": symptom, "cause": "liver_episode", "exact": {"quantity": 0}} for symptom in ("Confusion", "Jaundice")]
    ], "Terminal"))
    # Optional case-level referral decision. It never assumes that a referral needs ICU.
    referral = graphs["referral"] = Graph("MGUH referral", config)
    record = referral.chain("Record", [copy_value("referral_decision_case_id", "case_id"),
        set_value("referral_share", config["calibration"]["referral_share"])], "Terminal")
    p = config["calibration"]["referral_share"]
    referral.states["Draw"] = {"type": "Simple", "distributed_transition": [
        {"distribution": p, "transition": referral.chain("Referred", [set_value("referred_to_mguh", True)], record)},
        {"distribution": 1 - p, "transition": referral.chain("Elsewhere", [set_value("referred_to_mguh", False)], record)}]}
    referral.branch("Already_decided", [(attr("referral_same_case", "==", True), "Terminal")], "Draw")
    check = referral.chain("Check_case", [expr("referral_same_case",
        "#s{liver_case_id} = #s{liver_referral_decision_case_id}")], "Already_decided")
    referral.branch("Existing_decision", [(attr("referral_decision_case_id", "is not nil"), check)], "Draw")
    referral.branch("Entry", [(attr("case_id", "is nil"), referral.error("Set a unique liver_case_id before referral")),
        (attr("case_id", "==", ""), referral.error("liver_case_id must not be empty"))], "Existing_decision")
    referral.start("Entry")
    from liver_care import build as build_care
    from liver_cirrhosis import build as build_cirrhosis
    return {**{name: graph.document for name, graph in graphs.items()}, **build_care(config), **build_cirrhosis(config)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "synthea/liver_model.json")
    parser.add_argument("--output", type=Path, default=ROOT / "synthea/modules/liver")
    parser.add_argument("--check", action="store_true", help="Fail if generated modules differ; do not write")
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    validate(config)
    modules = build(config)
    for name, module in modules.items():
        target = args.output / (name + ".json")
        content = json.dumps(module, indent=2) + "\n"
        if args.check:
            if not target.exists() or target.read_text() != content:
                raise SystemExit(f"Stale generated module: {target}; run scripts/build_liver_modules.py")
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content)
    print(f"{'Verified' if args.check else 'Generated'} {len(modules)} liver submodules")


if __name__ == "__main__":
    main()
