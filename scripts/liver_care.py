"""Compile the ICU/transplant consumer of the shared liver state into stock GMF states."""
import math

from build_liver_modules import Graph, all_of, attr, copy_value, expr, set_value, valid_inputs


def code(value, display, system="SNOMED-CT"):
    return {"system": system, "code": value, "display": display}


CODES = {
    "icu": code("305351004", "Admission to intensive care unit"),
    "ward": code("305342007", "Admission to ward"),
    "followup": code("439740005", "Postoperative follow-up visit"),
    "cirrhosis": code("19943007", "Cirrhosis of liver"),
    "decompensation": code("716203000", "Decompensated cirrhosis of liver"),
    "acute_failure": code("197270009", "Acute hepatic failure"),
    "hepatic_failure": code("59927004", "Hepatic failure"),
    "encephalopathy": code("13920009", "Hepatic encephalopathy"),
    "renal": code("14669001", "Acute renal failure syndrome"),
    "respiratory": code("65710008", "Acute respiratory failure"),
    "circulatory": code("27942005", "Shock"),
    "ventilation_plan": code("53950000", "Respiratory therapy"),
    "ventilation": code("40617009", "Artificial respiration"),
    "ventilator": code("449071006", "Mechanical ventilator"),
    "rrt_plan": code("385971003", "Dialysis care"),
    "rrt": code("233585000", "Continuous venovenous hemofiltration"),
    "hemofilter": code("463140004", "Hemofiltration system"),
    "pressor": code("373112", "norepinephrine Injectable Solution", "RxNorm"),
    "evaluation": code("1076381000000104", "Pretransplant evaluation of liver recipient"),
    "waiting": code("698305006", "Awaiting transplantation of liver"),
    "transplant": code("18027006", "Transplantation of liver"),
    "recipient": code("737297006", "Transplanted liver present"),
    "rejection": code("431222008", "Acute rejection of liver transplant"),
    "tacrolimus": code("198377", "tacrolimus 1 MG Oral Capsule", "RxNorm"),
    "mycophenolate": code("199058", "mycophenolate mofetil 250 MG Oral Capsule", "RxNorm"),
    "steroid": code("312615", "prednisone 20 MG Oral Tablet", "RxNorm"),
    "tacrolimus_level": code("11253-2", "Tacrolimus [Mass/volume] in Blood", "LOINC"),
}
ACUTE_CONDITIONS = ("acute_failure", "hepatic_failure", "decompensation", "encephalopathy",
                    "renal", "respiratory", "circulatory", "waiting", "rejection")


def validate(config):
    care = config["care"]
    for key in ("ward_days",):
        if type(care[key]) is not int or not 1 <= care[key] <= 90:
            raise ValueError(f"care.{key} must be an integer in [1,90]")
    for context in ("acute", "chronic"):
        if type(care["evaluation_days"][context]) is not int or not 1 <= care["evaluation_days"][context] <= 7:
            raise ValueError("Evaluation must take 1-7 complete days")
        for key in ("candidate_probability", "graft_probability_per_day"):
            if not math.isfinite(care[key][context]) or not 0 <= care[key][context] <= 1:
                raise ValueError(f"Invalid {key} for {context}")
    multiplier = care["evaluation_odds_multiplier"]
    if not math.isfinite(multiplier) or not 0 <= multiplier <= 64:
        raise ValueError("Evaluation odds multiplier must be in [0,64]")
    if any(key in care for key in ("horizon_outcomes", "icu_decision_days", "postoperative_decision_days", "maximum_wait_days")):
        raise ValueError("Remove obsolete forced-outcome and graft-wait cutoffs")
    for key in ("postoperative_severity", "rejection_severity", "rejection_probability"):
        if not math.isfinite(care[key]) or not 0 <= care[key] <= 1:
            raise ValueError(f"Invalid care.{key}")
    if not 0 < care["surgery_hours"] < 24:
        raise ValueError("Surgery duration must be within one day")
    visits = care["followup_days_after_discharge"]
    if len(visits) < 2 or any(type(day) is not int or not 0 < day <= 365 for day in visits) or visits != sorted(set(visits)):
        raise ValueError("Follow-up days must be strictly increasing within one year")
    if (visits[1] - visits[0]) < care["rejection_severity"] * config["profiles"]["postoperative"]["recovery_days"]:
        raise ValueError("The bounded rejection scenario must resolve by the second visit")
    lo, hi = care["tacrolimus_monitoring_range"]
    if not 0 < lo <= hi or not math.isfinite(hi):
        raise ValueError("Invalid tacrolimus monitoring range")


def call(path):
    return {"type": "CallSubmodule", "submodule": "liver/" + path}


def counter(name, amount=1):
    return {"type": "Counter", "attribute": "liver_" + name, "action": "increment", "amount": amount}


def clear(name):
    return {"type": "SetAttribute", "attribute": "liver_" + name}


def delay(amount, unit="days"):
    return {"type": "Delay", "distribution": {"kind": "EXACT", "parameters": {"value": amount}}, "unit": unit}


def procedure(key, hours=0):
    return {"type": "Procedure", "codes": [CODES[key]],
            "distribution": {"kind": "EXACT", "parameters": {"value": hours}}, "unit": "hours"}


def encounter(key, inpatient=True):
    return {"type": "Encounter", "encounter_class": "inpatient" if inpatient else "ambulatory",
            "codes": [CODES[key]], "telemedicine_possibility": "none"}


def ensure(graph, key, target, kind="Condition", extra=None):
    """Only acquire ownership when this workflow creates the record."""
    owner = "owned_" + key
    start_type = {"Condition": "ConditionOnset", "CarePlan": "CarePlanStart", "Medication": "MedicationOrder"}[kind]
    created = graph.chain("Start_" + key, [{"type": start_type, "codes": [CODES[key]],
        "assign_to_attribute": "liver_" + owner, **(extra or {})}], target)
    graph.branch("Existing_" + key, [({"condition_type": "Active " + kind, "codes": [CODES[key]]}, target)], created)
    return "Existing_" + key


def end_owned(graph, key, target, kind="Condition", prefix="End"):
    owner = "owned_" + key
    ended = graph.chain(prefix + "_" + key, [{"type": kind + "End", "referenced_by_attribute": "liver_" + owner}, clear(owner)], target)
    guard = prefix + "_check_" + key
    graph.branch(guard, [(attr(owner, "is not nil"), ended)], target)
    return guard


def sync_condition(graph, key, required, target):
    on = ensure(graph, key, target)
    off = end_owned(graph, key, target)
    graph.branch("Need_" + key, [(required, on)], off)
    return "Need_" + key


def start_medications(graph, target):
    for key in ("tacrolimus", "mycophenolate"):
        target = ensure(graph, key, target, "Medication", {"chronic": True})
    return target


def reset_phase(severity):
    return [set_value("trajectory", "postoperative"), set_value("current_trajectory", "postoperative"),
            copy_value("phase_start_day", "elapsed_days"), set_value("phase_initial_severity", severity),
            set_value("phase_initial_injury", severity), set_value("last_assessed_day", -1)]


def build(config):
    care = config["care"]
    graphs = {}
    def graph(key):
        result = graphs[key] = Graph(key.replace("_", " "), config)
        result.document["remarks"] += care["assumptions"]
        return result

    # Parallel support has explicit CarePlan/Medication intervals. Instantaneous procedure
    # markers do not consume another day or serialize ventilation, RRT, and circulatory support.
    support = graph("support")
    target = "Terminal"
    for key, flag, kind, event, device in (
        ("ventilation_plan", "airway_support_required", "CarePlan", "ventilation", "ventilator"),
        ("rrt_plan", "renal_support_required", "CarePlan", "rrt", "hemofilter"),
        ("pressor", "circulatory_support_required", "Medication", None, None),
    ):
        off = end_owned(support, key, target, kind)
        if device:
            off = end_owned(support, device, off, "Device")
        event_target = support.chain("Event_" + key, [procedure(event)], target) if event else target
        if device:
            event_target = support.chain("Device_" + key, [{"type": "Device", "code": CODES[device],
                "assign_to_attribute": "liver_owned_" + device}], event_target)
        new = support.chain("Start_" + key, [{"type": "CarePlanStart" if kind == "CarePlan" else "MedicationOrder",
            "codes": [CODES[key]], "assign_to_attribute": "liver_owned_" + key,
            **({"activities": [CODES[event]]} if event else {})}], event_target)
        support.branch("Existing_" + key, [({"condition_type": "Active " + kind, "codes": [CODES[key]]}, target)], new)
        support.branch("Need_" + key, [(attr(flag, "==", True), "Existing_" + key)], off)
        target = "Need_" + key
    for organ in ("renal", "respiratory", "circulatory"):
        target = sync_condition(support, organ, attr(organ + "_dysfunction", "==", True), target)
    target = sync_condition(support, "encephalopathy", attr("encephalopathy_grade", ">", 0), target)
    support.start(support.chain("Classify", [call("phenotype")], target))

    stop = graph("stop_support")
    target = "Terminal"
    for key, kind in (("ventilation_plan", "CarePlan"), ("rrt_plan", "CarePlan"), ("pressor", "Medication"),
                      ("ventilator", "Device"), ("hemofilter", "Device")):
        target = end_owned(stop, key, target, kind)
    stop.start(target)

    resolve = graph("resolve_conditions")
    target = "Terminal"
    for key in ACUTE_CONDITIONS:
        target = end_owned(resolve, key, target)
    resolve.start(target)

    death = graph("care_death")
    end = death.chain("Death", [set_value("care_outcome", "death"), set_value("care_active", False), set_value("level_of_care", "deceased"),
        expr("transplant_status", "if #s{liver_transplant_status} = 'recipient' then 'recipient' else 'none'"),
        call("finish"), {"type": "EncounterEnd", "discharge_disposition": code("20", "Expired", "NUBC")},
        {"type": "Death", "codes": [CODES["hepatic_failure"]]}], "Terminal")
    # Leave the fatal disease active in the history; terminate owned treatments before death.
    for key in ("tacrolimus", "mycophenolate", "steroid"):
        end = end_owned(death, key, end, "Medication")
    end = end_owned(death, "waiting", end)
    death.start(death.chain("Cleanup", [call("stop_support")], end))

    # One daily unit of care, also used during evaluation and the wait for a graft.
    day = graph("care_day")
    day.start(day.chain("Day", [call("advance"), call("support"), call("choose_outcome")], "Terminal"))

    tx = graph("transplant")
    abort = tx.chain("Abandon", [set_value("transplant_status", "none")], "Terminal")
    recovered = tx.chain("Recovery", [set_value("tx_result", "recovery"), set_value("trajectory", "recovery")], abort)
    recovered = end_owned(tx, "waiting", recovered, prefix="Recovered")
    not_candidate = tx.chain("Not_candidate", [set_value("tx_result", "not_candidate")], abort)
    fatal = tx.chain("Fatal", [set_value("tx_result", "death"), call("care_death")], "Terminal")

    finish_surgery = tx.chain("After_surgery", [set_value("transplant_status", "recipient"),
        set_value("tx_result", "transplanted"), copy_value("transplant_episode", "episode"),
        counter("transplant_count"), copy_value("postoperative_start_day", "elapsed_days"),
        set_value("suggested_outcome", "continue"), *reset_phase(care["postoperative_severity"]),
        call("assess"), call("support")], "Terminal")
    finish_surgery = start_medications(tx, finish_surgery)
    finish_surgery = ensure(tx, "recipient", finish_surgery)
    # Native-liver cirrhosis ends at replacement, retaining its historical condition record.
    # This deliberate reconciliation also covers a diagnosis recorded by the etiology module.
    reconcile = tx.chain("Reconcile_native_liver", [
        {"type": "ConditionEnd", "codes": [CODES["cirrhosis"]]}, clear("owned_cirrhosis")], finish_surgery)
    for key in ("acute_failure", "hepatic_failure", "decompensation"):
        reconcile = end_owned(tx, key, reconcile, prefix="Transplanted")
    surgery = tx.chain("Surgery", [set_value("graft_available", True), procedure("transplant", care["surgery_hours"])], reconcile)
    surgery = end_owned(tx, "waiting", surgery, prefix="Graft")
    tx.branch("Surgery_guard", [(all_of(attr("transplant_status", "==", "listed"), attr("graft_available", "==", False)), surgery)],
              tx.error("Surgery requires a listed candidate without a prior graft in this episode"))
    for context in ("acute", "chronic"):
        p = care["graft_probability_per_day"][context]
        tx.states["Graft_" + context] = {"type": "Simple", "distributed_transition": [
            {"distribution": p, "transition": "Surgery_guard"}, {"distribution": 1 - p, "transition": "Wait_day_0"}]}
        # Continue competing clinical outcomes; elapsed time alone never ends listing.
    tx.branch("Graft_context", [(attr("evaluation_context", "==", "acute"), "Graft_acute")], "Graft_chronic")
    wait_deteriorate = tx.chain("Wait_deteriorate", [set_value("trajectory", "deterioration")], "Graft_context")
    tx.branch("Wait_outcome", [(attr("suggested_outcome", "==", "death"), fatal),
        (attr("suggested_outcome", "==", "recovery"), recovered),
        (attr("suggested_outcome", "==", "deterioration"), wait_deteriorate)], "Graft_context")
    tx.chain("Wait_day", [call("care_day"), counter("waiting_days")], "Wait_outcome")
    listed = ensure(tx, "waiting", "Wait_day_0")
    listed = tx.chain("List", [set_value("transplant_status", "listed"), set_value("waiting_days", 0)], listed)
    for context in ("acute", "chronic"):
        p = care["candidate_probability"][context]
        tx.states["Candidate_" + context] = {"type": "Simple", "distributed_transition": [
            {"distribution": p, "transition": listed}, {"distribution": 1 - p, "transition": not_candidate}]}
    tx.branch("Candidacy", [(attr("evaluation_context", "==", "acute"), "Candidate_acute")], "Candidate_chronic")
    tx.branch("Evaluation_complete", [
        (all_of(attr("evaluation_context", "==", context), attr("evaluation_days", ">=", care["evaluation_days"][context])), "Candidacy")
        for context in ("acute", "chronic")], "Evaluate_day_0")
    eval_deteriorate = tx.chain("Eval_deteriorate", [set_value("trajectory", "deterioration")], "Evaluation_complete")
    tx.branch("Evaluation_outcome", [(attr("suggested_outcome", "==", "death"), fatal),
        (attr("suggested_outcome", "==", "recovery"), recovered),
        (attr("suggested_outcome", "==", "deterioration"), eval_deteriorate)], "Evaluation_complete")
    tx.chain("Evaluate_day", [call("care_day"), counter("evaluation_days")], "Evaluation_outcome")
    evaluate = tx.chain("Evaluate", [set_value("evaluation_done", True), set_value("evaluation_days", 0),
        set_value("transplant_status", "evaluating"), set_value("graft_available", False),
        expr("evaluation_context", "if #b{liver_preexisting_cirrhosis} then 'chronic' else 'acute'"),
        procedure("evaluation")], "Evaluate_day_0")
    tx.branch("Entry", [(all_of(attr("active", "==", True), attr("care_active", "==", True), attr("level_of_care", "==", "icu"),
        attr("evaluation_done", "!=", True), attr("transplant_status", "==", "none")), evaluate)],
        tx.error("Transplant evaluation requires an active ICU episode and may run once per episode"))
    tx.start("Entry")

    follow = graph("transplant_followup")
    finish = follow.chain("Complete", [call("finish"), set_value("care_active", False), set_value("followup_complete", True)], "Terminal")
    next_visit = finish
    visits = care["followup_days_after_discharge"]
    for i in reversed(range(len(visits))):
        interval = visits[i] - (visits[i - 1] if i else 0)
        close = follow.chain(f"Visit_{i}_close", [set_value("active", False), {"type": "EncounterEnd"}, set_value("level_of_care", "discharged")], next_visit)
        if i == 1:
            close = end_owned(follow, "steroid", close, "Medication")
            close = end_owned(follow, "rejection", close)
        lo, hi = care["tacrolimus_monitoring_range"]
        monitor = follow.chain(f"Visit_{i}_monitor", [set_value("active", True), call("assess"),
            {"type": "Observation", "category": "laboratory", "codes": [CODES["tacrolimus_level"]],
             "unit": "ng/mL", "distribution": {"kind": "UNIFORM", "parameters": {"low": lo, "high": hi}}},
            counter("followup_count")], close)
        if i == 0:
            rejection = follow.chain("Rejection_phase", [set_value("rejection_occurred", True), *reset_phase(care["rejection_severity"])], monitor)
            rejection = ensure(follow, "steroid", rejection, "Medication")
            rejection = ensure(follow, "rejection", rejection)
            p = care["rejection_probability"]
            follow.states["Rejection_draw"] = {"type": "Simple", "distributed_transition": [
                {"distribution": p, "transition": rejection}, {"distribution": 1 - p, "transition": monitor}]}
            monitor = "Rejection_draw"
        next_visit = follow.chain(f"Visit_{i}", [delay(interval), counter("elapsed_days", interval),
            set_value("level_of_care", "outpatient"), encounter("followup", False)], monitor)
    follow.branch("Entry", [(all_of(attr("transplant_status", "==", "recipient"),
        attr("care_active", "==", True), attr("level_of_care", "==", "discharged"), attr("followup_complete", "==", False)), next_visit)],
        follow.error("Follow-up requires a discharged recipient in an unfinished care course"))
    follow.start("Entry")

    icu = graph("critical_care")
    error_exit = icu.chain("Invalid_episode", [{"type": "EncounterEnd"}, set_value("care_active", False)], "Terminal")
    discharged = icu.chain("Discharge", [call("finish"), set_value("care_outcome", "recovered"), set_value("level_of_care", "discharged"),
        {"type": "EncounterEnd", "discharge_disposition": code("01", "Discharged to home", "NUBC")}], "After_discharge")
    transplanted = icu.chain("Recipient_discharge", [set_value("care_outcome", "transplanted"), call("transplant_followup")], "Terminal")
    completed = icu.chain("Complete", [set_value("care_active", False)], "Terminal")
    icu.branch("After_discharge", [(attr("transplant_status", "==", "recipient"), transplanted)], completed)
    ward = [call("stop_support"), call("resolve_conditions"), {"type": "EncounterEnd"},
            set_value("level_of_care", "ward"), encounter("ward")]
    for _ in range(care["ward_days"]):
        ward += [call("advance")]
    ward_target = icu.chain("Ward", ward, discharged)
    fatal = icu.chain("Fatal", [call("care_death")], "Terminal")
    recovered = icu.chain("Recover", [set_value("trajectory", "recovery")], "ICU_day_0")
    deteriorated = icu.chain("Deteriorate", [set_value("trajectory", "deterioration")], "ICU_day_0")
    transplant = icu.chain("Transplant", [call("transplant")], "Check_resolution")
    icu.branch("Can_evaluate", [(all_of(attr("evaluation_done", "==", False), attr("transplant_status", "==", "none")), transplant)], "ICU_day_0")
    icu.branch("Outcome", [(attr("suggested_outcome", "==", "death"), fatal),
        (attr("suggested_outcome", "==", "recovery"), recovered),
        (attr("suggested_outcome", "==", "deterioration"), deteriorated),
        (attr("suggested_outcome", "==", "transplant_evaluation"), "Can_evaluate")], "ICU_day_0")
    icu.branch("Check_resolution", [(attr("severity", "==", 0), ward_target)], "Outcome")
    icu.chain("Choose_daily", [call("choose_outcome")], "Check_resolution")
    icu.chain("ICU_day", [call("advance"), call("support")], "Choose_daily_0")
    first_care = icu.chain("First_care", [call("support")], "ICU_day_0")
    phenotype = graph("phenotype")
    chronic_failure = ensure(phenotype, "hepatic_failure", "Terminal")
    chronic_failure = phenotype.chain("ACLF_label", [set_value("syndrome", "acute_on_chronic_liver_failure")], chronic_failure)
    decomp = phenotype.chain("Decomp_label", [set_value("syndrome", "decompensated_cirrhosis")], "Terminal")
    # This is an explicit synthetic phenotype, not a validated ACLF score/definition.
    organ_failure = {"condition_type": "Or", "conditions": [attr("encephalopathy_grade", ">=", 3),
        *[attr(organ + "_dysfunction", "==", True) for organ in ("renal", "respiratory", "circulatory")]]}
    phenotype.branch("ACLF_check", [(all_of(attr("inr", ">=", 1.5), attr("bilirubin", ">=", 5), organ_failure), chronic_failure)], decomp)
    chronic = ensure(phenotype, "decompensation", "ACLF_check")
    chronic = ensure(phenotype, "cirrhosis", chronic)
    acute = ensure(phenotype, "acute_failure", "Terminal")
    acute = phenotype.chain("ALF_label", [set_value("syndrome", "acute_liver_failure")], acute)
    injury = phenotype.chain("Injury_label", [set_value("syndrome", "acute_liver_injury")], "Terminal")
    phenotype.branch("ALF_check", [(all_of(attr("inr", ">=", 1.5), attr("encephalopathy_grade", ">", 0)), acute)], injury)
    postoperative = phenotype.chain("Postoperative", [set_value("syndrome", "postoperative")], "Terminal")
    phenotype.branch("Phenotype", [(attr("transplant_status", "==", "recipient"), postoperative),
        (attr("preexisting_cirrhosis", "==", True), chronic)], "ALF_check")
    phenotype.start("Phenotype")
    prepare = icu.chain("Prepare", [set_value("care_active", True), set_value("care_version", care["version"]),
        set_value("care_outcome", "ongoing"), set_value("level_of_care", "icu"),
        set_value("evaluation_done", False), set_value("graft_available", False),
        set_value("tx_result", "not_evaluated"), set_value("evaluation_days", 0), set_value("waiting_days", 0), clear("evaluation_context"),
        set_value("followup_complete", False), set_value("followup_count", 0), set_value("rejection_occurred", False),
        copy_value("care_start_day", "elapsed_days"), copy_value("postoperative_start_day", "elapsed_days")], first_care)
    icu.branch("Check_error", [(attr("error", "is not nil"), error_exit)], prepare)
    initialize = icu.chain("Initialize", [call("initialize")], "Check_error")
    icu.branch("Existing_episode", [(attr("active", "==", True), "Check_error")], initialize)
    admit = icu.chain("Admit", [encounter("icu")], "Existing_episode")
    icu.branch("Validate", [(valid_inputs(), admit)], icu.error("Set valid liver inputs before critical care"))
    icu.branch("Entry", [(attr("care_active", "==", True), icu.error("A liver care course is already active"))], "Validate")
    icu.start("Entry")
    return {key: value.document for key, value in graphs.items()}
