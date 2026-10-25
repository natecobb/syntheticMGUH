"""Connect the existing mixed-cause cirrhosis parent to shared acute liver care."""
import math

from build_liver_modules import Graph, all_of, attr, expr, set_value
from liver_care import CODES, call, clear, code, counter, delay, encounter, end_owned, ensure

CHRONIC_MEDS = ("chronic_spironolactone", "chronic_furosemide", "chronic_propranolol", "chronic_lactulose")
CODES.update({
    "emergency": code("50849002", "Emergency room admission (procedure)"),
    "cirrhosis_plan": code("183063000", "Low salt diet education"),
    "chronic_spironolactone": code("313096", "spironolactone 25 MG Oral Tablet", "RxNorm"),
    "chronic_furosemide": code("313988", "Furosemide 40 MG Oral Tablet", "RxNorm"),
    "chronic_propranolol": code("856519", "propranolol hydrochloride 40 MG Oral Tablet", "RxNorm"),
    "chronic_lactulose": code("391937", "lactulose 667 MG/ML Oral Solution", "RxNorm"),
})


def validate(config):
    settings = config["cirrhosis_integration"]
    lo, hi = settings["initial_severity_range"]
    if not all(math.isfinite(x) for x in (lo, hi)) or not 0 < lo <= hi <= 1:
        raise ValueError("Cirrhosis entry severity must be in (0,1]")
    if not 1 <= settings["workup_hours"] <= 24:
        raise ValueError("ED workup must take 1-24 hours before episode initialization")
    multiplier = settings["evaluation_odds_multiplier"]
    if not math.isfinite(multiplier) or not 0 <= multiplier <= 64:
        raise ValueError("Cirrhosis evaluation odds multiplier must be in [0,64]")


def native_cirrhosis():
    return all_of({"condition_type": "Active Condition", "codes": [CODES["cirrhosis"]]},
        {"condition_type": "Not", "condition": {"condition_type": "Active Condition", "codes": [CODES["recipient"]]}},
        {"condition_type": "Or", "conditions": [attr("transplant_status", "is nil"), attr("transplant_status", "!=", "recipient")]})


def build(config):
    graphs = {}
    def graph(name):
        result = graphs[name] = Graph(name.replace("_", " "), config)
        result.document["remarks"] += config["cirrhosis_integration"]["assumptions"]
        return result

    # Stop only prescriptions/plans created by the cirrhosis parent, never another owner.
    stop = graph("cirrhosis_stop_chronic")
    target = stop.chain("Held", [set_value("cirrhosis_treatment_held", True), *[
        {"type": "Symptom", "symptom": symptom, "cause": "Cirrhosis of liver (disorder)",
         "exact": {"quantity": 0}} for symptom in ("Jaundice", "Ascites", "Fatigue")]], "Terminal")
    target = end_owned(stop, "cirrhosis_plan", target, "CarePlan")
    for key in CHRONIC_MEDS:
        target = end_owned(stop, key, target, "Medication")
    stop.start(target)

    resume = graph("cirrhosis_resume_chronic")
    target = resume.chain("Resumed", [set_value("cirrhosis_treatment_held", False)], "Terminal")
    for key in CHRONIC_MEDS:
        target = ensure(resume, key, target, "Medication", {"chronic": True})
    target = ensure(resume, "cirrhosis_plan", target, "CarePlan")
    resume.branch("Entry", [(native_cirrhosis(), target)], resume.error("Chronic care requires active native-liver cirrhosis"))
    resume.start("Entry")

    acute = graph("cirrhosis_decompensation")
    returned = acute.chain("Returned", [set_value("cirrhosis_decompensation_active", False)], "Terminal")
    recovery = acute.chain("Recovery", [call("stop_support"), call("resolve_conditions"), call("finish"),
        set_value("care_outcome", "recovered"), set_value("care_active", False), set_value("level_of_care", "discharged"),
        {"type": "EncounterEnd", "discharge_disposition": code("01", "Discharged to home", "NUBC")}], returned)
    fatal = acute.chain("Fatal", [set_value("cirrhosis_decompensation_active", False), call("cirrhosis_stop_chronic"), call("care_death")], returned)
    # ICU continues the same episode and lab clock. No second initialization or severity draw.
    escalate = acute.chain("Escalate", [call("cirrhosis_stop_chronic"), {"type": "EncounterEnd"},
        set_value("care_active", False), set_value("cirrhosis_decompensation_active", False), set_value("cirrhosis_icu_escalated", True),
        call("critical_care")], returned)
    need_icu = {"condition_type": "Or", "conditions": [
        attr("severity", ">=", config["severity_bands"]["severe"]),
        *[attr(flag, "==", True) for flag in ("airway_support_required", "renal_support_required", "circulatory_support_required")]]}
    deteriorate = acute.chain("Deteriorate", [set_value("trajectory", "deterioration")], "Ward_day_0")
    improve = acute.chain("Improve", [set_value("trajectory", "recovery")], "Ward_day_0")
    acute.branch("Outcome", [(attr("suggested_outcome", "==", "death"), fatal),
        (attr("suggested_outcome", "==", "transplant_evaluation"), escalate),
        (attr("suggested_outcome", "==", "deterioration"), deteriorate),
        (attr("suggested_outcome", "==", "recovery"), improve)], "Ward_day_0")
    observe = acute.chain("Ward_support", [call("support"), call("choose_outcome")], "Outcome")
    acute.branch("Reassess", [(attr("severity", "==", 0), recovery), (need_icu, escalate)], observe)
    acute.chain("Ward_day", [call("advance"), counter("cirrhosis_non_icu_days")], "Reassess")
    ward_support = acute.chain("Start_ward_support", [call("support")], "Ward_day_0")
    lactulose = ensure(acute, "chronic_lactulose", ward_support, "Medication", {"chronic": True})
    ward = acute.chain("Ward", [{"type": "EncounterEnd"}, set_value("level_of_care", "ward"),
        encounter("ward"), set_value("trajectory", "recovery")], lactulose)
    acute.branch("Triage", [(need_icu, escalate)], ward)
    failed = acute.chain("Invalid", [{"type": "EncounterEnd"}, set_value("care_active", False)], returned)
    acute.branch("Check_error", [(attr("error", "is not nil"), failed)], "Triage")
    lo, hi = config["cirrhosis_integration"]["initial_severity_range"]
    emergency = {**encounter("emergency"), "encounter_class": "emergency"}
    initialize = acute.chain("Admission", [set_value("cirrhosis_decompensation_active", True),
        set_value("care_active", True), set_value("level_of_care", "emergency"), emergency,
        delay(config["cirrhosis_integration"]["workup_hours"], "hours"),
        expr("etiology", "if #s{liver_cirrhosis_etiology} = 'alcohol' then 'alcohol_cirrhosis' else 'cirrhosis'"),
        set_value("preexisting_cirrhosis", True), set_value("transplant_status", "none"),
        set_value("evaluation_done", False), set_value("tx_result", "not_evaluated"), clear("evaluation_context"),
        set_value("evaluation_days", 0), set_value("waiting_days", 0), set_value("graft_available", False),
        set_value("followup_complete", False), set_value("followup_count", 0), set_value("rejection_occurred", False),
        set_value("trajectory", "stable"),
        {"type": "SetAttribute", "attribute": "liver_initial_severity", "distribution": {
            "kind": "UNIFORM", "parameters": {"low": lo, "high": hi}}},
        set_value("cirrhosis_icu_escalated", False),
        set_value("cirrhosis_non_icu_days", 0), set_value("care_outcome", "ongoing"),
        call("initialize"), set_value("entry_path", "cirrhosis")], "Check_error")
    # A missing cause is explicit, so an old toxin episode cannot relabel chronic cirrhosis.
    default_cause = acute.chain("Default_cause", [set_value("cirrhosis_etiology", "unspecified")], initialize)
    acute.branch("Cause", [(attr("cirrhosis_etiology", "is nil"), default_cause)], initialize)
    acute.branch("Native_liver", [(native_cirrhosis(), "Cause")], acute.error("Decompensation requires active native-liver cirrhosis"))
    acute.branch("Entry", [(attr("active", "==", True), acute.error("Another liver episode owns the shared state")),
        (attr("care_active", "==", True), acute.error("Another liver care course is active"))], "Native_liver")
    acute.start("Entry")
    return {name: value.document for name, value in graphs.items()}
