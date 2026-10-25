# Liver ICU and transplant care

`liver/critical_care` implements the shared inpatient journey from issues #4 and #5. It consumes
the [liver severity contract](LIVER_MODEL.md) for generic or alcohol-related cirrhosis,
acetaminophen injury, and other toxic injury. Outcomes include medical recovery, death, or liver transplantation followed
by recovery or postoperative death. Surviving recipients receive follow-up and persistent
transplant status and immunosuppression.

Care configuration `1.1-demo`, in shared model `1.4-demo`, uses **synthetic scenario parameters**.
The [calibration runner](LIVER_CALIBRATION.md) fits scenario targets among index liver ICU entrants:
25% for the original reference cohort, and approximately 21% accepted by the user for integrated
cirrhosis. It separately measures a 25% regional referral share. Other clinical parameters remain
uncalibrated; these targets are not observed MedStar rates.
It does not implement donor allocation, clinical transplant-selection rules, or hospital transfers.
The [existing cirrhosis parent](CIRRHOSIS_PATHWAY.md) now calls this course after severe
decompensation or ward deterioration, using its existing population-onset logic. The shared course
itself adds no incidence. Alcohol-specific history and toxic-injury population entry remain in
#6/#7. The integrated pathway has a separate calibration cohort and evaluation multiplier; neither
cohort establishes real-world incidence or outcomes under all background disease modules.

## Integration and ownership

Set `liver_etiology`, `liver_trajectory`, `liver_initial_severity`, and the optional baseline inputs
from the shared contract, then call:

```json
{
  "type": "CallSubmodule",
  "submodule": "liver/critical_care",
  "direct_transition": "After_Liver_Care"
}
```

The course opens a coded inpatient ICU encounter. It initializes an episode if none is active,
or continues an initialized episode under the same parent. A new Encounter state closes an
existing encounter owned by that parent; reconcile pre-ICU treatments before handing over. Check
`liver_error` after return. Use one parent only: Synthea submodule histories and encounter ownership
are patient-scoped. Internal helpers must not be called concurrently or out of sequence.

The call returns after discharge for a nonrecipient, after the final scheduled follow-up for a
surviving recipient, or at death. Outpatient visits release their encounters; the patient does not
occupy an inpatient bed throughout follow-up. Normal Synthea processing terminates on death.

| Runtime output | Meaning |
|---|---|
| `liver_care_active`, `liver_care_version` | Ownership of the inpatient/follow-up course and its configuration version. |
| `liver_care_outcome` | ongoing, recovered, transplanted, or death. Transplanted means discharged after surgery, with follow-up then scheduled. |
| `liver_level_of_care` | icu, ward, outpatient, discharged, or deceased. |
| `liver_syndrome` | acute_liver_injury, acute_liver_failure, decompensated_cirrhosis, acute_on_chronic_liver_failure, or postoperative. |
| `liver_evaluation_context`, `liver_evaluation_done` | Acute without cirrhosis, otherwise chronic; whether evaluation has started in this episode. |
| `liver_tx_result` | not_evaluated initially; then recovery, not_candidate, transplanted, or death after the corresponding transplant branch. |
| `liver_graft_available` | A graft was selected before surgery. |
| `liver_transplant_episode`, `liver_transplant_count` | Episode receiving the most recent graft and lifetime modeled transplant count. |
| `liver_followup_count`, `liver_followup_complete` | Completed scheduled visits and completion flag. |
| `liver_rejection_occurred` | Rejection occurred during this course, even after resolution. |

These are runtime attributes, not new Parquet columns. Exported evidence comprises encounters,
conditions, procedures, devices, care plans, medications, observations, and notes. Internal
`liver_owned_*` references let the workflow end only records it created. A fatal episode retains
its last severity/support snapshot; do not interpret inactive or deceased state as current care.

## ICU, evaluation, and surgery

The ICU encounter uses `inpatient` and SNOMED **305351004** (Admission to intensive care unit).
Daily panels, symptoms, and support use the shared etiology/severity model. Recovery and
deterioration change the trajectory; death terminates care; transplant_evaluation starts one
evaluation per episode. Airway protection for encephalopathy does not itself create a
respiratory-failure diagnosis. Renal replacement and vasopressors require their corresponding flags.

Noncirrhotic patients receive acute hepatic failure coding when modeled INR is ≥1.5 with
encephalopathy. Chronic patients receive decompensated-cirrhosis coding. The synthetic ACLF
phenotype additionally requires INR ≥1.5, bilirubin ≥5, and modeled renal, respiratory, or
circulatory dysfunction or encephalopathy grade ≥3. Exported evidence is the combination of
cirrhosis, decompensation, hepatic failure, and relevant organ diagnoses. This is a simulation
definition, not a validated AASLD/EASL diagnostic implementation; decompensation alone is not ACLF.
The phenotype is reassessed during support, and recipients switch to postoperative.

Evaluation uses **1076381000000104**, pretransplant evaluation of a liver recipient. Candidacy is
drawn once among patients completing evaluation, using acute/chronic context. Neither a severity
score nor an alcohol-use label automatically includes or excludes a patient. Candidates acquire
awaiting-liver-transplant coding (**698305006**). Daily monitoring/support continue during evaluation
and waiting. Recovery and death take precedence over the daily graft-availability draw;
deterioration changes the trajectory while the candidate continues waiting. Non-candidacy
returns to ICU management without a graft. Listing does not expire after an arbitrary number
of days. This models an inpatient waiting episode; outpatient chronic waitlist care is still outside scope.

A successful availability draw ends the listing and starts a timed liver transplant
(**18027006**) in the inpatient encounter. Evaluation requires an active ICU episode and cannot
repeat in that episode. Surgery requires listed status and no previous graft in that episode.
The postoperative phase begins at surgery completion, with an immediate panel and then daily
panels. Preoperative outcome suggestions are cleared so they do not inadvertently determine the
new graft's trajectory. Surgery adds six real hours to the counted 24-hour assessment steps;
there is no intraoperative panel or minute-level ICU physiology.

Medical recovery preserves chronic cirrhosis. Graft replacement ends native-liver cirrhosis at
surgery completion, retaining its historical condition record, and adds persistent transplanted
liver status (**737297006**). Viral disease, CKD, and unrelated diagnoses are not cleared. Temporary
organ conditions created here resolve with the episode; other modules retain ownership of their
conditions and treatments. Coordinate any pre-existing support plans/devices before handoff.

When acute dysfunction resolves, support and ICU care end. A separate ward encounter uses
**305342007**, followed by two further daily assessments and discharge. On death, owned support
and medications stop, the encounter closes as deceased, and no ward care or follow-up follows.

## Exported support intervals

| Event | Records and codes | Precision |
|---|---|---|
| Ventilation | Artificial respiration 40617009; respiratory-therapy plan 53950000; mechanical ventilator device 449071006 | Device start/stop timestamps; care-plan dates |
| Renal replacement | Continuous venovenous hemofiltration 233585000; dialysis-care plan 385971003; hemofiltration device 463140004 | Device start/stop timestamps; care-plan dates |
| Circulatory support | Norepinephrine medication, RxNorm 373112 | Medication start/stop timestamps |
| Liver transplant | Procedure 18027006 | Procedure start/stop timestamps |
| Recipient status | Condition 737297006 | Persistent condition with source date precision |
| Follow-up | Encounter 439740005 | Encounter start/stop timestamps |

Ventilation and hemofiltration procedures are instantaneous initiation markers. Their associated
device intervals provide duration and can overlap other support. Device states represent temporary
equipment use, not literal implantation. Stock care-plan CSVs retain dates only; the existing
converter interprets those dates at America/New_York midnight. Use devices/medications for precise
support timing rather than treating a care-plan midnight as a measured clinical timestamp.

Codes were checked against the repository's Athena vocabulary and added to the rebuilt crosswalk.
Regression checks compare clinical records with Parquet, including precise device, medication,
procedure, and observation times, and verify patient/encounter linkage for records and notes.

## Recipient follow-up

Survivors retain chronic tacrolimus (RxNorm **198377**) and mycophenolate mofetil (**199058**) orders.
Product strengths identify terminology; individualized dosing, induction, and dose adjustment are
not modeled. Visits at 7, 14, 30, 90, and 180 days after discharge include the shared liver panel
and tacrolimus blood concentration, [LOINC 11253-2](https://loinc.org/11253-2/), in ng/mL. Assessment
is temporarily reactivated during visits without resetting the episode or drawing ICU mortality
outcomes. The model clock advances by each full calendar interval.

One optional rejection draw occurs at the first visit. The bounded scenario adds acute liver-graft
rejection (**431222008**), temporary functional/laboratory deterioration, and a temporary prednisone
product record (**312615**). Rejection and temporary medication end by the second visit. Maintenance
immunosuppression and recipient history persist after the modeled follow-up period. This simplified
mild complication does not model biopsy workup, refractory rejection, graft loss, or retransplantation.

## Configurable assumptions

Parameters are in `care` within [`synthea/liver_model.json`](../synthea/liver_model.json).

| Parameter | Default | Denominator/interval |
|---|---|---|
| Evaluation duration | Acute 1 day; chronic 3 days | Completed daily care steps |
| Candidacy | Acute 0.80; chronic 0.65 | One draw after evaluation without recovery/death |
| Graft availability | Acute 0.35; chronic 0.15 | Each completed listed waiting day after competing clinical outcomes |
| Evaluation odds multiplier | See configuration / calibration report | Scales evaluation entry odds only before evaluation in native-liver ICU care |
| Cirrhosis entry evaluation multiplier | See `cirrhosis_integration` / calibration report | Applies to episodes entered through the cirrhosis ED/ward bridge; the reference factor remains separate |
| Surgery | 6 hours | Per transplant |
| Postoperative severity | 0.8 | Initial synthetic graft-recovery phase |
| Ward care | 2 days | After acute resolution and ICU exit |
| Rejection | 0.15 probability; 0.4 severity | One draw at first completed outpatient follow-up |
| Tacrolimus level | Uniform 6–10 ng/mL | Each visit; a demonstration value, not a clinical target |

The former 30/21-day forced-outcome horizons and 14-day graft-wait limit have been removed.
Daily competing outcomes continue until clinical resolution, transplantation, or death. There is
no forced recovery/death lottery to terminate long episodes. A simulation observation limit
produces a censored course; unfinished care and follow-ups must not be interpreted as completed ones.
The calibration runner reports censoring and refuses to apply a fit with unresolved transplant
outcomes. A recorded transplant remains observed when later recipient follow-up is unfinished.

[AASLD acute liver failure](https://www.aasld.org/liver-fellow-network/core-series/back-basics/defining-and-managing-acute-liver-failure),
[AASLD ACLF](https://www.aasld.org/liver-fellow-network/core-series/clinical-pearls/management-acute-chronic-liver-failure), and
[AASLD liver-transplant immunosuppression](https://www.aasld.org/liver-fellow-network/core-series/clinical-pearls/beginners-guide-liver-transplant-immunosuppression)
support the qualitative structure, not these numeric probabilities or the simplified medication
scenario. Stock kidney-transplant and cardiac postoperative modules supplied GMF patterns for
evaluation, persistent history/medication, ICU/ward encounters, and follow-up; their organ-specific
diagnoses and probabilities were not imported.

## Verification

```bash
uv run python scripts/build_liver_modules.py --check
uv run python -m unittest discover -s scripts/tests -p 'test_liver_*.py'
uv run python scripts/tests/run_liver_care_tests.py
uv run python scripts/tests/liver_care_export_smoke.py
```

The engine suite runs 60 courses across four etiologies: recovery, death, recovery during
evaluation/waiting, non-candidacy, deterioration while waiting, no graft, transplant survival,
postoperative death, rejection, absent support indications, recovery/death after day 45, and ordinary
seeded stochastic courses. Additional checks preserve externally owned conditions/treatments and
reject a late transplant call. A 20-day graft wait ends through recovery, and repeated referral
checks retain the same case decision. Test fixtures alter probability inputs in isolated module copies;
production modules expose no forced-outcome switch. Seven representative courses run through the
actual generator with notes enabled and the CSV-to-Parquet pipeline. Tests use temporary directories
and leave production datasets unchanged.
