# Liver referral and ICU calibration

These scenarios implement user-defined assumptions for the DC metropolitan catchment. The
original ICU-transplant target was 25%; on 2026-10-25 the user accepted approximately 21% for the
connected cirrhosis pathway after its completed training run produced 20.74%.

| Assumption | Numerator | Denominator |
|---|---|---|
| 25% referral share | Cases referred to MGUH | Regional liver cases |
| 25% reference ICU transplant fraction | Patients with a completed liver transplant during the index inpatient course | Index native-liver ICU entrants in the original three-cause reference cohort |
| Approximately 21% accepted cirrhosis ICU transplant fraction | Patients with a completed liver transplant during the index inpatient course | Index ICU entrants from the connected cirrhosis ED/ward pathway |

The transplant numerator includes patients who subsequently die. Evaluation and listing are not
transplants. The target is pooled across the configured case mix; each cause may have a different
transplant rate. The initial reference fit used three etiologies. Acute liver failure and
decompensated cirrhosis retain separate labels.
These assumptions do not imply that 25% of all regional cases need ICU or that MGUH performs
25% of every type of liver transplant. These targets are not observed MedStar clinical rates.

## Direct-ICU reference cohort

The `calibration` section of [`liver_model.json`](../synthea/liver_model.json) records the catchment,
targets, case mix, fit/validation seeds, tolerances, observation window, and source of each assumption.
The initial cohort uses equal weights for alcohol-related cirrhosis, acetaminophen injury, and
other toxic injury; severity uniformly between 0.75 and 1.0; age 50; and a stable initial trajectory.
These are **placeholder calibration fixtures**, not estimates of MGUH case mix. Race, ethnicity,
income, and insurance in the engine harness are fixed technical fixtures, not population estimates.
The cohorts contain one index episode per patient and no existing graft recipients.
The runner also supports the generic `cirrhosis` profile. `cohort.etiology_weights` may contain any
nonempty subset of the four supported profiles, with weights summing to one; omitted profiles have
zero weight. Changing those weights requires a new fit and independent validation.

The runner creates regional **ICU-eligible** cases, calls the referral module, then follows only
referred cases through ICU in the stock Synthea engine. It assigns the MGUH provider from the
repository's provider file (NPI 1427145176). It does not invent encounters at a sending hospital.
This exercises referral within the ICU-eligible subgroup; the same fixed referral share can be
applied to other liver cases by their future entry modules.

```bash
# Fit with training seeds, validate with independent seeds, then save and regenerate modules.
uv run python scripts/calibrate_liver.py --fit --apply --output output/liver_calibration_run

# Measure the saved model without changing it.
uv run python scripts/calibrate_liver.py --output output/liver_calibration_check
```

Use a new or empty output directory. The default is three training seeds and three separate
validation seeds, each with 2,000 regional ICU-eligible cases. Independent seeds run in up to
three separate JVMs. This is a statistical workload and takes longer than the deterministic tests.
`--regional-cases-per-seed 100` is useful for a small smoke run, but cannot certify the target.

The reference mode fits only `care.evaluation_odds_multiplier`. Before evaluation in native-liver ICU care,
the compiler multiplies the evaluation probability by this factor and renormalizes the complete
daily probability distribution. Relative probabilities of the other outcomes are preserved.
Shared severity consumers outside ICU, evaluation/listing days, and postoperative probabilities
retain their configured values. Candidacy and graft availability remain separate assumptions.

The runner brackets/bisects this multiplier on fixed training seeds. It never tunes on the
validation seeds. Application requires a training error within 0.5 percentage points, validation
referral and transplant rates within 2.5 percentage points of their targets, at least 1,000
validation ICU entrants, and no unresolved transplant outcomes. A recipient with unfinished
follow-up already has an observed transplant endpoint and remains eligible for this rate check.
The absolute tolerances are scenario
acceptance criteria, not statistical proof that the population probabilities equal 25%.

The reference run on 2026-10-25, before the population cirrhosis integration (model `1.2-demo`),
selected an evaluation odds multiplier of **0.75**:

| Metric | Training seeds | Independent validation seeds |
|---|---|---|
| Regional ICU-eligible cases | 6,000 | 6,000 |
| Referred ICU entrants | 1,549 (25.82%) | 1,572 (26.20%) |
| Completed transplants / ICU entrants | 381 / 1,549 (24.60%) | 404 / 1,572 (25.70%) |
| Transplant fraction, Wilson 95% interval | 22.52–26.80% | 23.60–27.92% |
| Unresolved native-liver outcomes | 0 | 0 |

One validation recipient's follow-up extends beyond the observation window. Their transplant
is observed and counted; their later course remains censored. The initial acceptance check
incorrectly treated this as an unresolved transplant outcome. The check was corrected without
changing the fitted parameter or retuning on validation seeds. The local evidence bundle is
`output/liver_calibration_mguh/report.json`, with individual episodes and exact module snapshots
alongside it. Numeric laboratory, recovery, death, and organ-support parameters remain unfitted.
Model `1.4-demo` retains that multiplier and the original reference profiles. The table is evidence
for the saved reference snapshot. The integrated cirrhosis pathway uses its own cohort and
parameter, described below, so fitting it does not change the direct-ICU reference parameter.

## Integrated cirrhosis cohort

`--pathway cirrhosis` uses `cirrhosis_calibration` in the model configuration. It starts with
regional **index cirrhosis decompensation cases**, applies referral once upstream, and runs referred
patients through a copy of the actual cirrhosis parent: diagnosis and chronic prescriptions, ED
workup, the production uniform severity draw of 0.2–1.0, ward reassessment, and ICU escalation.
Clinical probabilities are unforced. Ward-only patients do not enter the ICU-transplant denominator.
Completed transplants count whether the recipient survives or dies.

The harness conditions on diagnosis followed by a first decompensation; it does not estimate their
incidence or waiting times. It stops after that episode and its follow-up, before recurrence.
Generic mixed-cause cirrhosis is the current cohort profile. Age 50, baseline creatinine 0.9, and
demographic/coverage fixtures are assumptions, and unrelated population disease modules are absent.
This tests the integrated acute pathway, not a full-population epidemiologic calibration.

```bash
uv run python scripts/calibrate_liver.py --pathway cirrhosis --fit --apply \
  --output output/cirrhosis_calibration_run

uv run python scripts/calibrate_liver.py --pathway cirrhosis \
  --output output/cirrhosis_calibration_check
```

The default is 8,000 regional cases per seed, with three training seeds (7101, 7202, 7303) and
three independent validation seeds (90101, 90202, 90303): 24,000 cases in each cohort. The parameter
is `cirrhosis_integration.evaluation_odds_multiplier`. The bridge tags its episode with
`liver_entry_path=cirrhosis`; initialization resets this tag for a subsequent unrelated episode.
The factor applies only before evaluation in native-liver ICU care. It does not alter ward
escalation, candidacy, graft availability, evaluation/waiting outcomes, or postoperative risks.
Consequently the number selected into ICU is independent of the fitted parameter. Each seed is
split across three JVMs, with at most nine workers. A 100-case reproducibility check produced
identical per-case records with one versus three shards.

The user accepted the completed 1.5 multiplier fit (778 transplants / 3,751 ICU entrants, 20.74%)
before the independent validation run. Further fitting toward 25% was stopped. The cirrhosis
scenario records an accepted target of 0.21 and retains the original 0.25 target as provenance;
the direct-ICU reference target and its 0.75 multiplier are unchanged.

The independent validation passed, and the 1.5 cirrhosis multiplier was applied locally:

| Metric | Accepted training cohort | Independent validation cohort |
|---|---|---|
| Regional index-decompensation cases | 24,000 | 24,000 |
| Referred to MGUH | 5,932 (24.72%) | 5,963 (24.85%) |
| Index ICU entrants | 3,751 | 3,791 |
| Completed transplants / ICU entrants | 778 / 3,751 (20.74%) | 809 / 3,791 (21.34%) |
| Transplant fraction, Wilson 95% interval | 19.47–22.07% | 20.07–22.67% |
| Recipients who subsequently died, included above | 186 | 191 |
| Unresolved courses, including recipient follow-up | 0 | 0 |

The previous 0.75 setting produced 611 / 3,751 transplants (16.29%) on the same training cases.
An audit confirmed identical case IDs, presenting severity, referral decisions, ICU entry, and
all ward-only records between the two completed passes. Validation used only the previously
specified independent seeds, with no parameter adjustment afterward.

The durable [result and provenance](calibration/cirrhosis_2026-10-25.json) include counts, intervals,
acceptance checks, parameter selection, and model/engine/parent/data hashes. Full local evidence is
in `output/cirrhosis_calibration_accepted/`; training snapshots are in
`output/cirrhosis_calibration_mguh_parallel/fit_00` and `fit_01`. The incomplete higher-parameter
pass was stopped when the user accepted 21% and is excluded from the result. This calibrates the
accepted synthetic target; mortality, length of stay, incidence, and recurrence are still unfitted.

The same training and validation tolerances apply as in the reference mode, with an additional
requirement that no referred ward-only case remains unresolved. A small smoke cohort checks the
runner but cannot certify the target. Validation seeds are excluded from fitting.

## Evidence and unresolved outcomes

Each run saves the exact model and scenario, generated GMF modules, engine logs, per-case JSONL,
and summaries. `report.json` includes the model SHA-256, fitted multiplier, training history,
independent validation counts, Wilson 95% intervals, rates by cause and seed, and acceptance checks.
It also records the fitted parameter name, stock JAR hash, and cirrhosis-parent hash when applicable.
It reports the ICU fraction among referrals, ward-only outcomes, direct versus ward-to-ICU routes,
recovery, pre/post-transplant death, support use, and ICU/wait durations. Those
additional metrics are diagnostic outputs; no clinical targets for them have yet been fitted.

The observation window defaults to 365 days from case entry (ICU entry in reference mode;
diagnosis one day before ED presentation in cirrhosis mode). A course still running is censored,
not assigned recovery or death. Censored entrants remain in the denominator; a completed transplant
before the cutoff still counts even if follow-up is unfinished. Censored courses and those
censored after transplantation are reported separately. The model has no former 30/21-day
forced-outcome horizon and no 14-day waitlist expiry. Waiting continues with competing clinical
outcomes. Chronic outpatient waitlist care is not implemented by this inpatient model.

All calibration episodes run the liver pathway in isolation. Background disease modules,
population incidence, real presenting case mix, longer-term mortality, and other transplant
indications are not calibrated. Refit after changing case mix, severity, competing outcomes,
candidacy, availability, or related care logic. A fit to these assumptions is not clinical validation.

## Referral integration and annual weights

A parent module sets a unique, nonempty string `liver_case_id`, then calls `liver/referral` and
checks `liver_error`. The Boolean `liver_referred_to_mguh` determines whether this case belongs
in the MGUH referral cohort. Rechecking the same current case retains its decision; a new case ID
gets a new draw. `liver_referral_decision_case_id` and `liver_referral_share` document the decision.
This helper creates no encounter and does not decide whether the patient requires ICU. The parent
still supplies liver inputs and makes the clinical ICU-entry decision before calling
`liver/critical_care`. The helper labels referral; provider assignment in population generation
remains the responsibility of the calling integration. Hospital transfers remain outside scope.

The existing cirrhosis parent already runs within the generated hospital cohort, so its new acute
care bridge does **not** apply another 25% referral draw. It also does not assign patients to a
particular hospital when multiple facilities are enabled. A future regional incidence/referral
integration must establish that denominator and provider routing explicitly.

Calibrated regional incidence and the ICU entry fraction remain unspecified. Connecting the
existing cirrhosis parent does not estimate them; fuller entry work remains in issues #6/#7.
These values remain null in the scenario, so no unsupported annual patient weight is emitted.
If both `cohort.regional_cases_per_year` and `cohort.icu_fraction_among_referred` are supplied:

```text
annual MGUH cases          = regional cases/year × 0.25
annual MGUH ICU cases      = annual MGUH cases × ICU fraction
annual ICU transplants    = annual MGUH ICU cases × scenario ICU transplant target
weight per ICU record     = annual MGUH ICU cases / simulated ICU entrants
```

For example, 1,000 regional cases/year with a 40% ICU fraction imply 250 MGUH cases and 100 ICU
cases: 25 ICU-derived transplants under the original reference target, or approximately 21 under
the accepted cirrhosis target. These weights describe the modeled etiologies and ICU
pathway only; they cannot be used as an all-indication transplant volume or an ALF incidence estimate.
The report records a common annual weight for its ICU records; it does not alter the existing
clinical Parquet schema or silently reweight the general synthetic population.
