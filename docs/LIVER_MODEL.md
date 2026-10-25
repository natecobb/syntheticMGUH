# Shared liver model (issue #3)

The five core `synthea/modules/liver/` submodules provide correlated liver laboratory measurements,
symptoms, support flags, and outcome probabilities for cirrhosis and acute toxic-injury pathways.
They run on the stock Synthea 4 engine. They are callable submodules; the existing mixed-etiology
`cirrhosis.json` pathway now calls them for [acute decompensation](CIRRHOSIS_PATHWAY.md), preserving
its original population-onset logic. Population entry for toxic injury is still separate work.

Additional [ICU/transplant submodules](LIVER_CARE.md) now consume this contract for inpatient care,
evaluation, surgery, and recipient follow-up (issues #4/#5).

**Version `1.4-demo` is a synthetic scenario model.** Its numeric endpoints, time constants,
severity thresholds, symptom intensities, and probabilities are demonstration assumptions. They
are not estimates of MedStar outcomes, clinical scores, treatment rules, or transplant eligibility
criteria. The [calibration runner](LIVER_CALIBRATION.md) fits only the user-defined aggregate
ICU transplant target for specified reference and integrated cirrhosis cohorts. Population incidence,
background disease effects, recurrence, and other clinical parameters remain uncalibrated. Fuller alcohol-specific history and toxic-injury
entry/treatment paths remain in issues #6/#7. Transfers
remain outside scope. The core routines themselves emit no treatment, surgery, or death events;
the linked care consumer owns those events.

## Calling the model

Load `synthea/modules/` with the normal `-d` option. The calling etiology module must own an open
encounter before initialization and keep it open throughout serial assessment. Use the same parent
module for all five calls; Synthea associates submodule symptoms and encounters with that parent.

1. Set all inputs below, then `CallSubmodule` → `liver/initialize`. This creates episode day zero
   and emits the first nine observations.
2. Check that `liver_error` is nil before using any output. Invalid inputs set an error and return
   without starting an episode. A second initialization during an active episode is rejected.
3. Call `liver/advance` for each subsequent assessment. It waits exactly 24 hours, increments the
   model day, and calls `liver/assess`. Looping it inside a parent works with Synthea's default
   weekly generator timestep: the engine rewinds completed delays to their daily event times.
4. Optionally call `liver/choose_outcome` after each advance, then have the parent consume the
   suggestion. `recovery` or `deterioration` can change `liver_trajectory`; `continue` preserves it;
   `transplant_evaluation` requests evaluation, not surgery; `death` must be implemented by the
   parent. Stop advancing when the parent ends an episode or records death.
5. Call `liver/finish` before releasing the encounter. It marks the episode inactive and clears
   this parent's Confusion/Jaundice symptoms. It retains the last snapshot for inspection; organ
   flags and labs are **not current after `liver_active=false`**. The parent ends any episode-owned
   conditions, treatments, and support procedures and closes the encounter.

For example, this is a valid GMF call state after the parent has assigned inputs and opened an
encounter:

```json
{
  "type": "CallSubmodule",
  "submodule": "liver/initialize",
  "direct_transition": "Check_Liver_Error"
}
```

`liver/assess` alone does not advance time. Repeated calls on the same model day emit no additional
observations and do not redraw complications. `choose_outcome` also caches its decision for the
day and never draws at initialization. Advance must run consecutively: do not add unrelated
delays between calls and interpret model days as wall-clock days. Do not invoke advance twice
concurrently or call these routines from competing parent modules.

## Attribute contract

All fields use the `liver_` prefix. Numeric fields are Java `Number` values; consumers should not
assume all are integers or doubles. Boolean flags are actual booleans, not strings.

| Caller input | Values and defaults |
|---|---|
| `etiology` | Required: `cirrhosis`, `alcohol_cirrhosis`, `acetaminophen`, `non_acetaminophen`. Keep fixed during an episode. Generic `cirrhosis` does not imply an alcohol cause. |
| `trajectory` | Required: `stable`, `recovery`, `deterioration`, `postoperative`. Can change between daily advances. |
| `initial_severity` | Required finite number in `[0,1]`; acute functional impairment, independent of chronic baseline. |
| `preexisting_cirrhosis` | Boolean; defaults true and must be true for either cirrhosis profile, false otherwise. Toxic injury may coexist with cirrhosis. |
| `renal_baseline` | Baseline creatinine in mg/dL, `(0,20]`; default 0.9. Pass the existing renal model's baseline when applicable. |
| `alcohol_use` | `unknown`, `active`, `abstinent`, `not_applicable`; default unknown for either cirrhosis profile, not_applicable otherwise. Metadata for the future etiology module; it currently does not alter probabilities. |
| `transplant_status` | `none`, `evaluating`, `listed`, `recipient`; default none. Only the parent changes this. Postoperative trajectory requires recipient status. |

Optional defaults apply only when an attribute is absent. **Set all inputs explicitly for each
subsequent episode**, especially when its etiology changes; initialization preserves existing
caller inputs. Do not edit baseline inputs during an active episode. A transplant parent should
set `transplant_status=recipient` and `trajectory=postoperative` together before the next advance.

| Model output | Meaning |
|---|---|
| `model_version`, `episode`, `active`, `error` | Version; patient-local monotonic episode counter; ownership flag; error string or absent. Pair the episode number with patient ID for identity. |
| `elapsed_days`, `last_assessed_day` | Completed 24-hour model steps and last emitted panel day. |
| `profile`, `severity`, `severity_band` | Effective profile (etiology or postoperative), current synthetic functional impairment, and resolved/mild/moderate/severe band. |
| `injury_activity` | Separate transaminase-driving activity; falling activity does not determine functional recovery. |
| `encephalopathy_grade` | Synthetic grade 0–4 derived from functional severity. |
| `renal_dysfunction`, `respiratory_dysfunction`, `circulatory_dysfunction` | Current episode-associated extrahepatic dysfunction flags. |
| `airway_support_required`, `renal_support_required`, `circulatory_support_required` | Proposed synthetic support state for the ICU module to consume, not an order or a procedure. |
| `inr`, `bilirubin`, `creatinine`, `sodium`, `albumin`, `glucose`, `lactate`, `ast`, `alt` | Current panel values, in the units below. Rounded to the stock CSV export precision. |
| `baseline_*` | Baseline actually used for each measurement, separate from its acute delta. |
| `p_recovery`, `p_deterioration`, `p_death`, `p_transplant_evaluation`, `p_continue` | Exhaustive categorical probabilities for the current profile/band, per completed day. |
| `p_renal`, `p_respiratory`, `p_circulatory` | Marginal complication probabilities for the current profile/band. |
| `suggested_outcome`, `last_outcome_day` | Cached categorical suggestion and the model day on which it was drawn. |

Other `liver_*` attributes (`phase_*`, random draws, delta coefficients, duplicate checks) are
internal. Successful assessment clears an earlier error; failed assessment must not be consumed.
The shared routines do not emit MELD, MELD-Na, MELD 3.0, Child-Pugh, SOFA, or an acute transplant
selection score. Severity is not an exported clinical score. Future scores must name their
formula/version and use the same emitted measurements and required additional inputs.

## Trajectories and probabilities

[`synthea/liver_model.json`](../synthea/liver_model.json) is the parameter source. Each profile
has laboratory deltas, recovery/deterioration time constants, injury decay, three severity-band
outcome distributions, and three complication probabilities ordered renal/respiratory/circulatory.
The generic cirrhosis profile currently copies the alcohol-cirrhosis demonstration coefficients;
this is a placeholder physiology profile, not an inference about etiology or a calibrated equivalence.

Within a phase, severity is the initial phase severity plus elapsed days / deterioration_days,
minus elapsed days / recovery_days, or unchanged for stable disease; it is clamped to `[0,1]`.
Thus recovery_days is the time from severity one to zero, not a fixed length of stay. A change of
trajectory starts a new phase at the next assessment, preserving both current severity and injury
activity. Subsequent days follow the new trajectory. Transplantation switches to the postoperative
profile and a healthy liver baseline; this represents a graft replacement, not spontaneous cure
of native-liver cirrhosis. Chronic renal baseline is retained.

For non-stable phases, injury activity follows severity relative to its phase starting level,
multiplied by `1 / (1 + phase_days / injury_decay_days)`. At a zero starting severity it uses
current severity directly. Stable disease retains its starting injury activity. This permits
falling AST/ALT alongside worsening INR, bilirubin, lactate, and encephalopathy. Labs share one
episode-level scale drawn uniformly from 0.95–1.05; they do not get independent random draws each
day. Each value is `baseline + profile_delta × activity × scale`, with functional severity as
activity except for AST/ALT. Creatinine gets an acute delta only when renal dysfunction is active.
The model does not infer liver functional recovery from AST/ALT alone.

Severity bands default to mild below 0.35, moderate below 0.75, and severe otherwise. Exactly zero
is resolved: recovery probability is one and all episode-complication probabilities are zero.
Resolved disease under either cirrhosis profile retains its chronic baseline, not a normal liver.

Outcome tables are **mutually exclusive next-step suggestions conditional on current etiology
and severity among active episodes after a completed 24-hour step**. Every table sums to one,
including continue. They are not annual risks, cumulative mortality, treatment effects, or a
patient-level prevalence. Drawing them daily compounds risks; a production parent must consume
each decision and stop on terminal outcomes. Skipping draws is not equivalent to combining daily
hazards, and repeated calls on one day return the existing suggestion without resampling.

Complication numbers are **marginal probabilities of current organ dysfunction conditional on
profile/band**, assessed daily, not daily incidence hazards and not an exhaustive categorical
distribution. Each organ gets one uniform latent draw per episode; comparing that same draw to
the current band's probability makes dysfunction persist or resolve with severity, rather than
randomly toggling at each measurement. Organs can co-occur. These simple marginals are not a
calibrated joint distribution. Circulatory support mirrors circulatory dysfunction; airway support
is flagged for grade ≥3 encephalopathy or respiratory dysfunction; renal support is flagged for
renal dysfunction with severity ≥0.8. All these thresholds are synthetic assumptions.

Encephalopathy grades use severity cutoffs 0.2, 0.5, 0.75, and 0.95. Confusion intensity follows
grades 0–4 as 0/20/40/70/90. Jaundice intensity is 0 below bilirubin 3, 25 below 10, and 60 otherwise.
Neither symptom intensity nor these grade thresholds is a validated clinical assessment.

## Terminology and export

These codes, specimen types, and example UCUM units were checked against the linked LOINC records.

| Measurement | LOINC | Unit |
|---|---|---|
| INR, platelet-poor plasma | [6301-6](https://loinc.org/6301-6/) | `{INR}` |
| Total bilirubin, serum/plasma | [1975-2](https://loinc.org/1975-2/) | mg/dL |
| Creatinine, serum/plasma | [2160-0](https://loinc.org/2160-0/) | mg/dL |
| Sodium, serum/plasma | [2951-2](https://loinc.org/2951-2/) | mmol/L |
| Albumin, serum/plasma | [1751-7](https://loinc.org/1751-7/) | g/dL |
| Glucose, serum/plasma | [2345-7](https://loinc.org/2345-7/) | mg/dL |
| Lactate, serum/plasma | [2524-7](https://loinc.org/2524-7/) | mmol/L |
| AST, serum/plasma | [1920-8](https://loinc.org/1920-8/) | U/L |
| ALT, serum/plasma | [1742-6](https://loinc.org/1742-6/) | U/L |

Toxin concentrations and other etiology-specific measurements belong to #7. All nine current
codes map to Athena Measurement concepts in the rebuilt crosswalk. They export into the existing
observations table, linked by patient/encounter/date, with descriptions in `codes.parquet`.
Namespaced state attributes are runtime data; the existing exporter does not persist the episode
counter, flags, or probability tables as new columns.

Stock Synthea CQL expressions return BigDecimal, but CSV export recognizes Double as numeric and
formats it to one decimal. The compiler rounds labs to one decimal and uses an immediate Counter
increment/decrement pair to convert the local attribute to Double. This preserves the numeric
value within floating-point precision, avoids modifying shared VitalSigns, and requires no JAR
changes. Tests verify numeric export type and equality across CSV and Parquet. The model's panel
precision is therefore one decimal, including INR and creatinine.

## Coordination with other modules

The parent is the sole owner of this acute episode's nine measurements and liver-associated
conditions/support. It should reuse these observations when assembling panels, not emit another
independent liver panel in the same encounter. It should test for existing organ-failure conditions
before creating them and end only conditions it created; chronic kidney disease and cirrhosis
must not disappear when an acute episode resolves. Pass baseline renal dysfunction into
`renal_baseline` rather than overwriting a global renal attribute.

The library writes no global VitalSigns, CKD attributes, conditions, medications, or procedures.
The existing cirrhosis, hepatitis, renal, and diabetes modules can still emit their own labs and
symptoms: global reconciliation is not automatic. The cirrhosis parent now delegates acute care
to this library; routine chronic follow-up retains its original measurements. Further #6/#7 work
must coordinate observations with those other producers.
Do not label the existing mixed alcohol/viral/NASH cirrhosis module as alcohol-only. In Synthea,
symptoms are scoped to the calling module; finishing here clears that parent's Confusion/Jaundice,
so the caller must separate any chronic symptom handling that should persist.

## Sources and applicability

- [AASLD, Defining and Managing Acute Liver Failure](https://www.aasld.org/liver-fellow-network/core-series/back-basics/defining-and-managing-acute-liver-failure): qualitative basis for distinct etiologies, serial functional/injury measurements, encephalopathy, and extrahepatic complications. Applies to acute failure without pre-existing liver disease; it is not the source of this model's numeric probabilities or lab endpoints.
- [AASLD, Management of Acute on Chronic Liver Failure](https://www.aasld.org/liver-fellow-network/core-series/clinical-pearls/management-acute-chronic-liver-failure): qualitative basis for separating chronic disease and acute decompensation and tracking extrahepatic dysfunction. The library does not claim to implement a validated ACLF definition or score.
- All numeric config values and the coupling equations are authored demonstration assumptions for issue #3. Calibration against an identified population, time period, and endpoint is still required before interpreting synthetic frequencies as clinical rates. Non-acetaminophen injury is a broad placeholder, not a single homogeneous disease.

## Build and verification

Edit the config for numbers, or `scripts/build_liver_modules.py` for state logic; regenerate the
committed JSON rather than editing it by hand:

```bash
uv run python scripts/build_liver_modules.py
uv run python scripts/build_liver_modules.py --check
uv run python -m unittest discover -s scripts/tests -p 'test_liver_*.py'
java -cp "synthea:synthea/bin/resources:synthea/bin/synthea-with-dependencies.jar" scripts/tests/LiverModelTest.java
uv run python scripts/tests/liver_export_smoke.py
```

The compiler rejects invalid lab endpoints, time constants, incomplete outcome distributions, and
unsupported probability intervals. The Java tests exercise actual engine states. The export test
runs seven seeded fixture episodes (recovery/deterioration for the original three etiologies, plus postoperative),
checks daily panels and trajectories, and compares all 315 observations across CSV and Parquet,
including units, numeric types, patient/encounter/date links, and nine OMOP mappings. Its fixtures
run in a temporary directory and do not alter production outputs. They test behavior, not incidence.
Rebuild the Athena crosswalk with `uv run python scripts/build_athena_crosswalk.py` after changing codes.
