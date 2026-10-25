# Clinical module regression checks

These checks use the installed Synthea JAR and Java 17+ source-file execution. Run them from the repository root after `./0_setup_synthea.sh`:

```bash
java -cp "synthea:synthea/bin/resources:synthea/bin/synthea-with-dependencies.jar" scripts/tests/CirrhosisModuleTest.java
java -cp "synthea:synthea/bin/resources:synthea/bin/synthea-with-dependencies.jar" scripts/tests/LiverModelTest.java
uv run python -m unittest discover -s scripts/tests -p 'test_liver_*.py'
uv run python scripts/build_liver_modules.py --check
uv run python scripts/tests/liver_export_smoke.py
uv run python scripts/tests/run_liver_care_tests.py
uv run python scripts/tests/liver_care_export_smoke.py
uv run python scripts/tests/run_cirrhosis_integration_tests.py
uv run python scripts/tests/cirrhosis_export_smoke.py
```

`CirrhosisModuleTest` loads `synthea/modules/` through the same local-module loader used by the generation pipeline's `-d` flag. It verifies exhaustive probability distributions, actual engine branch selection over 10,000 deterministic draws, and continued management with a fresh delay and no clinical event. It does not generate a population or modify output datasets. An optional argument selects another module directory for regression fixtures.

`LiverModelTest` calls the five shared liver submodules through real engine `CallSubmodule` states. It checks serial recovery/deterioration, baseline retention, postoperative recovery, course changes, symptoms/support flags, episode guards, and every configured outcome distribution with 10,000 fixed draws. The Python unit tests reject invalid configurations and detect stale generated modules. `liver_export_smoke.py` generates seven seeded episodes in an isolated temporary directory and verifies 315 observations through the actual CSV-to-Parquet/code-dictionary pipeline. See [the liver contract](../../docs/LIVER_MODEL.md) for assumptions and ownership rules.

`run_liver_care_tests.py` compiles test-only probability fixtures and runs `LiverCareTest` against the real engine for 60 ICU/transplant journeys across four etiologies. It checks recovery, death, evaluation and waiting, non-candidacy, timed surgery, postoperative mortality, persistent medication/status, follow-up, rejection, support intervals, care continuing beyond the former time cutoffs, repeated referral decisions, and ownership of pre-existing records. `liver_care_export_smoke.py` runs seven representative courses through the full generator with clinical notes enabled and compares CSV/Parquet clinical records and note linkage. See [the care contract](../../docs/LIVER_CARE.md).

`run_cirrhosis_integration_tests.py` exercises the actual cirrhosis parent with test-only onset/outcome fixtures in 16 journeys. It checks ward and ICU recovery/death, ward deterioration, transplant survival/postoperative death, recurrence, explicit alcohol cause, external medication/condition ownership, existing recipients, competing liver ownership, routine follow-up, and cancellation of delayed native-liver death after graft replacement. Recovery must return to the real management loop with native cirrhosis intact. `cirrhosis_export_smoke.py` sends six representative journeys through the full generator, notes, CSV, and Parquet. It checks sequential encounters, continuous daily lab timing across ED-to-ward/ICU handoff, chronic-medication reconciliation, and separate same-day ED/inpatient notes. See [the integrated pathway](../../docs/CIRRHOSIS_PATHWAY.md).

`test_liver_calibration.py` checks that postoperative deaths count toward transplantation, censored ICU entrants remain in the denominator, ward-only cases are excluded from it, annual weights apply referral once, and validation seeds never enter fitting. It also checks that cirrhosis fitting preserves the direct-ICU reference parameter and that the calibration parent retains the production clinical decisions. `LiverModelTest` verifies selection of each fitted factor, unscaled ward/evaluation probabilities, and reset of calibration context in subsequent episodes. The larger statistical check uses unforced stock-engine episodes; see [calibration commands and limits](../../docs/LIVER_CALIBRATION.md).

## Cirrhosis management assumptions

After each completed 4-10 month management delay, `cirrhosis.json` selects follow-up (66%), a complication encounter (10%), the disease-death state (4%), or another management delay without a clinical event (20%). The continuation branch explicitly allocates the previously missing probability, which Synthea otherwise assigned to the final death branch, making its effective selection probability 24% ([issue #2](https://github.com/natecobb/syntheticMGUH/issues/2)).

The 20% continuation branch is an explicit synthetic modeling assumption that preserves the three existing branch probabilities. These values are not clinically calibrated annual rates. Follow-up may wait for a wellness encounter. The disease-death branch now waits another 1-12 months, then rechecks native cirrhosis, recipient status, and competing liver ownership before recording death. A graft received during the delay cancels this native-liver death; it is no longer an irrevocably scheduled future death. Continuing management retains the underlying cirrhosis and samples another 4-10 month delay.
