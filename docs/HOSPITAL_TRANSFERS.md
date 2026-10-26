# Hospital transfer postprocessing

`scripts/process_transfers.py` implements the postprocessing layer in
[issue #9](https://github.com/natecobb/syntheticMGUH/issues/9). It reads normalized
Parquet after conversion/EHR cutoff repair, publishes a separate dataset, and
precedes optional LLM note rewriting. Synthea and the source dataset remain intact.

## Running and extending a population

```bash
uv run --frozen python3 scripts/process_transfers.py \
  --input output/parquet --output output/transfers

# Generate additional patients and then update the transfer projection.
uv run --frozen python3 2_generate_incremental.py \
  --total 15000 --batch_size 100 --seed 12345 \
  --transfers-output output/transfers

uv run --frozen python3 scripts/test_parquet_load.py --path output/transfers
```

Keep the generation seed, batch size, reference date and configuration fixed when
extending an existing population. A previous partial final batch stays complete;
additional patients use new batch indices and seeds. A retry of an already
published batch leaves all Parquet files and counters unchanged. This does not add
new longitudinal history to an existing patient.

Every source patient, including non-transfer patients, gets a content fingerprint.
Identical input returns `unchanged` without writing a snapshot. Added patients are
processed once; existing patient rows, transfer IDs, clinicians, orders and notes
are reused. Fingerprints ignore row order and preserve duplicate multiplicity.
Changing/removing a previous patient or changing the routing policy/engine version
fails before publication. Use a new output path for an explicit rebuild.

Both hospital organizations and receiving clinicians must exist in the source.
For a new population, `1_build_providers.py --hospitals both` builds the directory.
Missing destinations are audited; the postprocessor does not invent clinicians.

## Recognition and routing

`scripts/transfer_policy.json` contains configurable, versioned **synthetic** rules
and delays. These are demonstration parameters, not calibrated clinical criteria.

| Policy | Route | Recognition evidence |
| --- | --- | --- |
| Cardiac | MGUH → MWHC | Precisely timed acute MI onset (`22298006`, `57054005`) or explicit eligibility signal |
| Liver | MWHC → MGUH | Precisely timed acute hepatic failure (`197270009`), liver recipient evaluation (`1076381000000104`), or explicit eligibility signal |

ROMI without a qualifying signal, resolving liver injury, historical MI and
cirrhosis alone remain local. Transfer selection never uses an eventual transplant,
recovery or death. Evaluation can lead to transfer even when the patient later
recovers without transplant, is declined, or dies.

**Stock condition exports usually contain only dates.** UTC/New York midnight
condition timestamps are conservatively audited as `date_only_diagnosis`; they do
not acquire invented recognition hours. Therefore stock datasets can contain MI
diagnoses without any safely timed cardiac transfers.

Future builds with clinical notes enabled preserve exact modeled onset timestamps
through the repository's Synthea note template. Conversion extracts those markers
into `transfer_signals.parquet` and removes them from note prose, without changing
the stock JAR. Onset is the recognition point of this synthetic policy, not a claim
about real clinical recognition. Existing date-only populations stay unchanged;
their lost hours cannot be reconstructed safely. `--no_notes` disables this channel.

An optional source `transfer_signals.parquet` accepts upstream recognition evidence:
`id VARCHAR`, `patient VARCHAR`, `encounter VARCHAR`, `policy VARCHAR` (`cardiac` or
`liver`), `recognized_at TIMESTAMPTZ`. Supply signals before publishing their
patients. Eligibility must represent what was known then, not a later outcome.
A real midnight recognition can be represented explicitly this way.

Defaults allow 15 minutes for acceptance, 15 for preparation and 30 for transport.
Engine 2 searches for the earliest procedure-free transport window, with departure
no later than six hours after recognition (`max_departure_delay_minutes`). It may
wait until ongoing stabilization finishes or carry ED recognition into an existing
ICU stay. Synthea's brief ED/ICU overlaps remain at the sender and must finish before
departure. Treatment times are never shifted. `recognition_encounter` records where
the indication arose; `source_encounter` records the stay actually split.
ED/inpatient encounters separated by at most six hours form an acute episode. The
first feasible recognized transfer splits the sending encounter and routes the
remaining acute encounters to the destination. This version permits one transfer
per episode. Clinics, urgent care and outpatient follow-up remain unchanged. ED
senders remain ED; receivers can enter inpatient care directly.

Insufficient encounter time, no procedure-free window within the delay limit,
destination-only treatment before possible arrival, unresolved acute overlaps
after departure and third-facility episodes are audited. Procedures in any of the
patient's encounters block transport; outpatient routing itself remains unchanged. A same-day
death is ambiguous because Synthea does not export its hour; those cases are
skipped. Configured destination-procedure code lists are explicit and extensible,
not comprehensive terminology classifiers. Transport never changes physiology,
mortality, transplantation or the generated treatment sequence.

## Schema and continuity

| Table | Meaning |
| --- | --- |
| `transfers` | Stable ID, patient/episode, recognition/source/destination encounters and organizations, provider/service, policy/evidence, recognition/acceptance/departure/arrival times and rule hash |
| `transfer_audit` | Stable ID, patient/encounter, policy, status, reason and recognition time for transferred and skipped candidates |
| `transfer_orders` | Stable ID, patient/episode/encounter/transfer, order/event times, type, predecessor and original treatment-record key |
| `transfer_financial_allocations` | Linked source/destination encounters, allocation fractions, original claim total and billing-policy identifier |
| `transfer_source_notes` | Original affected notes retained for provenance; excluded from active notes and LLM rewriting |

Encounters add `episode_id`, `source_encounter_id`, `transfer_id`,
`admission_source`, `discharge_disposition`, `origin_organization` and
`destination_organization`. Patient and sending encounter IDs remain unchanged;
the receiving encounter has a deterministic ID.

Affected clinical rows gain `origin_encounter` and `event_location`. Precisely
timed observations, procedures, imaging, supplies, prescriptions and devices
beginning after arrival move to receiving care. Transport observations retain
their sending linkage and explicitly record `transport`. Procedures that overlap the proposed
transport interval postpone departure within the bounded window or prevent transfer. Date-only records retain their origin on an ambiguous date;
records whose date begins after arrival can safely move to the receiver.

Existing medication/device/care-plan intervals continue unchanged. Linked
transport-continuation and receiving-reconciliation orders do not duplicate doses,
devices or completed work. Pending-result status is not exported; handoff notes
explicitly leave it unknown rather than claim everything is complete.

### Financial contract

Split encounters use **consolidated source billing**. Original claims and financial
transaction identities/amounts remain intact. Their source claim covers the
original clinical course, linked to both encounters by the allocation table.
Financial `TRANSFERIN`/`TRANSFEROUT` rows are not clinical transfers.

Original encounter costs/coverage are allocated by non-transport duration. Sending
cents are rounded and the receiver gets the remainder, preserving totals. No new
transport charges are created. This is synthetic allocation, not reimbursement
logic or itemized facility billing. Claims for whole follow-on encounters retain
their amounts and move to their receiving facility/provider. Organization counts,
encounter-based revenue and provider counts are recomputed from the published
clinical data. Revenue means allocated encounter claim cost, not cash collected or
the generator's lifetime directory counters.

### Notes and LLM checkpoints

Notes add stable `note_id`, `note_type`, `author`, `authored_at` and `transfer_id`.
Affected original notes may contain future outcomes and are archived. Sending
summaries, receiving admissions and course summaries use structured facts available
at the note's time. Signing time is separate but equals event time in this version.
Identical repeated source notes retain distinct deterministic occurrence IDs.

The LLM rewriter checkpoints by note identity and source text, preserves timestamps,
and supports multiple same-day notes per encounter. It does not append final
encounter reasons or whole-encounter observations to transfer notes. Legacy LLM rows
are reusable only when their source text uniquely matches a current note.

Write optional LLM output outside the immutable snapshot:

```bash
uv run --frozen python3 scripts/process_notes.py transform \
  --notes_parquet output/transfers/notes.parquet \
  --output_parquet output/transfers_notes_llm.parquet
```

Supply the normal endpoint/model arguments for your environment. No model calls
are part of deterministic transfer processing.

## Atomic publication and verification

The output is a symlink into `.<output-name>.snapshots/`. A writer lock prevents
concurrent publishers. All Parquet tables and `_transfers.json` are staged before
one atomic pointer replacement. An interruption leaves the prior dataset intact;
rerun to retry. Previous snapshots are retained. Long-running readers should
resolve the symlink once before opening multiple tables.

Run after source generation/conversion finishes, not concurrently with its
table-by-table publication. Source file hashes are checked before and after
processing. An interrupted source conversion has a publication journal: conversion
finishes that journal before reading append inputs, and transfers refuse to read
an unfinished publication. The source is retained for explicit rebuilds.

```bash
uv run --frozen python3 -m unittest discover -s scripts/tests -p 'test_transfers.py'
uv run --frozen python3 scripts/tests/transfer_export_smoke.py
uv run --frozen python3 scripts/test_parquet_load.py --path output/transfers
```

Regression coverage includes both routes/settings, non-transfer controls, liver
recovery/transplant/death, ambiguous/impossible timing, cost conservation, continuous
support, outcome-safe notes, same-day LLM checkpoints, reruns, patient additions,
source/policy mismatch rejection and interrupted publication/retry.

## Population identity and cutoff validation

Conversion reserves all existing and incoming SSNs before allocating deterministic
`999-XX-XXXX` replacements for collisions. Existing published identifiers stay fixed
when patients are appended. Sorting incoming patient IDs and probing the reserved
namespace handles both original duplicates and replacement collisions; retries
keep the same values. Null SSNs remain unknown, not duplicate identifiers. Existing
published duplicates require an explicit audited source repair and a new projection.

Patients who died before EHR launch and have no retained encounters remain in the
population. Validation excludes only that verified cohort from first-encounter
coverage, and fails unexplained missing encounters or duplicate non-null SSNs.

The October 2026 build contains 10,000 living and 4,311 deceased patients. All 2,331
without first encounters died before 2010 (latest death: December 28, 2009). The
20 duplicate SSNs were repaired without changing patient IDs or clinical histories.
Engine 2 yields 246 cardiac transfers and one liver transfer. Of 432 exact cardiac
signals, 160 cannot arrive before destination-specific treatment, 15 lack sufficient
encounter time, nine have ambiguous same-day death timing, and two have unresolved
acute encounter overlaps. These stay audited
skips. Source validation passes 147 checks and projection validation 153, with no
warnings; repeated processing leaves every output Parquet hash unchanged.
Rebuild-specific checks and counts are recorded in the local output manifest;
generated patient data and snapshots are not committed.
