"""Scenario definitions and summaries shared by the liver compiler and calibration CLI."""
import math

ETIOLOGIES = ("alcohol_cirrhosis", "acetaminophen", "non_acetaminophen", "cirrhosis")


def validate(config):
    entry_path = config.get("entry_path", "direct_icu")
    if entry_path not in ("direct_icu", "cirrhosis"):
        raise ValueError("Unsupported calibration entry path")
    for name in ("referral_share", "icu_transplant_target"):
        value = config[name]
        if isinstance(value, bool) or not math.isfinite(value) or not 0 < value < 1:
            raise ValueError(f"calibration.{name} must be in (0,1)")
    cohort = config["cohort"]
    weights = cohort["etiology_weights"]
    if not weights or set(weights) - set(ETIOLOGIES) or any(not math.isfinite(x) or x < 0 for x in weights.values()) or not math.isclose(sum(weights.values()), 1, abs_tol=1e-12):
        raise ValueError("Etiology weights must use supported causes and sum to one")
    if entry_path == "cirrhosis" and set(weights) - {"cirrhosis", "alcohol_cirrhosis"}:
        raise ValueError("Integrated cirrhosis entry requires a chronic etiology")
    low, high = cohort["initial_severity_range"]
    if not 0 < low <= high <= 1:
        raise ValueError("Calibration severity must be within (0,1]")
    if type(cohort["age_years"]) is not int or not 18 <= cohort["age_years"] <= 100:
        raise ValueError("This calibration cohort requires adult ages in [18,100]")
    if cohort["trajectory"] not in ("stable", "recovery", "deterioration"):
        raise ValueError("Calibration entrants must have a native-liver trajectory")
    volume, icu_fraction = cohort["regional_cases_per_year"], cohort["icu_fraction_among_referred"]
    if volume is not None and (not math.isfinite(volume) or volume <= 0):
        raise ValueError("Annual regional cases must be positive or null")
    if icu_fraction is not None and (not math.isfinite(icu_fraction) or not 0 < icu_fraction <= 1):
        raise ValueError("ICU entry fraction must be in (0,1] or null")
    simulation = config["simulation"]
    shards = simulation.get("parallel_shards_per_seed", 1)
    if type(shards) is not int or not 1 <= shards <= 9:
        raise ValueError("Parallel shards per seed must be an integer in [1,9]")
    for key in ("regional_cases_per_seed", "observation_days", "max_fit_iterations"):
        if type(simulation[key]) is not int or simulation[key] <= 0:
            raise ValueError(f"simulation.{key} must be a positive integer")
    if simulation["observation_days"] > 1095:
        raise ValueError("Observation is limited to three years")
    for key in ("fit_tolerance", "validation_tolerance"):
        if not math.isfinite(simulation[key]) or not 0 < simulation[key] < 0.25:
            raise ValueError(f"Invalid simulation.{key}")
    seeds = []
    for key in ("fit_seeds", "validation_seeds"):
        batch = simulation[key]
        if not batch or any(type(seed) is not int or not 0 <= seed < 2**53 for seed in batch):
            raise ValueError("Seeds must be nonempty lists of nonnegative exact integers")
        seeds.extend(batch)
    if len(set(seeds)) != len(seeds):
        raise ValueError("Fit and validation seeds must be unique and disjoint")


def proportion(successes, total):
    """Binomial Wilson interval; never manufacture a zero rate for an empty cohort."""
    if not total:
        return {"numerator": successes, "denominator": total, "rate": None, "wilson_95": None}
    p, z = successes / total, 1.959963984540054
    denominator = 1 + z * z / total
    center = (p + z * z / (2 * total)) / denominator
    half = z * math.sqrt(p * (1 - p) / total + z * z / (4 * total * total)) / denominator
    return {"numerator": successes, "denominator": total, "rate": p,
            "wilson_95": [max(0, center - half), min(1, center + half)]}


def summarize(rows, config):
    """Count actual ICU admissions and completed operations, including postoperative deaths."""
    rows = list(rows)
    icu = [row for row in rows if row["icu_admitted"]]
    referred = [row for row in rows if row["referred"]]
    integrated = config.get("entry_path") == "cirrhosis"
    if not integrated and any(row["icu_admitted"] != row["referred"] for row in rows):
        raise ValueError("Every referred ICU-eligible calibration case must enter ICU")
    if any(row["icu_admitted"] and not row["referred"] for row in rows):
        raise ValueError("An MGUH ICU entrant must belong to the referred cohort")
    if any(row["transplanted"] and not row["icu_admitted"] for row in rows):
        raise ValueError("A calibration transplant requires an index ICU admission")
    results = {
        "regional_cases": len(rows),
        "entry_path": config.get("entry_path", "direct_icu"),
        "referral": proportion(len(referred), len(rows)),
        "icu_transplant": proportion(sum(row["transplanted"] for row in icu), len(icu)),
        "outcomes": {
            "transplanted_alive_at_last_observation": sum(row["transplanted"] and not row["dead"] for row in icu),
            "transplanted_then_died": sum(row["transplanted"] and row["dead"] for row in icu),
            "death_without_transplant": sum(not row["transplanted"] and row["dead"] for row in icu),
            "recovered_without_transplant": sum(not row["transplanted"] and not row["dead"] and row["complete"] for row in icu),
            "unresolved_without_transplant": sum(not row["transplanted"] and not row["complete"] and not row["dead"] for row in icu),
        },
        "censored_courses": sum(not row["complete"] for row in icu),
        "censored_after_transplant": sum(not row["complete"] and row["transplanted"] for row in icu),
        "by_etiology": {},
        "by_seed": {},
        "support": {},
    }
    if not integrated:
        results["regional_icu_eligible_cases"] = len(rows)
    results["icu_among_referred"] = proportion(len(icu), len(referred))
    results["non_icu_outcomes"] = {
        "recovered": sum(not r["icu_admitted"] and not r["dead"] and r["complete"] for r in referred),
        "died": sum(not r["icu_admitted"] and r["dead"] for r in referred),
        "unresolved": sum(not r["icu_admitted"] and not r["dead"] and not r["complete"] for r in referred),
    }
    results["icu_entry_routes"] = {
        route: proportion(sum(r.get("icu_entry_route") == route and r["transplanted"] for r in icu),
                          sum(r.get("icu_entry_route") == route for r in icu))
        for route in ("direct", "from_ward")
    }
    for name in ETIOLOGIES:
        subset = [row for row in icu if row["etiology"] == name]
        results["by_etiology"][name] = proportion(sum(row["transplanted"] for row in subset), len(subset))
    for seed in sorted({row["seed"] for row in rows}):
        subset = [row for row in icu if row["seed"] == seed]
        results["by_seed"][str(seed)] = proportion(sum(row["transplanted"] for row in subset), len(subset))
    for name in ("ventilation", "rrt", "vasopressors"):
        results["support"][name] = proportion(sum(row[name] for row in icu), len(icu))
    durations = [row["icu_days"] for row in icu if row["icu_closed"]]
    waits = [row["days_to_transplant"] for row in icu if row["transplanted"]]
    results["mean_completed_icu_days"] = sum(durations) / len(durations) if durations else None
    results["mean_days_to_transplant"] = sum(waits) / len(waits) if waits else None
    cohort = config["cohort"]
    annual_cases, icu_fraction = cohort["regional_cases_per_year"], cohort["icu_fraction_among_referred"]
    results["annualization"] = None
    if annual_cases is not None and icu_fraction is not None and icu:
        annual_icu = annual_cases * config["referral_share"] * icu_fraction
        results["annualization"] = {
            "expected_mguh_cases": annual_cases * config["referral_share"],
            "expected_mguh_icu_cases": annual_icu,
            "target_mguh_icu_transplants": annual_icu * config["icu_transplant_target"],
            "weight_per_simulated_icu_patient_year": annual_icu / len(icu),
            "scope": "The modeled liver causes only; excludes non-ICU and other-indication transplants.",
        }
    return results
