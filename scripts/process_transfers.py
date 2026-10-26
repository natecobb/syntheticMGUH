#!/usr/bin/env python3
"""Project immutable Synthea patients into a two-hospital acute-care dataset.

This is a synthetic routing policy, not a clinical decision rule. See
docs/HOSPITAL_TRANSFERS.md for recognition, financial and precision contracts.
"""

from argparse import ArgumentParser
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import tempfile
from uuid import NAMESPACE_URL, uuid5
from zoneinfo import ZoneInfo

import duckdb

try:
    from .note_identity import assign_note_ids
    from .transfer_store import (
        columns,
        identifier,
        inventory,
        literal,
        load_source,
        merge_previous,
        output_lock,
        patient_hashes,
        publish,
        tables,
    )
except ImportError:
    from note_identity import assign_note_ids
    from transfer_store import (
        columns,
        identifier,
        inventory,
        literal,
        load_source,
        merge_previous,
        output_lock,
        patient_hashes,
        publish,
        tables,
    )

ENGINE_VERSION = 2
DEFAULT_POLICY = Path(__file__).with_name("transfer_policy.json")
ACUTE = {"emergency", "inpatient"}
TIME_COLUMNS = {
    "observations": "date",
    "procedures": "start",
    "imaging_studies": "date",
    "supplies": "date",
    "immunizations": "date",
    "medications": "start",
    "devices": "start",
    "conditions": "start",
    "careplans": "start",
}
DATE_ONLY = {"conditions", "careplans", "immunizations"}


def stable_id(*values: object) -> str:
    return str(
        uuid5(
            NAMESPACE_URL,
            "syntheticMGUH/transfer/v1/" + json.dumps(values, default=str),
        )
    )


def records(
    con: duckdb.DuckDBPyConnection, sql: str, args: list | None = None
) -> list[dict]:
    result = con.execute(sql, args or [])
    names = [c[0] for c in result.description]
    return [dict(zip(names, row)) for row in result.fetchall()]


def insert(con: duckdb.DuckDBPyConnection, table: str, row: dict) -> None:
    con.execute(
        f"INSERT INTO {identifier(table)} ({','.join(identifier(k) for k in row)}) "
        f"VALUES ({','.join('?' for _ in row)})",
        list(row.values()),
    )


def extend(con: duckdb.DuckDBPyConnection, table: str, fields: dict[str, str]) -> None:
    for name, kind in fields.items():
        con.execute(
            f"ALTER TABLE {identifier(table)} ADD COLUMN IF NOT EXISTS {identifier(name)} {kind}"
        )


def precise(value: datetime | None) -> bool:
    """Midnight diagnosis dates are not evidence of recognition at midnight."""
    if value is None:
        return False
    return all(
        value.astimezone(zone).time().isoformat() != "00:00:00"
        for zone in (timezone.utc, ZoneInfo("America/New_York"))
    )


def read_policy(path: Path) -> tuple[dict, str]:
    policy = json.loads(path.read_text())
    if policy.get("version") != 1 or set(policy.get("policies", {})) != {
        "cardiac",
        "liver",
    }:
        raise ValueError("Expected version 1 cardiac and liver routing policies")
    for key in (
        "acceptance_minutes",
        "preparation_minutes",
        "transport_minutes",
        "episode_gap_hours",
        "max_departure_delay_minutes",
    ):
        value = policy.get(key)
        if type(value) is not int or not 0 <= value <= 1440:
            raise ValueError(f"{key} must be an integer between 0 and 1440")
    if policy["transport_minutes"] == 0:
        raise ValueError("transport_minutes must be positive")
    for rule in policy["policies"].values():
        if rule["source"] == rule["destination"] or any(
            h not in policy["hospitals"] for h in (rule["source"], rule["destination"])
        ):
            raise ValueError("Each policy must connect two configured hospitals")
        for field in (
            "condition_codes",
            "recognition_procedure_codes",
            "destination_procedure_codes",
        ):
            if not isinstance(rule[field], list) or any(
                not isinstance(c, str) for c in rule[field]
            ):
                raise ValueError(f"{field} must be a list of string codes")
    payload = {"engine": ENGINE_VERSION, "policy": policy}
    return policy, hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode()
    ).hexdigest()


def prepare(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""CREATE TABLE IF NOT EXISTS transfers (
        id VARCHAR, patient VARCHAR, episode_id VARCHAR, recognition_encounter VARCHAR, source_encounter VARCHAR,
        destination_encounter VARCHAR, source_organization VARCHAR, destination_organization VARCHAR,
        accepting_provider VARCHAR, service VARCHAR, policy VARCHAR, recognition_source VARCHAR,
        recognized_at TIMESTAMPTZ, accepted_at TIMESTAMPTZ, departed_at TIMESTAMPTZ,
        arrived_at TIMESTAMPTZ, status VARCHAR, rule_version VARCHAR)""")
    con.execute("""CREATE TABLE IF NOT EXISTS transfer_audit (
        id VARCHAR, patient VARCHAR, encounter VARCHAR, policy VARCHAR, status VARCHAR,
        reason VARCHAR, recognized_at TIMESTAMPTZ)""")
    con.execute("""CREATE TABLE IF NOT EXISTS transfer_orders (
        id VARCHAR, patient VARCHAR, episode_id VARCHAR, encounter VARCHAR, transfer_id VARCHAR,
        order_type VARCHAR, ordered_at TIMESTAMPTZ, event_at TIMESTAMPTZ, status VARCHAR,
        prior_order_id VARCHAR, source_table VARCHAR, source_key VARCHAR, code VARCHAR)""")
    con.execute("""CREATE TABLE IF NOT EXISTS transfer_financial_allocations (
        id VARCHAR, patient VARCHAR, transfer_id VARCHAR, source_encounter VARCHAR,
        destination_encounter VARCHAR, source_fraction DOUBLE, destination_fraction DOUBLE,
        original_total_claim_cost DOUBLE, billing_policy VARCHAR)""")
    con.execute("""CREATE TABLE IF NOT EXISTS notes (
        patient VARCHAR, date TIMESTAMPTZ, note_text VARCHAR, encounter VARCHAR)""")
    extend(
        con,
        "notes",
        {
            "note_id": "VARCHAR",
            "note_type": "VARCHAR",
            "author": "VARCHAR",
            "authored_at": "TIMESTAMPTZ",
            "transfer_id": "VARCHAR",
        },
    )
    assign_note_ids(con, "notes")
    con.execute("""UPDATE notes SET note_type = COALESCE(note_type, 'source'),
        authored_at = COALESCE(authored_at, date)""")
    con.execute(
        "CREATE TABLE IF NOT EXISTS transfer_source_notes AS SELECT * FROM notes WHERE false"
    )
    extend(
        con,
        "encounters",
        {
            "episode_id": "VARCHAR",
            "source_encounter_id": "VARCHAR",
            "transfer_id": "VARCHAR",
            "admission_source": "VARCHAR",
            "discharge_disposition": "VARCHAR",
            "origin_organization": "VARCHAR",
            "destination_organization": "VARCHAR",
        },
    )
    for table in TIME_COLUMNS:
        if table in tables(con):
            extend(
                con, table, {"origin_encounter": "VARCHAR", "event_location": "VARCHAR"}
            )


def audit(
    con: duckdb.DuckDBPyConnection,
    enc: dict,
    policy: str,
    status: str,
    reason: str,
    recognized: datetime | None = None,
) -> None:
    identity = stable_id(enc["id"], policy, reason, recognized)
    if con.execute("SELECT 1 FROM transfer_audit WHERE id = ?", [identity]).fetchone():
        return
    insert(
        con,
        "transfer_audit",
        dict(
            id=identity,
            patient=enc["patient"],
            encounter=enc["id"],
            policy=policy,
            status=status,
            reason=reason,
            recognized_at=recognized,
        ),
    )


def recognition(
    con: duckdb.DuckDBPyConnection, encounter: dict, policy: str, rule: dict
) -> list[tuple[datetime, str]]:
    signals = []
    if "transfer_signals" in tables(con):
        rows = records(
            con,
            "SELECT * FROM transfer_signals WHERE encounter = ? AND policy = ?",
            [encounter["id"], policy],
        )
        for row in rows:
            if row["patient"] != encounter["patient"] or row["recognized_at"] is None:
                raise ValueError("Invalid explicit transfer signal linkage/timestamp")
            signals.append((row["recognized_at"], "explicit:" + row["id"]))
    for table, codes in (
        ("conditions", rule["condition_codes"]),
        ("procedures", rule["recognition_procedure_codes"]),
    ):
        if not codes or table not in tables(con):
            continue
        rows = records(
            con,
            f"SELECT * FROM {table} WHERE encounter = ? AND CAST(code AS VARCHAR) IN (SELECT unnest(?))",
            [encounter["id"], codes],
        )
        for row in rows:
            if table == "conditions" and not precise(row["start"]):
                audit(
                    con,
                    encounter,
                    policy,
                    "skipped_signal",
                    "date_only_diagnosis",
                    row["start"],
                )
            elif row["start"] is not None:
                signals.append((row["start"], table + ":" + str(row["code"])))
    return sorted(set(signals))


def facts(
    con: duckdb.DuckDBPyConnection, encounter_ids: list[str], at: datetime
) -> str:
    """Only facts already recorded; no discharge note, ultimate diagnosis or outcome."""
    lines = []
    for table in ("observations", "procedures", "medications", "devices", "conditions"):
        if table not in tables(con):
            continue
        time = TIME_COLUMNS[table]
        rows = records(
            con,
            f"SELECT * FROM {table} WHERE COALESCE(origin_encounter, encounter) IN (SELECT unnest(?)) "
            f"AND {time} <= ? ORDER BY {time}, code",
            [encounter_ids, at],
        )
        for row in rows:
            if table == "conditions" and not precise(row[time]):
                # A date-only diagnosis becomes known only after its whole date has passed.
                if row[time] + timedelta(days=1) > at:
                    continue
            if table == "procedures" and row.get("stop") and row["stop"] > at:
                state = "in progress"
            elif table in {"medications", "devices"}:
                if row.get("stop") and row["stop"] <= at:
                    continue
                state = "ongoing; reconcile at receiving facility"
            else:
                state = "recorded"
            value = (
                f"; value={row.get('value')} {row.get('units') or ''}"
                if table == "observations"
                else ""
            )
            lines.append(
                f"{table}: code {row['code']}, {state}, {row[time].isoformat()}{value}"
            )
    return "\n".join(lines) or "No timestamped clinical facts exported by this point."


def archive_notes(con: duckdb.DuckDBPyConnection, encounter: str) -> None:
    con.execute(
        "INSERT INTO transfer_source_notes BY NAME SELECT * FROM notes WHERE encounter = ?",
        [encounter],
    )
    con.execute("DELETE FROM notes WHERE encounter = ?", [encounter])


def note(
    con: duckdb.DuckDBPyConnection,
    transfer: dict,
    encounter: str,
    kind: str,
    at: datetime,
    author: str,
    text: str,
) -> None:
    insert(
        con,
        "notes",
        dict(
            patient=transfer["patient"],
            encounter=encounter,
            date=at,
            authored_at=at,
            note_id=stable_id(transfer["id"], encounter, kind),
            note_type=kind,
            author=author,
            transfer_id=transfer["id"],
            note_text=text,
        ),
    )


def order(
    con: duckdb.DuckDBPyConnection,
    transfer: dict,
    encounter: str,
    kind: str,
    at: datetime,
    event: datetime,
    key: str = "",
    **extra,
) -> str:
    identity = stable_id(transfer["id"], encounter, kind, key)
    insert(
        con,
        "transfer_orders",
        dict(
            id=identity,
            patient=transfer["patient"],
            episode_id=transfer["episode_id"],
            encounter=encounter,
            transfer_id=transfer["id"],
            order_type=kind,
            ordered_at=at,
            event_at=event,
            status="recorded",
            **extra,
        ),
    )
    return identity


def reroute_children(
    con: duckdb.DuckDBPyConnection,
    source: dict,
    target: str,
    transfer: dict,
    entire: bool = False,
) -> None:
    for table, time in TIME_COLUMNS.items():
        if table not in tables(con):
            continue
        con.execute(
            f"UPDATE {table} SET origin_encounter = COALESCE(origin_encounter, encounter) WHERE encounter = ?",
            [source["id"]],
        )
        if entire:
            con.execute(
                f"UPDATE {table} SET encounter = ?, event_location = 'receiving_hospital' WHERE encounter = ?",
                [target, source["id"]],
            )
            continue
        if table not in DATE_ONLY:
            con.execute(
                f"UPDATE {table} SET event_location = 'transport' WHERE encounter = ? AND {time} >= ? AND {time} < ?",
                [source["id"], transfer["departed_at"], transfer["arrived_at"]],
            )
            con.execute(
                f"UPDATE {table} SET encounter = ?, event_location = 'receiving_hospital' WHERE encounter = ? AND {time} >= ?",
                [target, source["id"], transfer["arrived_at"]],
            )
        else:
            # The start of a later date is a safe lower bound; a same-day record retains
            # its origin because we cannot place it on either side of the transfer.
            con.execute(
                f"UPDATE {table} SET event_location = 'date_only_origin' WHERE encounter = ?",
                [source["id"]],
            )
            con.execute(
                f"UPDATE {table} SET encounter = ?, event_location = 'receiving_hospital_date_only' "
                f"WHERE encounter = ? AND {time} >= ?",
                [target, source["id"], transfer["arrived_at"]],
            )
        if table in {"medications", "devices", "careplans"}:
            # Keep the original interval and identity. Continuation orders do not duplicate doses or devices.
            rows = records(
                con,
                f"SELECT * FROM {table} WHERE encounter = ? AND start <= ? AND (stop IS NULL OR stop > ?)",
                [source["id"], transfer["departed_at"], transfer["arrived_at"]],
            )
            for index, row in enumerate(
                sorted(rows, key=lambda r: json.dumps(r, default=str, sort_keys=True))
            ):
                key = stable_id(table, row, index)
                prior = order(
                    con,
                    transfer,
                    source["id"],
                    "continue_during_transport",
                    transfer["accepted_at"],
                    transfer["departed_at"],
                    key,
                    source_table=table,
                    source_key=key,
                    code=str(row["code"]),
                )
                order(
                    con,
                    transfer,
                    target,
                    "reconcile_continuing_treatment",
                    transfer["arrived_at"],
                    transfer["arrived_at"],
                    key,
                    prior_order_id=prior,
                    source_table=table,
                    source_key=key,
                    code=str(row["code"]),
                )


def transport_window(con, episode, index, recognized, config):
    """Find the earliest procedure-free window without changing clinical times.

    Synthea can overlap a short ED encounter with its ICU admission. That overlap
    may finish before departure: retain it at the sender and split the later stay.
    Never carry overlapping stays across the handoff or transport during a procedure.
    """
    minimum = recognized + timedelta(
        minutes=config["acceptance_minutes"] + config["preparation_minutes"]
    )
    latest = recognized + timedelta(minutes=config["max_departure_delay_minutes"])
    duration = timedelta(minutes=config["transport_minutes"])
    reason = "insufficient_or_invalid_encounter_time"
    procedures = (
        records(
            con,
            """SELECT start, COALESCE(stop, start) AS stop
        FROM procedures WHERE patient = ? AND start < ? AND COALESCE(stop, start) >= ?
        ORDER BY start, stop""",
            [episode[index]["patient"], latest + duration, minimum],
        )
        if "procedures" in tables(con)
        else []
    )
    for selected in range(index, len(episode)):
        source = episode[selected]
        if source["organization"] != episode[index]["organization"]:
            break
        if source["stop"] is None:
            continue
        # Every prior acute encounter must be over before the ambulance departs.
        prior = episode[:selected]
        if any(e["stop"] is None for e in prior):
            reason = "overlapping_acute_encounters"
            continue
        departure = max([minimum, source["start"]] + [e["stop"] for e in prior])
        remaining = episode[selected:]
        if any(
            a["stop"] is None or b["start"] < a["stop"]
            for a, b in zip(remaining, remaining[1:])
        ):
            reason = "overlapping_acute_encounters"
            continue
        # Moving the proposed departure to the end of each conflicting procedure
        # is monotone and deterministic, including concurrent/zero-duration work.
        while departure <= latest and departure + duration < source["stop"]:
            crossing = [
                p
                for p in procedures
                if p["start"] < departure + duration
                and (p["stop"] > departure or p["start"] >= departure)
            ]
            if not crossing:
                return selected, departure, departure + duration, None
            reason = "procedure_overlaps_transport"
            departure = max(p["stop"] for p in crossing)
            # A point event at departure belongs to sending care; leave one
            # microsecond before starting transport so the interval is half-open.
            if any(p["start"] == p["stop"] == departure for p in crossing):
                departure += timedelta(microseconds=1)
    return index, minimum, minimum + duration, reason


def transfer_episode(
    con: duckdb.DuckDBPyConnection,
    episode: list[dict],
    index: int,
    policy: str,
    rule: dict,
    recognized: datetime,
    evidence: str,
    config: dict,
    config_hash: str,
    organizations: dict[str, str],
) -> bool:
    trigger = episode[index]
    if (
        recognized < trigger["start"]
        or trigger["stop"] is None
        or recognized >= trigger["stop"]
    ):
        audit(
            con,
            trigger,
            policy,
            "skipped",
            "insufficient_or_invalid_encounter_time",
            recognized,
        )
        return False
    index, departure, arrival, reason = transport_window(
        con, episode, index, recognized, config
    )
    source = episode[index]
    accepted = recognized + timedelta(minutes=config["acceptance_minutes"])
    tail = episode[index + 1 :]
    death = con.execute(
        "SELECT deathdate FROM patients WHERE id = ?", [source["patient"]]
    ).fetchone()[0]
    if death and arrival.date() >= death.date():
        reason = "death_before_arrival_or_ambiguous_death_date"
    if "procedures" in tables(con):
        destination_treatment = con.execute(
            """SELECT count(*) FROM procedures WHERE encounter IN (SELECT unnest(?))
            AND CAST(code AS VARCHAR) IN (SELECT unnest(?)) AND start < ?""",
            [[e["id"] for e in episode], rule["destination_procedure_codes"], arrival],
        ).fetchone()[0]
        if destination_treatment:
            reason = "destination_treatment_precedes_possible_arrival"
    destination = organizations.get(rule["destination"])
    if any(
        e["organization"] not in {source["organization"], destination} for e in tail
    ):
        reason = "episode_crosses_unconfigured_facility"
    providers = con.execute(
        "SELECT id FROM providers WHERE organization = ? ORDER BY id", [destination]
    ).fetchall()
    if destination is None or not providers:
        reason = "missing_destination_directory"
    if reason:
        audit(con, trigger, policy, "skipped", reason, recognized)
        return False
    # Existing patients are never reselected when the provider roster grows.
    provider = providers[
        int(hashlib.sha256(source["id"].encode()).hexdigest(), 16) % len(providers)
    ][0]
    episode_id = stable_id(source["patient"], episode[0]["id"], "episode")
    tid = stable_id(source["patient"], episode_id, policy)
    receiving = stable_id(tid, "receiving")
    transfer = dict(
        id=tid,
        patient=source["patient"],
        episode_id=episode_id,
        recognition_encounter=trigger["id"],
        source_encounter=source["id"],
        destination_encounter=receiving,
        source_organization=source["organization"],
        destination_organization=destination,
        accepting_provider=provider,
        service=rule["service"],
        policy=policy,
        recognition_source=evidence,
        recognized_at=recognized,
        accepted_at=accepted,
        departed_at=departure,
        arrived_at=arrival,
        status="completed",
        rule_version=config_hash,
    )
    insert(con, "transfers", transfer)
    con.execute(
        "UPDATE encounters SET episode_id = ? WHERE id IN (SELECT unnest(?))",
        [episode_id, [e["id"] for e in episode]],
    )
    # A documented synthetic time allocation conserves source totals. Original claims remain
    # consolidated at their source encounter; no new charge or payment is manufactured.
    fraction = (departure - source["start"]).total_seconds() / (
        (departure - source["start"]).total_seconds()
        + (source["stop"] - arrival).total_seconds()
    )
    dest = dict(
        source,
        id=receiving,
        start=arrival,
        organization=destination,
        provider=provider,
        encounterclass="inpatient",
        episode_id=episode_id,
        source_encounter_id=source["id"],
        transfer_id=tid,
        admission_source="transfer_from_acute_hospital",
        origin_organization=source["organization"],
        destination_organization=None,
    )
    if source["encounterclass"] == "emergency":
        dest["code"] = "305342007"
        if "description" in dest:
            dest["description"] = "Admission to ward"
        if (
            "codes" in tables(con)
            and not con.execute(
                "SELECT 1 FROM codes WHERE vocab='SNOMED' AND CAST(code AS VARCHAR)='305342007'"
            ).fetchone()
        ):
            insert(
                con,
                "codes",
                dict(vocab="SNOMED", code="305342007", description="Admission to ward"),
            )
    # ED remains an ED; receiving care is a direct inpatient admission.
    for field in ("base_encounter_cost", "total_claim_cost", "payer_coverage"):
        if field in source and source[field] is not None:
            sent = round(float(source[field]) * fraction, 2)
            dest[field] = float(source[field]) - sent
            con.execute(
                f"UPDATE encounters SET {field} = ? WHERE id = ?", [sent, source["id"]]
            )
    insert(con, "encounters", dest)
    con.execute(
        """UPDATE encounters SET stop = ?, episode_id = ?, source_encounter_id = id,
        transfer_id = ?, discharge_disposition = 'transfer_to_acute_hospital', destination_organization = ? WHERE id = ?""",
        [departure, episode_id, tid, destination, source["id"]],
    )
    insert(
        con,
        "transfer_financial_allocations",
        dict(
            id=stable_id(tid, "allocation"),
            patient=source["patient"],
            transfer_id=tid,
            source_encounter=source["id"],
            destination_encounter=receiving,
            source_fraction=fraction,
            destination_fraction=1 - fraction,
            original_total_claim_cost=source.get("total_claim_cost"),
            billing_policy="consolidated_source_claim_time_allocated_encounters",
        ),
    )
    for prior in episode[:index]:
        reroute_children(con, prior, receiving, transfer)
        archive_notes(con, prior["id"])
        note(
            con,
            transfer,
            prior["id"],
            "course_summary",
            prior["stop"],
            prior["provider"],
            "Sending-hospital care before transfer:\n"
            + facts(con, [prior["id"]], prior["stop"]),
        )
    history = [e["id"] for e in episode[: index + 1]]
    reroute_children(con, source, receiving, transfer)
    archive_notes(con, source["id"])
    order(con, transfer, source["id"], "transfer_discharge", accepted, departure)
    order(con, transfer, receiving, "admission", accepted, arrival)
    header = f"Synthetic {policy} transfer for {rule['service']}.\n"
    note(
        con,
        transfer,
        source["id"],
        "transfer_summary",
        departure,
        source["provider"],
        header
        + f"Accepted at {accepted.isoformat()}; departing for {rule['destination']} at {departure.isoformat()}.\n"
        + facts(con, history, departure)
        + "\nPending-result status is not exported; do not infer that all work is complete.",
    )
    note(
        con,
        transfer,
        receiving,
        "admission",
        arrival,
        provider,
        header
        + f"Arrived from {rule['source']} at {arrival.isoformat()}.\n"
        + facts(con, history, arrival)
        + "\nReconcile ongoing treatment; retain sending results and do not repeat completed work automatically.",
    )
    note(
        con,
        transfer,
        receiving,
        "course_summary",
        source["stop"],
        provider,
        header
        + "Recorded course through this encounter endpoint:\n"
        + facts(con, history, source["stop"]),
    )
    for encounter in tail:
        con.execute(
            """UPDATE encounters SET organization = ?, provider = ?, episode_id = ?, source_encounter_id = id,
            transfer_id = ? WHERE id = ?""",
            [destination, provider, episode_id, tid, encounter["id"]],
        )
        reroute_children(con, encounter, encounter["id"], transfer, entire=True)
        # Claims for an unsplit follow-on encounter can retain all amounts and identities.
        for table in ("claims", "claims_transactions"):
            if table in tables(con):
                cols = columns(con, table)
                for field in ("providerid", "supervisingproviderid"):
                    if field in cols:
                        con.execute(
                            f"UPDATE {table} SET {field} = ? WHERE appointmentid = ?",
                            [provider, encounter["id"]],
                        )
                if "placeofservice" in cols:
                    con.execute(
                        f"UPDATE {table} SET placeofservice = ? WHERE appointmentid = ?",
                        [destination, encounter["id"]],
                    )
        archive_notes(con, encounter["id"])
        end = encounter["stop"] or encounter["start"]
        note(
            con,
            transfer,
            encounter["id"],
            "course_summary",
            end,
            provider,
            f"Continued acute care at {rule['destination']}.\n"
            + facts(con, [encounter["id"]], end),
        )
    audit(con, trigger, policy, "transferred", evidence, recognized)
    return True


def process_new(
    con: duckdb.DuckDBPyConnection, patients: set[str], config: dict, config_hash: str
) -> None:
    organizations = {}
    for hospital, npi in config["hospitals"].items():
        found = con.execute(
            "SELECT id FROM organizations WHERE CAST(npi AS VARCHAR) = ?", [npi]
        ).fetchall()
        if len(found) > 1:
            raise ValueError(f"Ambiguous organization NPI for {hospital}")
        if found:
            organizations[hospital] = found[0][0]
    candidates = set()
    for table, field in (
        ("conditions", "condition_codes"),
        ("procedures", "recognition_procedure_codes"),
    ):
        if table in tables(con):
            codes = sorted(
                {c for rule in config["policies"].values() for c in rule[field]}
            )
            candidates.update(
                r[0]
                for r in con.execute(
                    f"SELECT DISTINCT patient FROM {table} WHERE CAST(code AS VARCHAR) IN (SELECT unnest(?))",
                    [codes],
                ).fetchall()
            )
    if "transfer_signals" in tables(con):
        candidates.update(
            r[0]
            for r in con.execute(
                "SELECT DISTINCT patient FROM transfer_signals"
            ).fetchall()
        )
    for patient in sorted(patients & candidates):
        encounters = records(
            con,
            "SELECT * FROM encounters WHERE patient = ? AND encounterclass IN ('emergency','inpatient') ORDER BY start, id",
            [patient],
        )
        episodes = []
        for encounter in encounters:
            if encounter["encounterclass"] not in ACUTE:
                continue
            previous = episodes[-1][-1] if episodes else None
            if (
                previous
                and previous["stop"]
                and encounter["start"]
                <= previous["stop"] + timedelta(hours=config["episode_gap_hours"])
            ):
                episodes[-1].append(encounter)
            else:
                episodes.append([encounter])
        for episode in episodes:
            candidates = []
            for index, encounter in enumerate(episode):
                for policy, rule in config["policies"].items():
                    if encounter["organization"] != organizations.get(rule["source"]):
                        continue
                    for recognized, evidence in recognition(
                        con, encounter, policy, rule
                    ):
                        if recognized < encounter["start"]:
                            audit(
                                con,
                                encounter,
                                policy,
                                "skipped_signal",
                                "historical_signal",
                                recognized,
                            )
                            continue
                        candidates.append((recognized, index, policy, evidence))
            for recognized, index, policy, evidence in sorted(candidates):
                if transfer_episode(
                    con,
                    episode,
                    index,
                    policy,
                    config["policies"][policy],
                    recognized,
                    evidence,
                    config,
                    config_hash,
                    organizations,
                ):
                    break


def utilization(con: duckdb.DuckDBPyConnection) -> None:
    """Recompute from published encounters; repeated input counters cannot accumulate."""
    if "utilization" in columns(con, "organizations"):
        con.execute(
            "UPDATE organizations SET utilization = (SELECT count(*) FROM encounters WHERE organization = organizations.id)"
        )
    if "revenue" in columns(con, "organizations") and "total_claim_cost" in columns(
        con, "encounters"
    ):
        con.execute(
            "UPDATE organizations SET revenue = COALESCE((SELECT sum(total_claim_cost) FROM encounters WHERE organization = organizations.id), 0)"
        )
    if "encounters" in columns(con, "providers"):
        con.execute(
            "UPDATE providers SET encounters = (SELECT count(*) FROM encounters WHERE provider = providers.id)"
        )
    if "procedures" in tables(con) and "procedures" in columns(con, "providers"):
        con.execute("""UPDATE providers SET procedures = (SELECT count(*) FROM procedures p JOIN encounters e
            ON p.encounter = e.id WHERE e.provider = providers.id)""")


def validate(con: duckdb.DuckDBPyConnection) -> None:
    checks = {
        "transfer chronology": "SELECT count(*) FROM transfers WHERE NOT (recognized_at <= accepted_at AND accepted_at <= departed_at AND departed_at < arrived_at)",
        "duplicate transfers": "SELECT count(*) - count(DISTINCT id) FROM transfers",
        "duplicate notes": "SELECT count(*) - count(DISTINCT note_id) FROM notes",
        "duplicate orders": "SELECT count(*) - count(DISTINCT id) FROM transfer_orders",
        "duplicate encounters": "SELECT count(*) - count(DISTINCT id) FROM encounters",
        "receiving provider": "SELECT count(*) FROM transfers t LEFT JOIN providers p ON t.accepting_provider = p.id WHERE p.organization IS DISTINCT FROM t.destination_organization",
        "transfer patient linkage": "SELECT count(*) FROM transfers t LEFT JOIN encounters s ON t.source_encounter = s.id LEFT JOIN encounters d ON t.destination_encounter = d.id WHERE s.patient IS DISTINCT FROM t.patient OR d.patient IS DISTINCT FROM t.patient OR s.stop IS DISTINCT FROM t.departed_at OR d.start IS DISTINCT FROM t.arrived_at",
        "recognition linkage": "SELECT count(*) FROM transfers t LEFT JOIN encounters e ON t.recognition_encounter=e.id WHERE e.patient IS DISTINCT FROM t.patient OR t.recognized_at < e.start OR t.recognized_at >= e.stop",
        "transport procedure overlap": "SELECT count(*) FROM transfers t JOIN procedures p ON p.patient=t.patient WHERE p.start < t.arrived_at AND (COALESCE(p.stop,p.start) > t.departed_at OR p.start >= t.departed_at)",
        "order predecessor": "SELECT count(*) FROM transfer_orders a LEFT JOIN transfer_orders b ON a.prior_order_id = b.id WHERE a.prior_order_id IS NOT NULL AND b.id IS NULL",
    }
    for name, query in checks.items():
        if name == "transport procedure overlap" and "procedures" not in tables(con):
            continue
        if con.execute(query).fetchone()[0]:
            raise ValueError(f"Transfer validation failed: {name}")
    for table in tables(con):
        cols = columns(con, table)
        if "encounter" in cols and "patient" in cols:
            count = con.execute(
                f"SELECT count(*) FROM {identifier(table)} t LEFT JOIN encounters e ON t.encounter = e.id WHERE t.encounter IS NOT NULL AND t.patient IS DISTINCT FROM e.patient"
            ).fetchone()[0]
            if count:
                raise ValueError(f"Invalid patient/encounter linkage in {table}")


def run(source: Path, output: Path, policy_path: Path = DEFAULT_POLICY) -> dict:
    source = source.resolve(strict=True)
    if (source / ".parquet-publication.json").exists():
        raise ValueError(
            "Source conversion is incomplete; rerun conversion to recover publication first"
        )
    output = (
        output.parent.resolve() / output.name
    )  # Keep the publication symlink, not its target.
    if (
        source == output.resolve()
        or source in output.resolve().parents
        or output.resolve() in source.parents
    ):
        raise ValueError("Source and output must be separate, non-nested directories")
    config, config_hash = read_policy(policy_path)
    with output_lock(output):
        if output.exists() and not output.is_symlink():
            raise ValueError(
                "Output must be absent or an existing transfer snapshot symlink"
            )
        previous = output.resolve() if output.exists() else None
        old = (
            json.loads((previous / "_transfers.json").read_text()) if previous else None
        )
        if old and old["config_hash"] != config_hash:
            raise ValueError(
                "Transfer policy/version changed; use a new output directory for an explicit rebuild"
            )
        files = inventory(source)
        if "transfers" in files:
            raise ValueError(
                "Input must be the original generated dataset, not a transfer projection"
            )
        if not {"patients", "encounters", "organizations", "providers"} <= files.keys():
            raise ValueError("Missing patients, encounters, organizations or providers")
        if old and old["source_files"] == files:
            return {"status": "unchanged", "new_patients": 0, "snapshot": str(previous)}
        with (
            tempfile.TemporaryDirectory(prefix="hospital-transfers-") as scratch,
            duckdb.connect() as con,
        ):
            con.execute("SET TimeZone='UTC'")
            con.execute("SET threads=4")
            con.execute("SET memory_limit='4GB'")
            con.execute(f"SET temp_directory={literal(scratch)}")
            load_source(con, source, files)
            if "transfer_signals" in files:
                types = {
                    r[0]: r[1]
                    for r in con.execute("DESCRIBE transfer_signals").fetchall()
                }
                if (
                    not {"id", "patient", "encounter", "policy", "recognized_at"}
                    <= types.keys()
                    or types["recognized_at"] != "TIMESTAMP WITH TIME ZONE"
                ):
                    raise ValueError(
                        "transfer_signals requires id, patient, encounter, policy and recognized_at TIMESTAMPTZ"
                    )
                if con.execute(
                    "SELECT count(*) FROM transfer_signals WHERE policy NOT IN ('cardiac','liver') OR policy IS NULL OR recognized_at IS NULL OR id IS NULL"
                ).fetchone()[0]:
                    raise ValueError("Invalid transfer signal policy, ID or timestamp")
            hashes = patient_hashes(con)
            old_hashes = old["patient_hashes"] if old else {}
            changed = [p for p, digest in old_hashes.items() if hashes.get(p) != digest]
            if changed:
                raise ValueError(
                    f"{len(changed)} existing patient histories changed or disappeared; use a new output for a rebuild"
                )
            if previous:
                merge_previous(con, previous, set(old_hashes))
            prepare(con)
            new_patients = set(hashes) - set(old_hashes)
            process_new(con, new_patients, config, config_hash)
            utilization(con)
            validate(con)
            if inventory(source) != files:
                raise ValueError(
                    "Source changed during processing; rerun after generation finishes"
                )
            manifest = dict(
                engine=ENGINE_VERSION,
                config_hash=config_hash,
                policy=config,
                source_files=files,
                patient_hashes=hashes,
                source=str(source),
            )
            snapshot = publish(con, output, manifest)
            return {
                "status": "published",
                "new_patients": len(new_patients),
                "snapshot": str(snapshot),
                "transfers": con.execute("SELECT count(*) FROM transfers").fetchone()[
                    0
                ],
            }


def main() -> None:
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("output/parquet"))
    parser.add_argument("--output", type=Path, default=Path("output/transfers"))
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    args = parser.parse_args()
    print(json.dumps(run(args.input, args.output, args.policy), indent=2))


if __name__ == "__main__":
    main()
