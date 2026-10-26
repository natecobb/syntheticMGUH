"""Resolve synthetic SSN collisions while preserving published identities."""

import hashlib


def allocate_ssns(con, existing_table=None):
    """Repair incoming duplicates deterministically; never change existing rows.

    Reserve all original SSNs before allocating replacements, including values
    belonging to later rows. Sorted patient IDs and linear probing make collisions
    independent of row order and stable on replay. Null SSNs remain unknown.
    """
    existing = []
    if existing_table is not None:
        if existing_table != "_existing_patients":
            raise ValueError("Unsupported existing patient table")
        existing = con.execute("SELECT id, ssn FROM _existing_patients").fetchall()
    occupied = {ssn for _, ssn in existing if ssn is not None}
    if len(occupied) != sum(ssn is not None for _, ssn in existing):
        raise ValueError(
            "Existing SSNs are duplicated; repair the source before appending"
        )
    rows = con.execute("SELECT id, ssn FROM patients ORDER BY id").fetchall()
    if len({patient for patient, _ in rows}) != len(rows):
        raise ValueError("Patient IDs must be unique before SSN allocation")
    reserved = occupied | {ssn for _, ssn in rows if ssn is not None}
    changes = []
    for patient, ssn in rows:
        if ssn is None:
            continue
        if ssn not in occupied:
            occupied.add(ssn)
            continue
        offset = (
            int(
                hashlib.sha256(
                    ("syntheticMGUH/ssn/v1/" + patient).encode()
                ).hexdigest(),
                16,
            )
            % 810000
        )
        for step in range(810000):
            candidate = (offset + step) % 810000
            replacement = (
                f"999-{10 + candidate // 9000:02d}-{1000 + candidate % 9000:04d}"
            )
            if replacement not in reserved:
                break
        else:
            raise ValueError("Synthetic SSN allocation space exhausted")
        reserved.add(replacement)
        occupied.add(replacement)
        changes.append((patient, ssn, replacement))
    if changes:
        con.executemany(
            "UPDATE patients SET ssn = ? WHERE id = ?",
            [(new, patient) for patient, _, new in changes],
        )
    return changes
