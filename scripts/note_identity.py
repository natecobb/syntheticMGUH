"""Stable identities for source notes, including repeated identical exports."""


def filter_notes_before_launch(con, launch_date: str) -> int:
    """Apply the EHR's New York calendar-date boundary after parsing timestamps."""
    predicate = "date < timezone('America/New_York', CAST(? AS TIMESTAMP))"
    removed = con.execute(
        f"SELECT count(*) FROM notes WHERE {predicate}", [launch_date]
    ).fetchone()[0]
    con.execute(f"DELETE FROM notes WHERE {predicate}", [launch_date])
    return removed


def assign_note_ids(con, table: str) -> None:
    if table not in {"notes", "notes_llm", "existing_notes"}:
        raise ValueError("Unsupported note table")
    con.execute(f"""UPDATE {table} t SET note_id = n.identity
        FROM (SELECT rowid AS rid, sha256(concat_ws('|', patient, encounter, date, note_text,
            row_number() OVER (PARTITION BY patient, encounter, date, note_text ORDER BY rowid))) AS identity
            FROM {table}) n
        WHERE t.rowid = n.rid AND t.note_id IS NULL""")
