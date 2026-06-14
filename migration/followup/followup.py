import json
import sys
import os
import argparse
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.db import get_legacy_engine, get_new_engine
from config.config import BATCH_SIZE, DRY_RUN, PREVIEW_SAMPLE_SIZE
from utils.id_gen import SafeIDGenerator, get_migrated_legacy_ids
from utils.logger import get_logger


log = get_logger("migrate_followup_schedule")

LEGACY_SOURCE_TABLE = "Appointments_followups"
LEGACY_SOURCE = "digiswasthya_database"

TARGET_APPDETAILS_ID: int = 42

# ---------------------------------------------------------------------------
# STATUS MAPPING
# Confirmed from real legacy Appointments_followups data:
#   0 → PENDING      (auto-created, no coordinator assigned yet)
#   1 → COMPLETED    (has coordinator, linked to a healthcase/visit)
#   2 → CANCELLED    (explicit cancellation)
#   3 → RESCHEDULED
#   4 → MISSED       (date passed, patient did not attend)
#
# TODO: verify no other values exist by running on legacy DB:
#   SELECT DISTINCT status, COUNT(*) FROM "Appointments_followups" GROUP BY status;
# ---------------------------------------------------------------------------

STATUS_MAP: dict[int, str] = {
    0: "PENDING",
    1: "COMPLETED",
    2: "CANCELLED",
    3: "RESCHEDULED",
    4: "MISSED",
}

# Statuses where comment → cancelled_reason (not notes)
CANCELLED_STATUSES: frozenset[int] = frozenset({2})

# Auto-generated system comments — not meaningful user notes, skip storing
_AUTO_COMMENT_PREFIXES: tuple[str, ...] = (
    "Auto-generated followup from prescription",
    "Auto-created followup due to status change",
    "Prescription follow_up_at cleared/changed",
)


# ---------------------------------------------------------------------------
# SQL — fetch legacy followups scoped to appdetails_id = 42
# Joins via healthcase to scope to the right app.
# sequence_no derived as ROW_NUMBER per person ordered by followupdate.
# ---------------------------------------------------------------------------

_FETCH_SQL = text("""
    SELECT
        f.id,
        f.followupdate,
        f.status,
        f.comment,
        f.created_at,
        f.updated_at,
        f.person_id,
        f.healthcase_id,
        f.coordinator_id,
        ROW_NUMBER() OVER (
            PARTITION BY f.person_id
            ORDER BY f.followupdate ASC NULLS LAST, f.id ASC
        ) AS sequence_no
    FROM "Appointments_followups" f
    WHERE f.person_id IN (
        SELECT DISTINCT hc.person_id
        FROM "HealthCase_healthcase" hc
        WHERE hc.appdetails_id = :target_appdetails_id
          AND hc.person_id IS NOT NULL
    )
      AND f.person_id IS NOT NULL
    ORDER BY f.person_id ASC, f.followupdate ASC NULLS LAST, f.id ASC
""")

_FETCH_LIMITED_SQL = text("""
    SELECT
        f.id,
        f.followupdate,
        f.status,
        f.comment,
        f.created_at,
        f.updated_at,
        f.person_id,
        f.healthcase_id,
        f.coordinator_id,
        ROW_NUMBER() OVER (
            PARTITION BY f.person_id
            ORDER BY f.followupdate ASC NULLS LAST, f.id ASC
        ) AS sequence_no
    FROM "Appointments_followups" f
    WHERE f.person_id IN (
        SELECT DISTINCT hc.person_id
        FROM "HealthCase_healthcase" hc
        WHERE hc.appdetails_id = :target_appdetails_id
          AND hc.person_id IS NOT NULL
    )
      AND f.person_id IS NOT NULL
    ORDER BY f.person_id ASC, f.followupdate ASC NULLS LAST, f.id ASC
    LIMIT :lim
""")


# ---------------------------------------------------------------------------
# INSERT SQL
# ---------------------------------------------------------------------------

INSERT_SQL = text("""
    INSERT INTO followup_schedule (
        id,
        treatment_plan_id,
        sequence_no,
        scheduled_date,
        status,
        completed_visit_id,
        assigned_coordinator_id,
        reminder_sent_at,
        rescheduled_from_id,
        cancelled_reason,
        notes,
        created_at,
        updated_at,
        patient_id,
        source_prescription_id,
        resolution,
        resolution_notes,
        resolved_by_user_id,
        resolved_at,
        legacy_id,
        legacy_source
    ) VALUES (
        :id,
        :treatment_plan_id,
        :sequence_no,
        :scheduled_date,
        :status,
        :completed_visit_id,
        :assigned_coordinator_id,
        :reminder_sent_at,
        :rescheduled_from_id,
        :cancelled_reason,
        :notes,
        :created_at,
        :updated_at,
        :patient_id,
        :source_prescription_id,
        :resolution,
        :resolution_notes,
        :resolved_by_user_id,
        :resolved_at,
        :legacy_id,
        :legacy_source
    )
    ON CONFLICT (id) DO NOTHING
""")


# ---------------------------------------------------------------------------
# Lookups — new DB
# ---------------------------------------------------------------------------

def load_person_id_to_patient_uuid(new_engine: Any) -> dict[str, str]:
    """{ legacy person_id (str) → patient.id (UUID str) }"""
    with new_engine.connect() as conn:
        rows = conn.execute(
            text("SELECT legacy_id, id::text FROM patient WHERE legacy_id IS NOT NULL")
        ).fetchall()
    mapping = {row[0]: row[1] for row in rows}
    log.info("Loaded %d patient legacy_id → UUID mappings", len(mapping))
    return mapping


def load_healthcase_id_to_visit_uuid(new_engine: Any) -> dict[str, str]:
    """{ legacy healthcase_id (str) → visit.id (UUID str) }"""
    with new_engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT legacy_id, id::text
                FROM visit
                WHERE legacy_id IS NOT NULL
                  AND legacy_source = :src
            """),
            {"src": LEGACY_SOURCE},
        ).fetchall()
    mapping = {row[0]: row[1] for row in rows}
    log.info("Loaded %d visit legacy_id → UUID mappings", len(mapping))
    return mapping


def load_coordinator_id_to_user_uuid(new_engine: Any) -> dict[str, str]:
    """
    { legacy coordinator_id (str) → user.id (UUID str) }

    Coordinators were migrated with a legacy_id in the user table.
    TODO: Confirm the legacy_id column name in the user table for coordinators.
          Run: SELECT column_name FROM information_schema.columns
               WHERE table_name = 'user' AND column_name ILIKE '%legacy%';
    """
    with new_engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT legacy_id, id::text
                FROM "user"
                WHERE legacy_id IS NOT NULL
                  AND role = 'COORDINATOR'
                  AND is_active = TRUE
            """)
        ).fetchall()
    mapping = {row[0]: row[1] for row in rows}
    log.info("Loaded %d coordinator legacy_id → UUID mappings", len(mapping))
    return mapping


# ---------------------------------------------------------------------------
# Status mapper
# ---------------------------------------------------------------------------

def map_status(raw_status: Any) -> str:
    """
    Map legacy integer status to new DB string status.
    Falls back to PENDING for unknown values.
    Known legacy values: 0=PENDING, 1=COMPLETED, 2=CANCELLED, 3=RESCHEDULED, 4=MISSED.
    """
    if raw_status is None:
        return "PENDING"
    try:
        mapped = STATUS_MAP.get(int(raw_status))
    except (TypeError, ValueError):
        mapped = None

    if mapped is None:
        log.warning("Unknown legacy followup status %r — defaulting to PENDING", raw_status)
        return "PENDING"

    return mapped


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def build_followup_record(
    row: Any,
    new_id: str,
    patient_uuid: str,
    visit_uuid: str | None,
    coordinator_uuid: str | None,
) -> dict:
    raw_status  = row.get("status")
    status_int  = None
    try:
        status_int = int(raw_status) if raw_status is not None else None
    except (TypeError, ValueError):
        pass

    status      = map_status(raw_status)
    raw_comment = (row.get("comment") or "").strip()

    # Filter out auto-generated system messages — they carry no user meaning.
    # Only keep comment if it is a real user-entered note.
    is_auto_comment = any(raw_comment.startswith(p) for p in _AUTO_COMMENT_PREFIXES)
    comment = None if (not raw_comment or is_auto_comment) else raw_comment

    # comment → cancelled_reason when CANCELLED; otherwise → notes
    cancelled_reason = comment if status_int in CANCELLED_STATUSES else None
    notes            = comment if status_int not in CANCELLED_STATUSES else None

    # completed_visit_id: link to visit when status is COMPLETED.
    # MISSED rows have a healthcase_id but the visit was never completed — leave NULL.
    completed_visit_id = visit_uuid if status == "COMPLETED" else None

    return {
        "id":                       new_id,
        "treatment_plan_id":        None,           # no legacy source
        "sequence_no":              int(row.get("sequence_no", 1)),
        "scheduled_date":           row.get("followupdate"),
        "status":                   status,
        "completed_visit_id":       completed_visit_id,
        "assigned_coordinator_id":  coordinator_uuid,
        "reminder_sent_at":         None,           # no legacy source
        "rescheduled_from_id":      None,           # no legacy source
        "cancelled_reason":         cancelled_reason,
        "notes":                    notes,
        "created_at":               row.get("created_at"),
        "updated_at":               row.get("updated_at"),
        "patient_id":               patient_uuid,
        "source_prescription_id":   None,           # no legacy source
        "resolution":               None,           # no legacy source
        "resolution_notes":         None,           # no legacy source
        "resolved_by_user_id":      None,           # no legacy source
        "resolved_at":              None,           # no legacy source
        "legacy_id":                str(row["id"]),
        "legacy_source":            LEGACY_SOURCE,
    }


# ---------------------------------------------------------------------------
# Batch flush
# ---------------------------------------------------------------------------

def _flush_batch(new_engine: Any, batch: list[dict]) -> None:
    if DRY_RUN:
        log.info("[DRY RUN] Would insert %d followup_schedule rows.", len(batch))
        return

    with Session(new_engine) as session:
        session.execute(INSERT_SQL, batch)
        session.commit()


# ---------------------------------------------------------------------------
# Preview
# ---------------------------------------------------------------------------

def preview_followup_schedules(limit: int | None = None) -> None:
    if limit is None:
        limit = PREVIEW_SAMPLE_SIZE

    legacy_engine = get_legacy_engine()
    new_engine    = get_new_engine()

    person_to_patient     = load_person_id_to_patient_uuid(new_engine)
    healthcase_to_visit   = load_healthcase_id_to_visit_uuid(new_engine)
    coordinator_to_user   = load_coordinator_id_to_user_uuid(new_engine)

    with legacy_engine.connect() as conn:
        rows = conn.execute(
            _FETCH_LIMITED_SQL,
            {"target_appdetails_id": TARGET_APPDETAILS_ID, "lim": limit},
        ).mappings().all()

    log.info(
        "════════ PREVIEW: followup_schedule migration appdetails_id=%d — %d row(s) ════════",
        TARGET_APPDETAILS_ID,
        len(rows),
    )

    for i, row in enumerate(rows, start=1):
        legacy_id    = row.get("id")
        person_id    = str(row.get("person_id", ""))
        healthcase_id = str(row.get("healthcase_id", "")) if row.get("healthcase_id") else None
        coord_id     = str(row.get("coordinator_id", "")) if row.get("coordinator_id") else None

        patient_uuid     = person_to_patient.get(person_id)
        visit_uuid       = healthcase_to_visit.get(healthcase_id) if healthcase_id else None
        coordinator_uuid = coordinator_to_user.get(coord_id) if coord_id else None

        if patient_uuid is None:
            log.warning(
                "PREVIEW [%d/%d] followup_id=%s — WOULD SKIP: "
                "no patient found for person_id=%s",
                i, len(rows), legacy_id, person_id,
            )
            continue

        record = build_followup_record(
            row,
            f"preview-{legacy_id}",
            patient_uuid,
            visit_uuid,
            coordinator_uuid,
        )

        log.info(
            "PREVIEW [%d/%d] followup_id=%s person_id=%s\n%s",
            i, len(rows), legacy_id, person_id,
            json.dumps(record, indent=2, default=str),
        )

    log.info("════════ PREVIEW complete — no rows written to new DB ════════")


# ---------------------------------------------------------------------------
# Main migration
# ---------------------------------------------------------------------------

def migrate_followup_schedules(limit: int | None = None) -> None:
    """
    Migrate Appointments_followups rows (scoped to appdetails_id=42 patients)
    to followup_schedule table in new DB.

    Prerequisites (run in order):
      1. migrate_patient_appid42.py
      2. migrate_visit_appid42.py       ← visit.legacy_id needed for completed_visit_id
      3. migrate_followup_schedule_appid42.py  ← this script
    """
    legacy_engine = get_legacy_engine()
    new_engine    = get_new_engine()

    person_to_patient   = load_person_id_to_patient_uuid(new_engine)
    healthcase_to_visit = load_healthcase_id_to_visit_uuid(new_engine)
    coordinator_to_user = load_coordinator_id_to_user_uuid(new_engine)

    already_migrated: set[str] = get_migrated_legacy_ids(new_engine, "followup_schedule")
    log.info("Already migrated: %d followup_schedule rows in new DB", len(already_migrated))

    with legacy_engine.connect() as conn:
        if limit is not None:
            rows = conn.execute(
                _FETCH_LIMITED_SQL,
                {"target_appdetails_id": TARGET_APPDETAILS_ID, "lim": limit},
            ).mappings().all()
        else:
            rows = conn.execute(
                _FETCH_SQL,
                {"target_appdetails_id": TARGET_APPDETAILS_ID},
            ).mappings().all()

    total = len(rows)
    log.info(
        "══ SOURCE ══  Appointments_followups eligible (appdetails_id=%d) "
        "fetched=%d (limit=%s)",
        TARGET_APPDETAILS_ID,
        total,
        limit if limit is not None else "ALL",
    )

    id_gen = SafeIDGenerator(new_engine, table="followup_schedule")

    batch: list[dict] = []
    inserted = 0
    skipped_migrated = skipped_no_patient = skipped_no_date = errors = 0
    with_visit = with_coordinator = 0

    for row in rows:
        legacy_id = str(row.get("id", ""))

        # 1. Already migrated → skip (safe re-run).
        if legacy_id in already_migrated:
            log.debug("SKIP (already migrated) followup_id=%s", legacy_id)
            skipped_migrated += 1
            continue

        # 2. followupdate is NOT NULL in new DB — skip rows without a date.
        if row.get("followupdate") is None:
            log.warning(
                "SKIP followup_id=%s — followupdate is NULL (required field)",
                legacy_id,
            )
            skipped_no_date += 1
            continue

        # 3. Must have a patient.
        person_id    = str(row.get("person_id", ""))
        patient_uuid = person_to_patient.get(person_id)
        if patient_uuid is None:
            log.warning(
                "SKIP followup_id=%s — no patient found for person_id=%s",
                legacy_id, person_id,
            )
            skipped_no_patient += 1
            continue

        try:
            healthcase_id    = str(row.get("healthcase_id")) if row.get("healthcase_id") else None
            coord_id         = str(row.get("coordinator_id")) if row.get("coordinator_id") else None

            visit_uuid       = healthcase_to_visit.get(healthcase_id) if healthcase_id else None
            coordinator_uuid = coordinator_to_user.get(coord_id) if coord_id else None

            if visit_uuid is None and healthcase_id:
                log.debug(
                    "followup_id=%s — healthcase_id=%s not found in visit table "
                    "(visit may have been skipped); completed_visit_id will be NULL",
                    legacy_id, healthcase_id,
                )

            if coordinator_uuid is None and coord_id:
                log.debug(
                    "followup_id=%s — coordinator_id=%s not found in user table; "
                    "assigned_coordinator_id will be NULL",
                    legacy_id, coord_id,
                )

            new_id = id_gen.next()
            record = build_followup_record(
                row,
                new_id,
                patient_uuid,
                visit_uuid,
                coordinator_uuid,
            )

            if visit_uuid is not None:
                with_visit += 1
            if coordinator_uuid is not None:
                with_coordinator += 1

            batch.append(record)

            if len(batch) >= BATCH_SIZE:
                _flush_batch(new_engine, batch)
                inserted += len(batch)
                log.info("  … %d / %d rows committed", inserted, total)
                batch.clear()

        except Exception as e:
            log.error("ERROR processing followup_id=%s: %s", legacy_id, e)
            errors += 1
            continue

    if batch:
        _flush_batch(new_engine, batch)
        inserted += len(batch)

    log.info(
        "═══ followup_schedule migration complete (appdetails_id=%d) ═══  "
        "limit=%s | fetched=%d | inserted=%d | "
        "skipped(re-run)=%d | skipped(no-patient)=%d | skipped(no-date)=%d | errors=%d | "
        "with_visit=%d | with_coordinator=%d",
        TARGET_APPDETAILS_ID,
        limit if limit is not None else "ALL",
        total,
        inserted,
        skipped_migrated,
        skipped_no_patient,
        skipped_no_date,
        errors,
        with_visit,
        with_coordinator,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            f"Migrate followup schedules (Appointments_followups) for appdetails_id={TARGET_APPDETAILS_ID} "
            "(DS-TMC-009 / Narsala) from legacy DB to followup_schedule in new DB."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python migrate_followup_schedule_appid42.py              # migrate ALL
  python migrate_followup_schedule_appid42.py --limit 10   # migrate first 10
  python migrate_followup_schedule_appid42.py --preview    # preview only, no DB writes

Prerequisites (run in order):
  1. migrate_patient_appid42.py
  2. migrate_visit_appid42.py                    ← must run before this
  3. migrate_followup_schedule_appid42.py        ← this script
        """,
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Migrate only the first N rows. Omit to migrate ALL.",
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Run preview only — no rows are written to the new DB.",
    )
    args = parser.parse_args()

    preview_sample = args.limit if args.limit is not None else PREVIEW_SAMPLE_SIZE
    preview_followup_schedules(limit=preview_sample)

    if args.preview:
        log.info("--preview flag set — exiting without migrating.")
        sys.exit(0)

    limit_label = str(args.limit) if args.limit is not None else "ALL"
    log.info(
        "Review the PREVIEW logs above. "
        "About to migrate followup_schedule rows for %s record(s) (appdetails_id=%d). "
        "Type 'yes' to proceed.",
        limit_label,
        TARGET_APPDETAILS_ID,
    )
    answer = input("Migrate to new DB? (yes/no): ").strip().lower()
    if answer != "yes":
        log.info("Migration cancelled — no data written to new DB.")
        sys.exit(0)

    migrate_followup_schedules(limit=args.limit)