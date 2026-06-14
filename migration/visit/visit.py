import json
import sys
import os
import argparse
import random
from datetime import datetime, timezone
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from psycopg2.extras import Json
from sqlalchemy import text
from sqlalchemy.orm import Session

from config.db import get_legacy_engine, get_new_engine
from config.config import BATCH_SIZE, DRY_RUN, PREVIEW_SAMPLE_SIZE
from utils.id_gen import SafeIDGenerator, get_migrated_legacy_ids
from utils.logger import get_logger


log = get_logger("migrate_visit")

LEGACY_SOURCE_TABLE = "HealthCase_healthcase"
LEGACY_SOURCE = "digiswasthya_database"

TARGET_APPDETAILS_ID: int = 42

# ---------------------------------------------------------------------------
# STATUS MAPPING
# Legacy status is an integer. New DB expects a string (USER-DEFINED type).
# 0, 1, 3 → COMPLETED (confirmed by manager)
# None / unknown → ACTIVE (safe fallback)
# ---------------------------------------------------------------------------

STATUS_MAP: dict[int, str] = {
    0: "COMPLETED",
    1: "COMPLETED",
    3: "COMPLETED",
}

# ---------------------------------------------------------------------------
# APPID → CENTER_CODE mapping (same as patient migration)
# ---------------------------------------------------------------------------

APPID_TO_CENTER_CODE: dict[int, str] = {
    45: "DS-TMC-012",
    42: "DS-TMC-009",   # Narsala — our target
    41: "DS-TMC-015",
    43: "DS-TMC-010",
    44: "DS-TMC-011",
    37: "DS-TMC-004",
    67: "TMC-DSF0018",
    40: "DS-TMC-007",
    24: "DS-TMC-013",
}

UNRESOLVED_APP_IDS: frozenset[int] = frozenset({
    15, 16, 17, 18, 19, 20, 21, 31, 32, 36,
})


# ---------------------------------------------------------------------------
# SQL — fetch only healthcases scoped to appdetails_id = 42
# ---------------------------------------------------------------------------

_FETCH_HEALTHCASES_SQL = text("""
    SELECT DISTINCT ON (hc.id)
        hc.id,
        hc.healthcaseid,
        hc.problem,
        hc.status,
        hc.created_at,
        hc.lastupdated,
        hc.person_id,
        hc.doctor_id,
        hc.appdetails_id
    FROM "HealthCase_healthcase" hc
    WHERE hc.appdetails_id = :target_appdetails_id
      AND hc.person_id IS NOT NULL
    ORDER BY hc.id ASC
""")

_FETCH_HEALTHCASES_LIMITED_SQL = text("""
    SELECT DISTINCT ON (hc.id)
        hc.id,
        hc.healthcaseid,
        hc.problem,
        hc.status,
        hc.created_at,
        hc.lastupdated,
        hc.person_id,
        hc.doctor_id,
        hc.appdetails_id
    FROM "HealthCase_healthcase" hc
    WHERE hc.appdetails_id = :target_appdetails_id
      AND hc.person_id IS NOT NULL
    ORDER BY hc.id ASC
    LIMIT :lim
""")

# ---------------------------------------------------------------------------
# SQL — fetch latest comment (vitals + history) per healthcase
# ---------------------------------------------------------------------------

_FETCH_COMMENTS_SQL = text("""
    SELECT DISTINCT ON (c.healthcase_id)
        c.healthcase_id,
        c.commenttext
    FROM "HealthCase_commentshealthcase" c
    WHERE c.healthcase_id IS NOT NULL
      AND c.commenttext IS NOT NULL
      AND TRIM(c.commenttext) <> ''
    ORDER BY
        c.healthcase_id,
        c.datetime DESC NULLS LAST,
        c.commentid DESC NULLS LAST
""")

# ---------------------------------------------------------------------------
# SQL — fetch doctor_id per healthcase from BodyVitals_prescription
# then join Doctor_doctordetails to get name and phone.
# ---------------------------------------------------------------------------

_FETCH_HEALTHCASE_DOCTOR_SQL = text("""
    SELECT DISTINCT ON (p.healthcase_id)
        p.healthcase_id,
        p.doctor_id,
        d.name   AS doctor_name,
        d.phone  AS doctor_phone
    FROM "BodyVitals_prescription" p
    INNER JOIN "Doctor_doctordetails" d
        ON d.doctorid = p.doctor_id
    WHERE p.healthcase_id IS NOT NULL
      AND p.doctor_id IS NOT NULL
    ORDER BY
        p.healthcase_id,
        p.datetime DESC NULLS LAST,
        p.id DESC
""")


# ---------------------------------------------------------------------------
# Vitals builder
# ---------------------------------------------------------------------------

def build_vitals_json(commenttext: str | None) -> dict:
    """
    Parse vitals from legacy commenttext JSON.
    Always returns a dict (never None) because vitals_json is NOT NULL in new DB.
    Missing fields default to None.

    FIX: spo2 checks both "Spo2 " (trailing space, legacy key) and "Spo2" (no space)
    to handle inconsistent key naming across legacy records.
    """
    empty = {
        "bp": None,
        "spo2": None,
        "pulse": None,
        "temp_f": None,
        "height_cm": None,
        "weight_kg": None,
        "sugar_mg_dl": None,
    }

    if not commenttext or not str(commenttext).strip():
        return empty

    try:
        raw: dict = json.loads(commenttext)
    except (json.JSONDecodeError, TypeError):
        log.warning("Could not parse commenttext as JSON for vitals: %s", str(commenttext)[:120])
        return empty

    if not isinstance(raw, dict):
        return empty

    def _safe_num(val: Any) -> Any:
        if val is None or str(val).strip() in ("", "None", "null"):
            return None
        try:
            return float(val) if "." in str(val) else int(val)
        except (ValueError, TypeError):
            return None

    return {
        "bp":          raw.get("BP"),
        # FIX: fallback from "Spo2 " (trailing space) to "Spo2" (no space)
        "spo2":        _safe_num(raw.get("Spo2 ") or raw.get("Spo2")),
        "pulse":       _safe_num(raw.get("Pulse")),
        "temp_f":      _safe_num(raw.get("Temperature")),
        "height_cm":   _safe_num(raw.get("Height")),
        "weight_kg":   _safe_num(raw.get("Weight")),
        "sugar_mg_dl": None,
    }


# ---------------------------------------------------------------------------
# Status mapper
# ---------------------------------------------------------------------------

def map_status(raw_status: Any) -> str:
    if raw_status is None:
        return "ACTIVE"
    try:
        mapped = STATUS_MAP.get(int(raw_status))
    except (TypeError, ValueError):
        mapped = None

    if mapped is None:
        log.warning("Unknown legacy status %r — defaulting to ACTIVE", raw_status)
        return "ACTIVE"

    return mapped


# ---------------------------------------------------------------------------
# Center resolver
# ---------------------------------------------------------------------------

def load_center_code_to_uuid(new_engine: Any) -> dict[str, str]:
    with new_engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id::text, center_code FROM center WHERE center_code IS NOT NULL")
        ).fetchall()

    mapping = {row[1].strip(): row[0] for row in rows if row[1] and row[1].strip()}
    log.info("Loaded %d center records from new DB: %s", len(mapping), sorted(mapping.keys()))
    return mapping


def resolve_center_id(
    appdetails_id: Any,
    center_code_to_uuid: dict[str, str],
) -> str | None:
    if appdetails_id is None:
        return None
    try:
        app_id = int(appdetails_id)
    except (TypeError, ValueError):
        return None

    if app_id in UNRESOLVED_APP_IDS:
        return None

    center_code = APPID_TO_CENTER_CODE.get(app_id)
    if center_code is None:
        return None

    center_uuid = center_code_to_uuid.get(center_code)
    if center_uuid is None:
        log.warning(
            "No center UUID found for appdetails_id=%s center_code=%s",
            app_id, center_code,
        )
    return center_uuid


# ---------------------------------------------------------------------------
# Patient UUID loader
# ---------------------------------------------------------------------------

def load_person_id_to_patient_uuid(new_engine: Any) -> dict[str, str]:
    with new_engine.connect() as conn:
        rows = conn.execute(
            text("SELECT legacy_id, id::text FROM patient WHERE legacy_id IS NOT NULL")
        ).fetchall()

    mapping = {row[0]: row[1] for row in rows}
    log.info("Loaded %d patient legacy_id → UUID mappings from new DB", len(mapping))
    return mapping


# ---------------------------------------------------------------------------
# Coordinator loader
# ---------------------------------------------------------------------------

def load_coordinator_ids(new_engine: Any, center_uuid: str | None) -> list[str]:
    if center_uuid is None:
        log.warning("center_uuid is None — cannot load coordinators, created_by_user_id will be NULL")
        return []

    try:
        with new_engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT id::text
                    FROM "user"
                    WHERE center_id = :center_uuid
                      AND is_active = TRUE
                      AND role = 'COORDINATOR'
                    -- TODO: confirm role string — change 'COORDINATOR' if needed
                """),
                {"center_uuid": center_uuid},
            ).fetchall()

        coordinator_ids = [row[0] for row in rows]
        log.info(
            "Loaded %d active coordinator(s) for center_id=%s",
            len(coordinator_ids),
            center_uuid,
        )
        if not coordinator_ids:
            log.warning(
                "No active coordinators found for center_id=%s — "
                "created_by_user_id will be NULL. "
                "TODO: check role string or center UUID.",
                center_uuid,
            )
        return coordinator_ids

    except Exception as e:
        log.warning("Could not load coordinators: %s — created_by_user_id will be NULL", e)
        return []


def pick_random_coordinator(coordinator_ids: list[str]) -> str | None:
    if not coordinator_ids:
        return None
    return random.choice(coordinator_ids)


# ---------------------------------------------------------------------------
# Doctor resolver
# ---------------------------------------------------------------------------

def load_healthcase_doctor_lookup(legacy_engine: Any) -> dict[int, tuple[str, str]]:
    with legacy_engine.connect() as conn:
        rows = conn.execute(_FETCH_HEALTHCASE_DOCTOR_SQL).fetchall()

    lookup: dict[int, tuple[str, str]] = {}
    for healthcase_id, doctor_id, doctor_name, doctor_phone in rows:
        if healthcase_id is not None:
            lookup[int(healthcase_id)] = (
                (doctor_name or "").strip(),
                (doctor_phone or "").strip(),
            )

    log.info(
        "Loaded doctor info for %d healthcases from BodyVitals_prescription",
        len(lookup),
    )
    return lookup


def _normalize_phone(phone: str) -> str:
    if not phone:
        return ""
    digits = "".join(c for c in phone if c.isdigit())
    if len(digits) > 10:
        digits = digits[-10:]
    return digits


def load_doctor_name_phone_to_uuid(new_engine: Any) -> tuple[dict[tuple[str, str], str], dict[str, str]]:
    """
    Load two lookups for active DOCTOR role users in new DB:
      1. { (normalized_name_lower, phone_last10) → user.id }  — primary (name + phone)
      2. { phone_last10 → user.id }                           — fallback (phone only)
    """
    with new_engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT
                    id::text,
                    LOWER(TRIM(COALESCE(first_name, '') || ' ' || COALESCE(last_name, ''))) AS full_name,
                    phone
                FROM "user"
                WHERE role = 'DOCTOR'
                  AND is_active = TRUE
            """)
        ).fetchall()

    name_phone_lookup: dict[tuple[str, str], str] = {}
    phone_only_lookup: dict[str, str] = {}
    for user_id, full_name, phone in rows:
        normalized_name  = (full_name or "").strip().lower()
        normalized_phone = _normalize_phone(phone or "")
        if normalized_name or normalized_phone:
            name_phone_lookup[(normalized_name, normalized_phone)] = user_id
        if normalized_phone:
            phone_only_lookup[normalized_phone] = user_id

    log.info(
        "Loaded %d active doctor(s) from new DB for name+phone matching",
        len(name_phone_lookup),
    )
    return name_phone_lookup, phone_only_lookup


def resolve_doctor_uuid(
    healthcase_id: int,
    healthcase_doctor_lookup: dict[int, tuple[str, str]],
    doctor_name_phone_to_uuid: tuple[dict[tuple[str, str], str], dict[str, str]],
) -> str | None:
    doctor_info = healthcase_doctor_lookup.get(healthcase_id)
    if doctor_info is None:
        return None

    doctor_name, doctor_phone = doctor_info
    normalized_name  = doctor_name.lower()
    normalized_phone = _normalize_phone(doctor_phone)

    name_phone_lookup, phone_only_lookup = doctor_name_phone_to_uuid

    # Primary: name + phone
    user_uuid = name_phone_lookup.get((normalized_name, normalized_phone))

    # Fallback: phone only (handles "Dr." prefix, swapped names, spelling differences)
    if user_uuid is None and normalized_phone:
        user_uuid = phone_only_lookup.get(normalized_phone)

    if user_uuid is None:
        log.debug(
            "No doctor match in new DB for healthcase_id=%s "
            "name=%r phone=%r — assigned_doctor_user_id will be NULL",
            healthcase_id, doctor_name, doctor_phone,
        )

    return user_uuid


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def build_visit_record(
    row: Any,
    new_id: str,
    patient_uuid: str,
    center_uuid: str | None,
    coordinator_uuid: str | None,
    doctor_uuid: str | None,
    vitals_json: dict,
    comment_text: str | None,
) -> dict:
    status_int = row.get("status")

    # FIX: completed_at set for ALL COMPLETED statuses (0, 1, 3), not just 3.
    # Manager confirmed 0, 1, 3 are all COMPLETED.
    completed_at = row.get("lastupdated") if status_int in (0, 1, 3) else None

    return {
        "id":                                        new_id,
        "patient_id":                                patient_uuid,
        "center_id":                                 center_uuid,
        "created_by_user_id":                        coordinator_uuid,
        "assigned_doctor_user_id":                   doctor_uuid,
        "status":                                    map_status(status_int),
        "chief_complaint":                           row.get("problem"),
        "complaint_duration":                        None,
        "history_notes":                             comment_text,
        "is_reconsultation":                         False,
        "vitals_json":                               vitals_json,
        "created_at":                                row.get("created_at"),
        "updated_at":                                row.get("lastupdated"),
        "completed_at":                              completed_at,
        "camp_id":                                   None,
        "resolved_followup_visit_id":                None,
        "family_living_count_at_visit":              None,
        "family_living_status_confirmed":            None,
        "family_living_status_updated_during_visit": None,
        "triage_session_id":                         None,
        "outcome":                                   None,
        "deleted_at":                                None,
        "tenant_id":                                 None,
        "consultation_type":                         None,
        "legacy_id":                                 str(row["id"]),
        "legacy_source":                             LEGACY_SOURCE,
        "resolved_followup_schedule_id":             None,
    }


def _prepare_row_for_insert(record: dict) -> dict:
    row = record.copy()
    vitals = row.get("vitals_json")
    if vitals is not None:
        row["vitals_json"] = Json(vitals)
    return row


# ---------------------------------------------------------------------------
# INSERT SQL
# ---------------------------------------------------------------------------

INSERT_SQL = text("""
    INSERT INTO visit (
        id,
        patient_id,
        center_id,
        created_by_user_id,
        assigned_doctor_user_id,
        status,
        chief_complaint,
        complaint_duration,
        history_notes,
        is_reconsultation,
        vitals_json,
        created_at,
        updated_at,
        completed_at,
        camp_id,
        resolved_followup_visit_id,
        family_living_count_at_visit,
        family_living_status_confirmed,
        family_living_status_updated_during_visit,
        triage_session_id,
        outcome,
        deleted_at,
        tenant_id,
        consultation_type,
        legacy_id,
        legacy_source,
        resolved_followup_schedule_id
    ) VALUES (
        :id,
        :patient_id,
        :center_id,
        :created_by_user_id,
        :assigned_doctor_user_id,
        :status,
        :chief_complaint,
        :complaint_duration,
        :history_notes,
        :is_reconsultation,
        :vitals_json,
        :created_at,
        :updated_at,
        :completed_at,
        :camp_id,
        :resolved_followup_visit_id,
        :family_living_count_at_visit,
        :family_living_status_confirmed,
        :family_living_status_updated_during_visit,
        :triage_session_id,
        :outcome,
        :deleted_at,
        :tenant_id,
        :consultation_type,
        :legacy_id,
        :legacy_source,
        :resolved_followup_schedule_id
    )
    ON CONFLICT (id) DO NOTHING
""")


# ---------------------------------------------------------------------------
# Batch flush
# ---------------------------------------------------------------------------

def _flush_batch(new_engine: Any, batch: list[dict]) -> None:
    if DRY_RUN:
        log.info("[DRY RUN] Would insert %d visit rows.", len(batch))
        return

    payload = [_prepare_row_for_insert(r) for r in batch]
    with Session(new_engine) as session:
        session.execute(INSERT_SQL, payload)
        session.commit()


# ---------------------------------------------------------------------------
# Preview
# ---------------------------------------------------------------------------

def preview_visits(limit: int | None = None) -> None:
    if limit is None:
        limit = PREVIEW_SAMPLE_SIZE

    legacy_engine = get_legacy_engine()
    new_engine = get_new_engine()

    center_code_to_uuid = load_center_code_to_uuid(new_engine)
    person_id_to_patient_uuid = load_person_id_to_patient_uuid(new_engine)

    narsala_center_uuid = center_code_to_uuid.get("DS-TMC-009")
    coordinator_ids = load_coordinator_ids(new_engine, narsala_center_uuid)

    healthcase_doctor_lookup  = load_healthcase_doctor_lookup(legacy_engine)
    doctor_name_phone_to_uuid = load_doctor_name_phone_to_uuid(new_engine)

    with legacy_engine.connect() as conn:
        rows = conn.execute(
            _FETCH_HEALTHCASES_LIMITED_SQL,
            {"target_appdetails_id": TARGET_APPDETAILS_ID, "lim": limit},
        ).mappings().all()

        comment_rows = conn.execute(_FETCH_COMMENTS_SQL).fetchall()

    comment_lookup: dict[int, str] = {int(r[0]): r[1] for r in comment_rows}

    log.info(
        "════════ PREVIEW: visit migration appdetails_id=%d — %d healthcase(s) ════════",
        TARGET_APPDETAILS_ID,
        len(rows),
    )

    for i, row in enumerate(rows, start=1):
        legacy_hc_id = row.get("id")
        person_id    = str(row.get("person_id", ""))

        patient_uuid = person_id_to_patient_uuid.get(person_id)
        if patient_uuid is None:
            log.warning(
                "PREVIEW [%d/%d] healthcase_id=%s — WOULD SKIP: "
                "no patient found for person_id=%s",
                i, len(rows), legacy_hc_id, person_id,
            )
            continue

        center_uuid      = resolve_center_id(row.get("appdetails_id"), center_code_to_uuid)
        coordinator_uuid = pick_random_coordinator(coordinator_ids)
        doctor_uuid      = resolve_doctor_uuid(
            int(legacy_hc_id),
            healthcase_doctor_lookup,
            doctor_name_phone_to_uuid,
        )

        if doctor_uuid is None:
            log.warning(
                "PREVIEW [%d/%d] healthcase_id=%s — WOULD SKIP: "
                "doctor_id not mapped to any new DB user",
                i, len(rows), legacy_hc_id,
            )
            continue

        comment_text = comment_lookup.get(int(legacy_hc_id)) if legacy_hc_id else None
        vitals       = build_vitals_json(comment_text)

        record = build_visit_record(
            row,
            f"preview-{legacy_hc_id}",
            patient_uuid,
            center_uuid,
            coordinator_uuid,
            doctor_uuid,
            vitals,
            comment_text,
        )

        log.info(
            "PREVIEW [%d/%d] healthcase_id=%s person_id=%s\n%s",
            i, len(rows), legacy_hc_id, person_id,
            json.dumps(
                {k: v for k, v in record.items() if k != "history_notes"},
                indent=2, default=str,
            ),
        )

    log.info("════════ PREVIEW complete — no rows written to new DB ════════")


# ---------------------------------------------------------------------------
# Main migration
# ---------------------------------------------------------------------------

def migrate_visit(
    limit: int | None = None,
) -> None:
    """
    Migrate HealthCase_healthcase rows (appdetails_id=42) to visit table in new DB.
    """
    legacy_engine = get_legacy_engine()
    new_engine    = get_new_engine()

    center_code_to_uuid       = load_center_code_to_uuid(new_engine)
    person_id_to_patient_uuid = load_person_id_to_patient_uuid(new_engine)

    narsala_center_uuid = center_code_to_uuid.get("DS-TMC-009")
    coordinator_ids     = load_coordinator_ids(new_engine, narsala_center_uuid)

    healthcase_doctor_lookup  = load_healthcase_doctor_lookup(legacy_engine)
    doctor_name_phone_to_uuid = load_doctor_name_phone_to_uuid(new_engine)

    already_migrated: set[str] = get_migrated_legacy_ids(new_engine, "visit")
    log.info("Already migrated in new DB: %d visit rows", len(already_migrated))

    with legacy_engine.connect() as conn:
        if limit is not None:
            rows = conn.execute(
                _FETCH_HEALTHCASES_LIMITED_SQL,
                {"target_appdetails_id": TARGET_APPDETAILS_ID, "lim": limit},
            ).mappings().all()
        else:
            rows = conn.execute(
                _FETCH_HEALTHCASES_SQL,
                {"target_appdetails_id": TARGET_APPDETAILS_ID},
            ).mappings().all()

    total = len(rows)

    with legacy_engine.connect() as conn:
        legacy_total = conn.execute(
            text('SELECT COUNT(*) FROM "HealthCase_healthcase"')
        ).scalar()

    log.info(
        "══ SOURCE ══  HealthCase_healthcase total=%d | "
        "eligible (appdetails_id=%d)=%d | fetched=%d (limit=%s)",
        legacy_total,
        TARGET_APPDETAILS_ID,
        total,
        total,
        limit if limit is not None else "ALL",
    )

    with legacy_engine.connect() as conn:
        comment_rows = conn.execute(_FETCH_COMMENTS_SQL).fetchall()

    comment_lookup: dict[int, str] = {int(r[0]): r[1] for r in comment_rows}
    log.info("Loaded comments for %d healthcases", len(comment_lookup))

    id_gen = SafeIDGenerator(new_engine, table="visit")

    batch: list[dict] = []
    inserted = skipped_migrated = skipped_no_patient = skipped_no_doctor = errors = 0
    with_vitals = with_doctor = with_coordinator = with_completed_at = 0

    for row in rows:
        legacy_hc_id = str(row.get("id", ""))
        person_id    = str(row.get("person_id", ""))

        # 1. Already migrated in a previous run → skip.
        if legacy_hc_id in already_migrated:
            log.debug("SKIP (already migrated) healthcase_id=%s", legacy_hc_id)
            skipped_migrated += 1
            continue

        # 2. No matching patient in new DB → skip.
        patient_uuid = person_id_to_patient_uuid.get(person_id)
        if patient_uuid is None:
            log.warning(
                "SKIP healthcase_id=%s — no patient found for person_id=%s",
                legacy_hc_id, person_id,
            )
            skipped_no_patient += 1
            continue

        try:
            center_uuid      = resolve_center_id(row.get("appdetails_id"), center_code_to_uuid)
            coordinator_uuid = pick_random_coordinator(coordinator_ids)
            doctor_uuid      = resolve_doctor_uuid(
                int(legacy_hc_id),
                healthcase_doctor_lookup,
                doctor_name_phone_to_uuid,
            )

            # 3. No doctor mapped → skip.
            if doctor_uuid is None:
                log.warning(
                    "SKIP healthcase_id=%s — doctor_id not mapped to any new DB user",
                    legacy_hc_id,
                )
                skipped_no_doctor += 1
                continue

            comment_text = comment_lookup.get(int(legacy_hc_id)) if legacy_hc_id else None
            vitals       = build_vitals_json(comment_text)
            new_id       = id_gen.next()

            record = build_visit_record(
                row,
                new_id,
                patient_uuid,
                center_uuid,
                coordinator_uuid,
                doctor_uuid,
                vitals,
                comment_text,
            )

            if any(v is not None for k, v in vitals.items() if k != "sugar_mg_dl"):
                with_vitals += 1
            if doctor_uuid is not None:
                with_doctor += 1
            if coordinator_uuid is not None:
                with_coordinator += 1
            if record.get("completed_at") is not None:
                with_completed_at += 1

            batch.append(record)

            if len(batch) >= BATCH_SIZE:
                _flush_batch(new_engine, batch)
                inserted += len(batch)
                log.info("  … %d / %d rows committed", inserted, total)
                batch.clear()

        except Exception as e:
            log.error("ERROR processing healthcase_id=%s: %s", legacy_hc_id, e)
            errors += 1
            continue

    if batch:
        _flush_batch(new_engine, batch)
        inserted += len(batch)

    log.info(
        "═══ Visit migration complete (appdetails_id=%d) ═══  "
        "limit=%s | fetched=%d | inserted=%d | "
        "skipped(re-run)=%d | skipped(no-patient)=%d | skipped(no-doctor)=%d | errors=%d | "
        "with_vitals=%d | with_coordinator=%d | with_doctor=%d | with_completed_at=%d",
        TARGET_APPDETAILS_ID,
        limit if limit is not None else "ALL",
        total,
        inserted,
        skipped_migrated,
        skipped_no_patient,
        skipped_no_doctor,
        errors,
        with_vitals,
        with_coordinator,
        with_doctor,
        with_completed_at,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            f"Migrate visits (HealthCase) with appdetails_id={TARGET_APPDETAILS_ID} "
            "(DS-TMC-009 / Narsala) from legacy DB to new DB."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python migrate_visit_appid42.py              # migrate ALL eligible visits
  python migrate_visit_appid42.py --limit 10   # migrate first 10
  python migrate_visit_appid42.py --limit 100  # migrate first 100
  python migrate_visit_appid42.py --preview    # preview only, no DB writes
        """,
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Migrate only the first N eligible visits. Omit to migrate ALL.",
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Run preview only — no rows are written to the new DB.",
    )
    args = parser.parse_args()

    preview_sample = args.limit if args.limit is not None else PREVIEW_SAMPLE_SIZE
    preview_visits(limit=preview_sample)

    if args.preview:
        log.info("--preview flag set — exiting without migrating.")
        sys.exit(0)

    limit_label = str(args.limit) if args.limit is not None else "ALL"
    log.info(
        "Review the PREVIEW logs above. "
        "About to migrate %s visit(s) with appdetails_id=%d. "
        "Type 'yes' to proceed.",
        limit_label,
        TARGET_APPDETAILS_ID,
    )
    answer = input("Migrate to new DB? (yes/no): ").strip().lower()
    if answer != "yes":
        log.info("Migration cancelled — no data written to new DB.")
        sys.exit(0)

    migrate_visit(limit=args.limit)