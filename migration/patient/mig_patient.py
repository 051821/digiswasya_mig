import json
import sys
import os
import uuid
import argparse
from datetime import date, datetime, timezone
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from psycopg2.extras import Json
from sqlalchemy import text
from sqlalchemy.orm import Session

from config.db import get_legacy_engine, get_new_engine
from config.config import BATCH_SIZE, DRY_RUN, PREVIEW_SAMPLE_SIZE
from utils.id_gen import SafeIDGenerator, get_migrated_legacy_ids
from utils.logger import get_logger


log = get_logger("migrate_patient")

LEGACY_SOURCE_TABLE = "Person_persondetails"

# ---------------------------------------------------------------------------
# THIS SCRIPT IS SCOPED TO appdetails_id = 42 (DS-TMC-009 / Narsala) ONLY.
# Only patients whose latest HealthCase.appdetails_id = 42 will be migrated.
# ---------------------------------------------------------------------------

TARGET_APPDETAILS_ID: int = 42

APPID_TO_CENTER_CODE: dict[int, str] = {
    45: "DS-TMC-012",   # Bharatwada / Vijay Nagar
    42: "DS-TMC-009",   # Narsala
    41: "DS-TMC-015",   # Karanjawane
    43: "DS-TMC-010",   # Hasanbagh
    44: "DS-TMC-011",   # Chakole
    37: "DS-TMC-004",   # Itaunja
    67: "TMC-DSF0018",  # Khodala / Palghar
    40: "DS-TMC-007",   # IGR / Dharampeth
    24: "DS-TMC-013",   # Peth
}

UNRESOLVED_APP_IDS: frozenset[int] = frozenset({
    15, 16, 17, 18, 19, 20, 21, 31, 32, 36,
})


def load_center_code_to_uuid(new_engine: Any) -> dict[str, str]:
    with new_engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id::text, center_code FROM center WHERE center_code IS NOT NULL")
        ).fetchall()

    mapping: dict[str, str] = {
        row[1].strip(): row[0]
        for row in rows
        if row[1] and row[1].strip()
    }

    log.info(
        "Loaded %d center records from new DB: %s",
        len(mapping),
        sorted(mapping.keys()),
    )
    return mapping


def load_existing_patients(new_engine: Any) -> set[tuple]:
    with new_engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT full_name, phone_last10, date_of_birth
                FROM patient
                WHERE legacy_id IS NULL
                  AND deleted_at IS NULL
            """)
        ).fetchall()

    live_patients: set[tuple] = set()
    for full_name, phone, dob in rows:
        key = (
            (full_name or "").strip().lower(),
            (phone or "").strip(),
            str(dob) if dob else "",
        )
        live_patients.add(key)

    log.info(
        "Loaded %d live patients from new DB for duplicate check",
        len(live_patients),
    )
    return live_patients


def is_duplicate_in_new_db(row: Any, live_patients: set[tuple]) -> bool:
    name  = (row.get("name") or "").strip().lower()
    phone = normalize_phone(row.get("phone")) or ""
    dob   = str(_parse_birth_date(row.get("dob"))) if _parse_birth_date(row.get("dob")) else ""
    return (name, phone, dob) in live_patients


def build_legacy_survivor_ids(rows: list) -> set[str]:
    """
    Within the legacy rows themselves, detect duplicates by
    (name.lower(), phone_last10, age_years) and keep only the row
    with the highest personid (newest) per group.

    Returns a set of personid strings that should be migrated.
    Rows whose personid is NOT in this set are intra-legacy duplicates.
    """
    key_to_best: dict[tuple, int] = {}

    for row in rows:
        pid = row.get("personid")
        if pid is None:
            continue
        try:
            pid_int = int(pid)
        except (TypeError, ValueError):
            continue

        name  = (row.get("name") or "").strip().lower()
        phone = normalize_phone(row.get("phone")) or ""
        age   = str(calculate_age_years(row)) if calculate_age_years(row) is not None else ""

        key = (name, phone, age)

        # Keep the highest personid (newest row) for each key
        if key not in key_to_best or pid_int > key_to_best[key]:
            key_to_best[key] = pid_int

    survivor_ids = {str(v) for v in key_to_best.values()}

    total_rows = sum(1 for r in rows if r.get("personid") is not None)
    duplicates = total_rows - len(survivor_ids)
    log.info(
        "Intra-legacy dedup: %d total rows → %d survivors, %d duplicates will be skipped",
        total_rows,
        len(survivor_ids),
        duplicates,
    )
    return survivor_ids


def map_appid_to_center_uuid(
    appid: Any,
    center_code_to_uuid: dict[str, str],
) -> str | None:
    if appid is None:
        return None

    try:
        app_id = int(appid)
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
            "No center found for appdetails_id=%s center_code=%s — center_id will be NULL",
            app_id,
            center_code,
        )
        return None

    log.debug(
        "Center resolved: appdetails_id=%s center_code=%s center_id=%s",
        app_id,
        center_code,
        center_uuid,
    )
    return center_uuid


def resolve_center_id(
    personid: Any,
    appuid: Any,
    healthcase_appdetails_by_person_id: dict[int, int],
    center_code_to_uuid: dict[str, str],
) -> str | None:
    pid = _coerce_person_id(personid)

    if pid is not None:
        appdetails_id = healthcase_appdetails_by_person_id.get(pid)
        if appdetails_id is not None:
            center_code = APPID_TO_CENTER_CODE.get(appdetails_id)
            center_uuid = map_appid_to_center_uuid(appdetails_id, center_code_to_uuid)

            if center_uuid is not None:
                log.debug(
                    "Center audit: person_id=%s appdetails_id=%s "
                    "center_code=%s center_id=%s",
                    pid, appdetails_id, center_code, center_uuid,
                )
            elif appdetails_id not in UNRESOLVED_APP_IDS:
                log.debug(
                    "Center audit: person_id=%s appdetails_id=%s "
                    "center_code=%s → no UUID resolved",
                    pid, appdetails_id, center_code,
                )

            return center_uuid

    return map_appid_to_center_uuid(appuid, center_code_to_uuid)


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------

_HEALTH_COMMENT_SQL = text("""
    SELECT DISTINCT ON (hc.person_id)
        hc.person_id,
        hc.id AS healthcase_id,
        c.commenttext
    FROM "HealthCase_healthcase" hc
    INNER JOIN "HealthCase_commentshealthcase" c
        ON c.healthcase_id = hc.id
    WHERE hc.person_id IS NOT NULL
      AND c.commenttext IS NOT NULL
      AND TRIM(c.commenttext) <> ''
    ORDER BY
        hc.person_id,
        COALESCE(hc.lastupdated, hc.created_at) DESC NULLS LAST,
        hc.id DESC,
        c.commentid DESC NULLS LAST
""")

_CENTER_APPDETAILS_SQL = text("""
    SELECT DISTINCT ON (person_id)
        person_id,
        appdetails_id
    FROM "HealthCase_healthcase"
    WHERE appdetails_id IS NOT NULL
      AND person_id IS NOT NULL
    ORDER BY
        person_id,
        COALESCE(lastupdated, created_at) DESC NULLS LAST,
        id DESC
""")

_APPID42_PERSON_IDS_SQL = text("""
    SELECT DISTINCT ON (person_id)
        person_id
    FROM "HealthCase_healthcase"
    WHERE appdetails_id = :target_appdetails_id
      AND person_id IS NOT NULL
    ORDER BY
        person_id,
        COALESCE(lastupdated, created_at) DESC NULLS LAST,
        id DESC
""")


# ---------------------------------------------------------------------------
# Field helpers
# ---------------------------------------------------------------------------

def _coerce_person_id(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        log.warning("Invalid person_id value %r — cannot map lookups", value)
        return None


SEX_MAP: dict[str, str] = {
    "m": "MALE",
    "male": "MALE",
    "1": "MALE",
    "f": "FEMALE",
    "female": "FEMALE",
    "0": "FEMALE",
    "other": "OTHER",
    "o": "OTHER",
}


def map_gender(raw: Any) -> str | None:
    if raw is None:
        return None
    mapped = SEX_MAP.get(str(raw).strip().lower())
    if mapped is None:
        log.warning("Unknown gender value %r — sex will be NULL", raw)
    return mapped


def map_mother_tongue(raw: Any) -> int | None:
    if raw is None:
        return None
    return raw


def _parse_birth_date(dob: Any) -> date | None:
    if dob is None:
        return None
    if isinstance(dob, datetime):
        return dob.date()
    if isinstance(dob, date):
        return dob
    try:
        return datetime.strptime(str(dob)[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def calculate_age_years(row: Any) -> int | None:
    birth = _parse_birth_date(row.get("dob"))
    if birth is not None:
        today = datetime.now(timezone.utc).date()
        age = today.year - birth.year - (
            (today.month, today.day) < (birth.month, birth.day)
        )
        return max(age, 0)

    dob_year = row.get("dob_year")
    if dob_year is not None:
        try:
            return datetime.now(timezone.utc).year - int(dob_year)
        except (TypeError, ValueError):
            log.warning("Invalid dob_year %r for personid=%s", dob_year, row.get("personid"))
    return None


def normalize_phone(phone: Any) -> str | None:
    if phone is None or str(phone).strip() == "":
        return None

    digits = "".join(c for c in str(phone) if c.isdigit())
    if not digits:
        return None

    if len(digits) < 10:
        log.warning("Phone %r has fewer than 10 digits (%d) — storing as-is", phone, len(digits))
        return digits

    if len(digits) > 10:
        digits = digits[-10:]

    return digits


def concat_address(addr1: Any, addr2: Any) -> str | None:
    parts = [p.strip() for p in [addr1 or "", addr2 or ""] if p and p.strip()]
    return ", ".join(parts) if parts else None


# ---------------------------------------------------------------------------
# Health history
# ---------------------------------------------------------------------------

_CONDITION_MAP: dict[str, list[str]] = {
    "Do you consume alcohol?": ["alcohol"],
    "Do you have asthma?": ["asthma"],
    "Do you have hypertension?": ["hypertension"],
    "Do you have diabetes?": ["diabetes"],
    "Do you have heart diease?": ["heart_disease"],
    "Do you have high cholestrol?": ["high_cholesterol"],
    "Do you have any drug allergies?": ["drug_allergies"],
    "Any history of past surgery or hospitalization?": ["surgeries", "hospitalisations"],
    "Do you smoke?": ["smoking"],
}

_QTY_KEYS = frozenset({"alcohol", "smoking"})


def _empty_health_history() -> dict:
    conditions = [
        "asthma", "alcohol", "smoking", "thyroid", "diabetes",
        "surgeries", "hypertension", "tuberculosis", "heart_disease",
        "drug_allergies", "high_cholesterol", "hospitalisations",
    ]
    result: dict[str, Any] = {}
    for key in conditions:
        if key in _QTY_KEYS:
            result[key] = {"qty": "", "present": False}
        else:
            result[key] = {"notes": "", "present": False}
    result["other_notes"] = ""
    return result


def _parse_yes_no(raw_val: Any) -> bool:
    if raw_val is None:
        return False
    if isinstance(raw_val, bool):
        return raw_val
    normalized = str(raw_val).strip().lower()
    if normalized in {"yes", "y", "true", "1"}:
        return True
    if normalized in {"no", "n", "false", "0", ""}:
        return False
    log.warning("Unexpected health answer %r — treating as False", raw_val)
    return False


def build_health_history_json(commenttext: str | None) -> dict | None:
    if not commenttext or not str(commenttext).strip():
        return None

    try:
        raw: dict = json.loads(commenttext)
    except (json.JSONDecodeError, TypeError):
        log.warning("Could not parse commenttext as JSON: %s", str(commenttext)[:120])
        return None

    if not isinstance(raw, dict):
        log.warning("commenttext JSON is not an object: %r", type(raw))
        return None

    result = _empty_health_history()

    for legacy_key, new_keys in _CONDITION_MAP.items():
        raw_val = raw.get(legacy_key)
        if raw_val is None:
            continue
        present = _parse_yes_no(raw_val)
        for new_key in new_keys:
            result[new_key]["present"] = present

    return result


# ---------------------------------------------------------------------------
# govt_id / validate
# ---------------------------------------------------------------------------

def _birth_year_from_row(row: Any) -> str:
    birth = _parse_birth_date(row.get("dob"))
    if birth is not None:
        return str(birth.year)
    dob_year = row.get("dob_year")
    if dob_year is not None:
        try:
            return str(int(dob_year))
        except (TypeError, ValueError):
            pass
    return ""


def generate_govt_id(row: Any) -> str:
    full_name = (row.get("name") or row.get("fullname") or "").strip()
    year = _birth_year_from_row(row)
    parts = full_name.split()

    if len(parts) >= 2:
        first, last = parts[0], parts[-1]
        if len(first) < 3:
            return f"{first.upper()}{last[:3].upper()}{year}"
        if len(last) >= 3:
            return f"{first[:3].upper()}{last[:3].upper()}{year}"
        return f"{first[:3].upper()}{last.upper()}{year}"

    if len(parts) == 1:
        return f"{parts[0].upper()}{year}"

    return f"MIGRATED-{row.get('personid', 'UNKNOWN')}{year}"


def validate_row(raw: Any) -> list[str]:
    errors = []
    if not raw.get("name"):
        errors.append("full_name (name) is NULL or empty — NOT NULL in new schema")
    if raw.get("datetime_joined") is None:
        errors.append("created_at (datetime_joined) is NULL — NOT NULL in new schema")
    return errors


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def build_patient_record(
    row: Any,
    new_id: str,
    health_lookup: dict[int, str] | None = None,
    healthcase_appdetails_by_person_id: dict[int, int] | None = None,
    center_code_to_uuid: dict[str, str] | None = None,
) -> dict:
    if health_lookup is None:
        health_lookup = {}
    if healthcase_appdetails_by_person_id is None:
        healthcase_appdetails_by_person_id = {}
    if center_code_to_uuid is None:
        center_code_to_uuid = {}

    pid = _coerce_person_id(row.get("personid"))
    commenttext = health_lookup.get(pid) if pid is not None else None
    health_json = build_health_history_json(commenttext)

    center_id = resolve_center_id(
        pid,
        row.get("appuid"),
        healthcase_appdetails_by_person_id,
        center_code_to_uuid,
    )

    phone = normalize_phone(row.get("phone"))

    return {
        "id": new_id,
        "full_name": row.get("name"),
        "phone": phone,
        "sex": map_gender(row.get("gender")),
        "date_of_birth": row.get("dob"),
        "age_years": calculate_age_years(row),
        "age_months": None,
        "address_text": concat_address(row.get("address1"), row.get("address2")),
        "address_line_1": row.get("address1"),
        "address_line_2": row.get("address2"),
        "district": None,
        "state": None,
        "village_town_city": None,
        "postal_code": None,
        "country": None,
        "address_resolution_method": None,
        "pincode_master_id": None,
        "occupation": None,
        "category": None,
        "mother_tongue": map_mother_tongue(row.get("preferredlanguage")),
        "government_id": generate_govt_id(row),
        "health_history_json": health_json,
        "long_term_conditions": None,
        "consent_status": None,
        "patient_status": "CURRENT",
        "is_demo": False,
        "center_id": center_id,
        "family_id": None,
        "merged_into_patient_id": None,
        "created_at": row["datetime_joined"],
        "deleted_at": None,
        "migrated_at": datetime.now(timezone.utc),
        "tenant_id": None,
        "phone_last10": phone,
        "legacy_id": str(row["personid"]),
        "legacy_source": "dgiswasthya_database",
        "legacy_extra_1": row.get("personuid"),
        "legacy_extra_2": row.get("appuid"),
        "legacy_extra_3": row.get("hierarchy"),
    }


def _prepare_row_for_insert(record: dict) -> dict:
    row = record.copy()
    health_json = row.get("health_history_json")
    if health_json is not None:
        row["health_history_json"] = Json(health_json)
    return row


# ---------------------------------------------------------------------------
# INSERT SQL
# ---------------------------------------------------------------------------

INSERT_SQL = text("""
    INSERT INTO patient (
        id,
        full_name,
        phone,
        sex,
        date_of_birth,
        age_years,
        age_months,
        address_text,
        address_line_1,
        address_line_2,
        district,
        state,
        village_town_city,
        postal_code,
        country,
        address_resolution_method,
        pincode_master_id,
        occupation,
        category,
        mother_tongue,
        government_id,
        health_history_json,
        long_term_conditions,
        consent_status,
        patient_status,
        is_demo,
        center_id,
        family_id,
        merged_into_patient_id,
        created_at,
        deleted_at,
        migrated_at,
        tenant_id,
        phone_last10,
        legacy_id,
        legacy_source,
        legacy_extra_1,
        legacy_extra_2,
        legacy_extra_3
    ) VALUES (
        :id,
        :full_name,
        :phone,
        :sex,
        :date_of_birth,
        :age_years,
        :age_months,
        :address_text,
        :address_line_1,
        :address_line_2,
        :district,
        :state,
        :village_town_city,
        :postal_code,
        :country,
        :address_resolution_method,
        :pincode_master_id,
        :occupation,
        :category,
        :mother_tongue,
        :government_id,
        :health_history_json,
        :long_term_conditions,
        :consent_status,
        :patient_status,
        :is_demo,
        :center_id,
        :family_id,
        :merged_into_patient_id,
        :created_at,
        :deleted_at,
        :migrated_at,
        :tenant_id,
        :phone_last10,
        :legacy_id,
        :legacy_source,
        :legacy_extra_1,
        :legacy_extra_2,
        :legacy_extra_3
    )
    ON CONFLICT (id) DO NOTHING
""")


# ---------------------------------------------------------------------------
# Lookup loaders
# ---------------------------------------------------------------------------

def _load_appid42_person_ids(legacy_engine: Any) -> set[int]:
    """
    Return the set of person_ids whose most recent HealthCase
    has appdetails_id = TARGET_APPDETAILS_ID (42).
    """
    with legacy_engine.connect() as conn:
        rows = conn.execute(
            _APPID42_PERSON_IDS_SQL,
            {"target_appdetails_id": TARGET_APPDETAILS_ID},
        ).fetchall()

    person_ids = {int(r[0]) for r in rows}
    log.info(
        "Found %d person_id(s) with latest HealthCase.appdetails_id = %d",
        len(person_ids),
        TARGET_APPDETAILS_ID,
    )
    return person_ids


def _load_migration_lookups(
    legacy_engine: Any,
) -> tuple[dict[int, str], dict[int, int], dict[int, int]]:
    with legacy_engine.connect() as conn:
        health_rows = conn.execute(_HEALTH_COMMENT_SQL).fetchall()

    health_lookup: dict[int, str] = {}
    healthcase_id_by_person: dict[int, int] = {}
    for person_id, healthcase_id, commenttext in health_rows:
        pid = int(person_id)
        health_lookup[pid] = commenttext
        healthcase_id_by_person[pid] = int(healthcase_id)

    log.info(
        "Loaded health history for %d patients (via HealthCase join)",
        len(health_lookup),
    )

    with legacy_engine.connect() as conn:
        center_rows = conn.execute(_CENTER_APPDETAILS_SQL).fetchall()

    healthcase_appdetails_by_person_id: dict[int, int] = {
        int(r[0]): int(r[1]) for r in center_rows
    }
    log.info(
        "Loaded center mapping (person_id → appdetails_id) for %d patients",
        len(healthcase_appdetails_by_person_id),
    )

    return health_lookup, healthcase_appdetails_by_person_id, healthcase_id_by_person


# ---------------------------------------------------------------------------
# Preview helpers
# ---------------------------------------------------------------------------

def _record_for_preview_log(
    record: dict,
    healthcase_appdetails_id: int | None = None,
    healthcase_id: int | None = None,
    comment_snippet: str | None = None,
) -> dict:
    return {
        "legacy_id": record.get("legacy_id"),
        "id": record.get("id"),
        "full_name": record.get("full_name"),
        "phone": record.get("phone"),
        "phone_last10": record.get("phone_last10"),
        "sex": record.get("sex"),
        "date_of_birth": record.get("date_of_birth"),
        "age_years": record.get("age_years"),
        "government_id": record.get("government_id"),
        "healthcase_id": healthcase_id,
        "healthcase_appdetails_id": healthcase_appdetails_id,
        "center_id": record.get("center_id"),
        "health_history_json": record.get("health_history_json"),
        "comment_snippet": (comment_snippet or "")[:200],
        "address_text": record.get("address_text"),
        "created_at": record.get("created_at"),
        "legacy_extra_2_appuid": record.get("legacy_extra_2"),
    }


def preview_patients(limit: int | None = None) -> None:
    if limit is None:
        limit = PREVIEW_SAMPLE_SIZE

    legacy_engine = get_legacy_engine()
    new_engine = get_new_engine()

    center_code_to_uuid = load_center_code_to_uuid(new_engine)
    live_patients = load_existing_patients(new_engine)
    health_lookup, healthcase_appdetails_by_person_id, healthcase_id_by_person = (
        _load_migration_lookups(legacy_engine)
    )

    appid42_person_ids = _load_appid42_person_ids(legacy_engine)

    if not appid42_person_ids:
        log.warning("No person_ids found for appdetails_id=%d — nothing to preview.", TARGET_APPDETAILS_ID)
        return

    with legacy_engine.connect() as conn:
        rows = conn.execute(
            text(
                'SELECT * FROM "Person_persondetails" '
                "WHERE personid = ANY(:pids) "
                "ORDER BY personid ASC LIMIT :limit"
            ),
            {"pids": list(appid42_person_ids), "limit": limit},
        ).mappings().all()

    # Build survivor set for intra-legacy dedup preview
    survivor_ids = build_legacy_survivor_ids(rows)

    log.info(
        "════════ PREVIEW: appdetails_id=%d — %d patient(s) to inspect ════════",
        TARGET_APPDETAILS_ID,
        len(rows),
    )

    for i, row in enumerate(rows, start=1):
        legacy_pid = row.get("personid")

        # Intra-legacy duplicate check
        if str(legacy_pid) not in survivor_ids:
            log.debug(
                "PREVIEW [%d/%d] personid=%s — WOULD SKIP (intra-legacy duplicate): "
                "name=%r phone=%r age=%s",
                i, len(rows), legacy_pid,
                row.get("name"), row.get("phone"), calculate_age_years(row),
            )
            continue

        if is_duplicate_in_new_db(row, live_patients):
            log.debug(
                "PREVIEW [%d/%d] personid=%s — WOULD SKIP (live duplicate): "
                "name=%r phone=%r dob=%r",
                i, len(rows), legacy_pid,
                row.get("name"), row.get("phone"), row.get("dob"),
            )
            continue

        validation_errors = validate_row(row)
        preview_id = f"preview-{uuid.uuid4()}"

        if validation_errors:
            log.warning(
                "PREVIEW [%d/%d] personid=%s — WOULD SKIP (validation): %s",
                i, len(rows), legacy_pid, " | ".join(validation_errors),
            )
            continue

        pid = _coerce_person_id(legacy_pid)
        appdetails_id = (
            healthcase_appdetails_by_person_id.get(pid) if pid is not None else None
        )
        healthcase_id = healthcase_id_by_person.get(pid) if pid is not None else None
        commenttext = health_lookup.get(pid) if pid is not None else None

        record = build_patient_record(
            row,
            preview_id,
            health_lookup,
            healthcase_appdetails_by_person_id,
            center_code_to_uuid,
        )
        payload = _record_for_preview_log(
            record,
            appdetails_id,
            healthcase_id,
            commenttext,
        )
        log.info(
            "PREVIEW [%d/%d] personid=%s\n%s",
            i,
            len(rows),
            legacy_pid,
            json.dumps(payload, indent=2, default=str),
        )

    log.info("════════ PREVIEW complete — no rows written to new DB ════════")


# ---------------------------------------------------------------------------
# Batch flush
# ---------------------------------------------------------------------------

def _flush_batch(new_engine: Any, batch: list[dict]) -> None:
    if DRY_RUN:
        log.info("[DRY RUN] Would insert %d patient rows.", len(batch))
        return

    payload = [_prepare_row_for_insert(r) for r in batch]
    with Session(new_engine) as session:
        session.execute(INSERT_SQL, payload)
        session.commit()


# ---------------------------------------------------------------------------
# Main migration — scoped to appdetails_id = 42
# ---------------------------------------------------------------------------

def migrate_patient(
    id_map: dict[int, str],
    limit: int | None = None,
) -> None:
    """
    Migrate only patients whose latest HealthCase.appdetails_id = 42
    (DS-TMC-009 / Narsala) from the legacy DB to the new DB.

    Args:
        id_map: populated in-place with { legacy_personid → new UUID }.
        limit:  cap on number of rows to process (None = all eligible rows).
    """
    legacy_engine = get_legacy_engine()
    new_engine = get_new_engine()

    center_code_to_uuid = load_center_code_to_uuid(new_engine)
    live_patients = load_existing_patients(new_engine)

    id_gen = SafeIDGenerator(new_engine, table="patient")
    already_migrated: set[str] = get_migrated_legacy_ids(new_engine, "patient")
    log.info("Already migrated in new DB: %d patient rows", len(already_migrated))

    # Step 1: resolve which person_ids qualify (appdetails_id = 42).
    appid42_person_ids = _load_appid42_person_ids(legacy_engine)

    if not appid42_person_ids:
        log.warning(
            "No person_ids found for appdetails_id=%d — nothing to migrate.",
            TARGET_APPDETAILS_ID,
        )
        return

    # Step 2: fetch only those Person rows from legacy DB.
    if limit is not None:
        fetch_sql = text(
            'SELECT * FROM "Person_persondetails" '
            "WHERE personid = ANY(:pids) "
            "ORDER BY personid ASC "
            "LIMIT :lim"
        )
        with legacy_engine.connect() as conn:
            rows = conn.execute(
                fetch_sql,
                {"pids": list(appid42_person_ids), "lim": limit},
            ).mappings().all()
    else:
        fetch_sql = text(
            'SELECT * FROM "Person_persondetails" '
            "WHERE personid = ANY(:pids) "
            "ORDER BY personid ASC"
        )
        with legacy_engine.connect() as conn:
            rows = conn.execute(
                fetch_sql,
                {"pids": list(appid42_person_ids)},
            ).mappings().all()

    total = len(rows)

    with legacy_engine.connect() as conn:
        legacy_total = conn.execute(
            text('SELECT COUNT(*) FROM "Person_persondetails"')
        ).scalar()

    log.info(
        "══ SOURCE ══  Person_persondetails total=%d | "
        "eligible (appdetails_id=%d)=%d | fetched=%d (limit=%s)",
        legacy_total,
        TARGET_APPDETAILS_ID,
        len(appid42_person_ids),
        total,
        limit if limit is not None else "ALL",
    )

    health_lookup, healthcase_appdetails_by_person_id, _ = _load_migration_lookups(
        legacy_engine
    )

    # Step 3: build intra-legacy survivor set (newest personid wins per duplicate group).
    survivor_ids = build_legacy_survivor_ids(rows)

    batch: list[dict] = []
    inserted = skipped = deduped = legacy_deduped = errors = 0
    with_health = with_center = 0

    for row in rows:
        legacy_pid = str(row.get("personid", ""))

        # 1. Already migrated in a previous run → skip.
        if legacy_pid in already_migrated:
            log.debug("SKIP (already migrated) personid=%s", legacy_pid)
            skipped += 1
            id_map[int(legacy_pid)] = "__already_migrated__"
            continue

        # 2. Intra-legacy duplicate → skip, only the newest personid per group migrates.
        if legacy_pid not in survivor_ids:
            log.debug(
                "SKIP (intra-legacy duplicate) personid=%s name=%r phone=%r age=%s",
                legacy_pid,
                row.get("name"),
                row.get("phone"),
                calculate_age_years(row),
            )
            legacy_deduped += 1
            continue

        # 3. Duplicate of a live patient in new DB → skip.
        if is_duplicate_in_new_db(row, live_patients):
            log.debug(
                "SKIP (live duplicate) personid=%s name=%r phone=%r dob=%r",
                legacy_pid,
                row.get("name"),
                row.get("phone"),
                row.get("dob"),
            )
            deduped += 1
            continue

        # 4. Validation — must-have fields.
        validation_errors = validate_row(row)
        if validation_errors:
            log.warning(
                "SKIP personid=%s — validation failed: %s",
                legacy_pid, " | ".join(validation_errors),
            )
            errors += 1
            continue

        new_id = id_gen.next()
        id_map[int(legacy_pid)] = new_id

        record = build_patient_record(
            row,
            new_id,
            health_lookup,
            healthcase_appdetails_by_person_id,
            center_code_to_uuid,
        )

        if record.get("health_history_json") is not None:
            with_health += 1
        if record.get("center_id") is not None:
            with_center += 1

        batch.append(record)

        if len(batch) >= BATCH_SIZE:
            _flush_batch(new_engine, batch)
            inserted += len(batch)
            log.info("  … %d / %d rows committed", inserted, total)
            batch.clear()

    if batch:
        _flush_batch(new_engine, batch)
        inserted += len(batch)

    # Back-fill UUIDs for rows already migrated in a prior run.
    if any(v == "__already_migrated__" for v in id_map.values()):
        log.info("Back-filling UUIDs for already-migrated rows …")
        with new_engine.connect() as conn:
            existing = conn.execute(
                text("SELECT legacy_id, id::text FROM patient WHERE legacy_id IS NOT NULL")
            ).fetchall()
        for legacy_id_str, new_uuid in existing:
            try:
                id_map[int(legacy_id_str)] = new_uuid
            except (ValueError, TypeError):
                pass

    log.info(
        "═══ Patient migration complete (appdetails_id=%d) ═══  "
        "limit=%s | eligible=%d | fetched=%d | inserted=%d | "
        "skipped(re-run)=%d | skipped(legacy-dup)=%d | skipped(live-dup)=%d | errors=%d | "
        "with_health_history=%d | with_center_id=%d",
        TARGET_APPDETAILS_ID,
        limit if limit is not None else "ALL",
        len(appid42_person_ids),
        total,
        inserted,
        skipped,
        legacy_deduped,
        deduped,
        errors,
        with_health,
        with_center,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            f"Migrate patients with appdetails_id={TARGET_APPDETAILS_ID} "
            "(DS-TMC-009 / Narsala) from legacy DB to new DB."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python migrate_patient_appid42.py              # migrate ALL eligible patients
  python migrate_patient_appid42.py --limit 10   # migrate first 10
  python migrate_patient_appid42.py --limit 100  # migrate first 100
  python migrate_patient_appid42.py --preview    # preview only, no DB writes
        """,
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Migrate only the first N eligible patients. Omit to migrate ALL.",
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Run preview only — no rows are written to the new DB.",
    )
    args = parser.parse_args()

    preview_sample = args.limit if args.limit is not None else PREVIEW_SAMPLE_SIZE
    preview_patients(limit=preview_sample)

    if args.preview:
        log.info("--preview flag set — exiting without migrating.")
        sys.exit(0)

    limit_label = str(args.limit) if args.limit is not None else "ALL"
    log.info(
        "Review the PREVIEW logs above. "
        "About to migrate %s patient(s) with appdetails_id=%d. "
        "Type 'yes' to proceed.",
        limit_label,
        TARGET_APPDETAILS_ID,
    )
    answer = input("Migrate to new DB? (yes/no): ").strip().lower()
    if answer != "yes":
        log.info("Migration cancelled — no data written to new DB.")
        sys.exit(0)

    id_map: dict[int, str] = {}
    migrate_patient(id_map, limit=args.limit)

    output_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "patient_id_map_appid42.json"
    )
    with open(output_path, "w") as f:
        json.dump({str(k): v for k, v in id_map.items()}, f, indent=2)
    log.info("patient_id_map_appid42.json written to %s", os.path.abspath(output_path))