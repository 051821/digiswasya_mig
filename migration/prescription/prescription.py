# import json
# import sys
# import os
# import argparse
# from datetime import datetime, timezone
# from typing import Any

# sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# from sqlalchemy import text
# from sqlalchemy.orm import Session

# from config.db import get_legacy_engine, get_new_engine
# from config.config import BATCH_SIZE, DRY_RUN, PREVIEW_SAMPLE_SIZE
# from utils.id_gen import SafeIDGenerator, get_migrated_legacy_ids
# from utils.logger import get_logger


# log = get_logger("migrate_prescription")

# LEGACY_SOURCE_TABLE = "BodyVitals_prescription"
# LEGACY_SOURCE       = "digiswasthya_database"

# TARGET_APPDETAILS_ID: int = 42

# # ---------------------------------------------------------------------------
# # NOTE: Prescriptions for Narsala (appdetails_id=42) healthcases have
# # appdetails_id=23 in BodyVitals_prescription. So we filter by joining
# # to HealthCase_healthcase and checking hc.appdetails_id = 42 instead
# # of filtering directly on p.appdetails_id.
# # ---------------------------------------------------------------------------

# # ---------------------------------------------------------------------------
# # SQL — fetch prescriptions whose healthcase belongs to appdetails_id = 42
# # ---------------------------------------------------------------------------

# _FETCH_PRESCRIPTIONS_SQL = text("""
#     SELECT
#         p.id,
#         p.prescriptionid,
#         p.prescription,
#         p.investigation,
#         p.clinicalnote,
#         p.diagnosis,
#         p.recommendation,
#         p.datetime,
#         p.followupdatetime,
#         p.appdetails_id,
#         p.doctor_id,
#         p.person_id,
#         p.healthcase_id
#     FROM "BodyVitals_prescription" p
#     WHERE p.healthcase_id IS NOT NULL
#       AND p.person_id IS NOT NULL
#       AND p.healthcase_id IN (
#           SELECT id FROM "HealthCase_healthcase"
#           WHERE appdetails_id = :target_appdetails_id
#       )
#     ORDER BY p.id ASC
# """)

# _FETCH_PRESCRIPTIONS_LIMITED_SQL = text("""
#     SELECT
#         p.id,
#         p.prescriptionid,
#         p.prescription,
#         p.investigation,
#         p.clinicalnote,
#         p.diagnosis,
#         p.recommendation,
#         p.datetime,
#         p.followupdatetime,
#         p.appdetails_id,
#         p.doctor_id,
#         p.person_id,
#         p.healthcase_id
#     FROM "BodyVitals_prescription" p
#     WHERE p.healthcase_id IS NOT NULL
#       AND p.person_id IS NOT NULL
#       AND p.healthcase_id IN (
#           SELECT id FROM "HealthCase_healthcase"
#           WHERE appdetails_id = :target_appdetails_id
#       )
#     ORDER BY p.id ASC
#     LIMIT :lim
# """)

# # ---------------------------------------------------------------------------
# # SQL — fetch doctor info from legacy for doctor resolution
# # ---------------------------------------------------------------------------

# _FETCH_DOCTOR_DETAILS_SQL = text("""
#     SELECT
#         d.doctorid,
#         d.name   AS doctor_name,
#         d.phone  AS doctor_phone
#     FROM "Doctor_doctordetails" d
#     WHERE d.doctorid IS NOT NULL
# """)


# # ---------------------------------------------------------------------------
# # Loaders — new DB
# # ---------------------------------------------------------------------------

# def load_healthcase_id_to_visit_uuid(new_engine: Any) -> dict[str, str]:
#     """
#     Load { legacy_healthcase_id → visit.id (UUID) } from new DB.
#     visit.legacy_id = old HealthCase_healthcase.id (set during visit migration).
#     Used to resolve prescription.visit_id.
#     """
#     with new_engine.connect() as conn:
#         rows = conn.execute(
#             text("SELECT legacy_id, id::text FROM visit WHERE legacy_id IS NOT NULL")
#         ).fetchall()

#     mapping = {row[0]: row[1] for row in rows}
#     log.info("Loaded %d visit legacy_id → UUID mappings from new DB", len(mapping))
#     return mapping


# # ---------------------------------------------------------------------------
# # Doctor resolver
# # Same logic as visit migration — match by name + phone against new DB users.
# # ---------------------------------------------------------------------------

# def _normalize_phone(phone: str) -> str:
#     if not phone:
#         return ""
#     digits = "".join(c for c in phone if c.isdigit())
#     if len(digits) > 10:
#         digits = digits[-10:]
#     return digits


# def load_legacy_doctor_lookup(legacy_engine: Any) -> dict[int, tuple[str, str]]:
#     """
#     Load { doctor_id → (name, phone) } from legacy Doctor_doctordetails.
#     """
#     with legacy_engine.connect() as conn:
#         rows = conn.execute(_FETCH_DOCTOR_DETAILS_SQL).fetchall()

#     lookup: dict[int, tuple[str, str]] = {}
#     for doctorid, doctor_name, doctor_phone in rows:
#         if doctorid is not None:
#             lookup[int(doctorid)] = (
#                 (doctor_name or "").strip(),
#                 (doctor_phone or "").strip(),
#             )

#     log.info("Loaded %d doctor records from legacy Doctor_doctordetails", len(lookup))
#     return lookup


# def load_doctor_name_phone_to_uuid(new_engine: Any) -> tuple[dict[tuple[str, str], str], dict[str, str]]:
#     """
#     Load two lookups for active DOCTOR role users in new DB:
#       1. { (normalized_name_lower, phone_last10) → user.id }  — primary (name + phone)
#       2. { phone_last10 → user.id }                           — fallback (phone only)
#     The phone-only fallback handles name mismatches such as "Dr." prefix,
#     swapped first/last name, or spelling differences.
#     """
#     with new_engine.connect() as conn:
#         rows = conn.execute(
#             text("""
#                 SELECT
#                     id::text,
#                     LOWER(TRIM(COALESCE(first_name, '') || ' ' || COALESCE(last_name, ''))) AS full_name,
#                     phone
#                 FROM "user"
#                 WHERE role = 'DOCTOR'
#                   AND is_active = TRUE
#             """)
#         ).fetchall()

#     name_phone_lookup: dict[tuple[str, str], str] = {}
#     phone_only_lookup: dict[str, str] = {}
#     for user_id, full_name, phone in rows:
#         normalized_name  = (full_name or "").strip().lower()
#         normalized_phone = _normalize_phone(phone or "")
#         if normalized_name or normalized_phone:
#             name_phone_lookup[(normalized_name, normalized_phone)] = user_id
#         if normalized_phone:
#             phone_only_lookup[normalized_phone] = user_id

#     log.info("Loaded %d active doctor(s) from new DB for name+phone matching", len(name_phone_lookup))
#     return name_phone_lookup, phone_only_lookup


# def resolve_doctor_uuid(
#     doctor_id: Any,
#     legacy_doctor_lookup: dict[int, tuple[str, str]],
#     doctor_name_phone_to_uuid: tuple[dict[tuple[str, str], str], dict[str, str]],
# ) -> str | None:
#     """
#     Resolve prescription.doctor_user_id:
#       Step 1: Get name + phone from legacy Doctor_doctordetails via doctor_id.
#       Step 2: Normalize name (lowercase) and phone (last 10 digits).
#       Step 3: Match against new DB user (role=DOCTOR) by name+phone (primary),
#               then phone only (fallback for name mismatches).
#       Step 4: Return user UUID or None if not found.
#     """
#     if doctor_id is None:
#         return None
#     try:
#         did = int(doctor_id)
#     except (TypeError, ValueError):
#         return None

#     doctor_info = legacy_doctor_lookup.get(did)
#     if doctor_info is None:
#         log.debug("No doctor found in legacy for doctor_id=%s", doctor_id)
#         return None

#     doctor_name, doctor_phone = doctor_info
#     normalized_name  = doctor_name.lower()
#     normalized_phone = _normalize_phone(doctor_phone)

#     name_phone_lookup, phone_only_lookup = doctor_name_phone_to_uuid

#     # Primary: name + phone
#     user_uuid = name_phone_lookup.get((normalized_name, normalized_phone))

#     # Fallback: phone only (handles "Dr." prefix, swapped names, etc.)
#     if user_uuid is None and normalized_phone:
#         user_uuid = phone_only_lookup.get(normalized_phone)

#     if user_uuid is None:
#         log.debug(
#             "No doctor match in new DB for doctor_id=%s name=%r phone=%r",
#             doctor_id, doctor_name, doctor_phone,
#         )
#     return user_uuid


# # ---------------------------------------------------------------------------
# # Field helpers
# # ---------------------------------------------------------------------------

# def build_assessment(clinicalnote: Any) -> str:
#     """
#     Map legacy clinicalnote → assessment.
#     assessment is NOT NULL in new DB — default to empty string if NULL.
#     """
#     return (clinicalnote or "").strip() or ""


# def build_investigation_notes(investigation: Any) -> str | None:
#     """
#     Map legacy investigation JSON string → investigation_notes text.
#     Legacy format: [{"investigationcat":"Blood Test","investigationname":"Cbc rbs ecg ..."}]
#     Extracts only the investigationname values, joined by newline.
#     Example output: "Cbc rbs ecg kft lipid profile hba1c"
#     """
#     if not investigation or not str(investigation).strip():
#         return None
#     try:
#         parsed = json.loads(str(investigation).strip())
#         if not isinstance(parsed, list):
#             return str(investigation).strip()
#         names = [
#             item["investigationname"].strip()
#             for item in parsed
#             if isinstance(item, dict) and item.get("investigationname", "").strip()
#         ]
#         return "\n".join(names) if names else None
#     except (json.JSONDecodeError, TypeError):
#         log.warning("Could not parse investigation JSON: %s", str(investigation)[:120])
#         return str(investigation).strip()


# def build_patient_instructions(prescription: Any) -> str | None:
#     """
#     Map legacy prescription (medicine list JSON) → patient_instructions text.
#     Legacy format: [{"medicine":"Tab omez","instructions":"","morning":true,...}]
#     Stored as raw JSON string.
#     TODO: If new DB needs a specific format for medicines, parse and reformat here.
#     """
#     if not prescription or not str(prescription).strip():
#         return None
#     return str(prescription).strip()


# # ---------------------------------------------------------------------------
# # Record builder
# # ---------------------------------------------------------------------------

# def build_prescription_record(
#     row: Any,
#     new_id: str,
#     visit_uuid: str,
#     doctor_uuid: str | None,
# ) -> dict:
#     """
#     Build a single prescription record for INSERT into new DB.

#     Field mapping:
#       id                      → generated UUID
#       visit_id                → visit.id (via healthcase_id → visit.legacy_id)
#       doctor_user_id          → user.id (via doctor_id → Doctor_doctordetails → name+phone match)
#       assessment              → clinicalnote (NOT NULL, defaults to "")
#       provisional_diagnosis   → diagnosis
#       confirmed_diagnosis     → NULL (no separate confirmed diagnosis in legacy)
#       is_referral_recommended → FALSE (no legacy source)
#       referral_treatment_name → recommendation
#       referral_specialist_name→ NULL (no legacy source)
#       instructions            → recommendation
#       follow_up_at            → followupdatetime
#       created_at              → datetime
#       investigation_notes     → investigation (raw JSON string)
#       deleted_at              → NULL
#       patient_instructions    → prescription (medicine list raw JSON string)
#       legacy_id               → BodyVitals_prescription.id
#       legacy_source           → "digiswasthya_database"
#     """
#     return {
#         "id":                       new_id,
#         "visit_id":                 visit_uuid,
#         "doctor_user_id":           doctor_uuid,
#         "assessment":               row.get("diagnosis"),
#         "provisional_diagnosis":    row.get("diagnosis"),
#         "confirmed_diagnosis":      None,               # no separate confirmed in legacy
#         "is_referral_recommended":  False,              # no legacy source, default False
#         "referral_treatment_name":  None,
#         "referral_specialist_name": None,               # no legacy source
#         "instructions":             row.get("recommendation"),
#         "follow_up_at":             row.get("followupdatetime"),
#         "created_at":               row.get("datetime"),
#         "investigation_notes":      build_investigation_notes(row.get("investigation")),
#         "deleted_at":               None,
#         "patient_instructions":     build_assessment(row.get("clinicalnote")),
#         "legacy_id":                str(row["id"]),
#         "legacy_source":            LEGACY_SOURCE,
#     }


# # ---------------------------------------------------------------------------
# # INSERT SQL
# # ---------------------------------------------------------------------------

# INSERT_SQL = text("""
#     INSERT INTO prescription (
#         id,
#         visit_id,
#         doctor_user_id,
#         assessment,
#         provisional_diagnosis,
#         confirmed_diagnosis,
#         is_referral_recommended,
#         referral_treatment_name,
#         referral_specialist_name,
#         instructions,
#         follow_up_at,
#         created_at,
#         investigation_notes,
#         deleted_at,
#         patient_instructions,
#         legacy_id,
#         legacy_source
#     ) VALUES (
#         :id,
#         :visit_id,
#         :doctor_user_id,
#         :assessment,
#         :provisional_diagnosis,
#         :confirmed_diagnosis,
#         :is_referral_recommended,
#         :referral_treatment_name,
#         :referral_specialist_name,
#         :instructions,
#         :follow_up_at,
#         :created_at,
#         :investigation_notes,
#         :deleted_at,
#         :patient_instructions,
#         :legacy_id,
#         :legacy_source
#     )
#     ON CONFLICT (id) DO NOTHING
# """)


# # ---------------------------------------------------------------------------
# # Batch flush
# # ---------------------------------------------------------------------------

# def _flush_batch(new_engine: Any, batch: list[dict]) -> None:
#     if DRY_RUN:
#         log.info("[DRY RUN] Would insert %d prescription rows.", len(batch))
#         return

#     with Session(new_engine) as session:
#         session.execute(INSERT_SQL, batch)
#         session.commit()


# # ---------------------------------------------------------------------------
# # Preview
# # ---------------------------------------------------------------------------

# def preview_prescriptions(limit: int | None = None) -> None:
#     if limit is None:
#         limit = PREVIEW_SAMPLE_SIZE

#     legacy_engine = get_legacy_engine()
#     new_engine    = get_new_engine()

#     healthcase_id_to_visit_uuid = load_healthcase_id_to_visit_uuid(new_engine)
#     legacy_doctor_lookup        = load_legacy_doctor_lookup(legacy_engine)
#     doctor_name_phone_to_uuid   = load_doctor_name_phone_to_uuid(new_engine)

#     with legacy_engine.connect() as conn:
#         rows = conn.execute(
#             _FETCH_PRESCRIPTIONS_LIMITED_SQL,
#             {"target_appdetails_id": TARGET_APPDETAILS_ID, "lim": limit},
#         ).mappings().all()

#     log.info(
#         "════════ PREVIEW: prescription migration appdetails_id=%d — %d row(s) ════════",
#         TARGET_APPDETAILS_ID,
#         len(rows),
#     )

#     for i, row in enumerate(rows, start=1):
#         legacy_pres_id = row.get("id")
#         healthcase_id  = str(row.get("healthcase_id", ""))

#         visit_uuid = healthcase_id_to_visit_uuid.get(healthcase_id)
#         if visit_uuid is None:
#             log.warning(
#                 "PREVIEW [%d/%d] prescription_id=%s — WOULD SKIP: "
#                 "no visit found for healthcase_id=%s (visit not yet migrated?)",
#                 i, len(rows), legacy_pres_id, healthcase_id,
#             )
#             continue

#         doctor_uuid = resolve_doctor_uuid(
#             row.get("doctor_id"),
#             legacy_doctor_lookup,
#             doctor_name_phone_to_uuid,
#         )

#         if doctor_uuid is None:
#             log.warning(
#                 "PREVIEW [%d/%d] prescription_id=%s — WOULD SKIP: "
#                 "doctor_id=%s not mapped to any new DB user",
#                 i, len(rows), legacy_pres_id, row.get("doctor_id"),
#             )
#             continue

#         record = build_prescription_record(
#             row,
#             f"preview-{legacy_pres_id}",
#             visit_uuid,
#             doctor_uuid,
#         )

#         log.info(
#             "PREVIEW [%d/%d] prescription_id=%s healthcase_id=%s\n%s",
#             i, len(rows), legacy_pres_id, healthcase_id,
#             json.dumps(
#                 {k: v for k, v in record.items() if k != "patient_instructions"},
#                 indent=2, default=str,
#             ),
#         )

#     log.info("════════ PREVIEW complete — no rows written to new DB ════════")


# # ---------------------------------------------------------------------------
# # Main migration
# # ---------------------------------------------------------------------------

# def migrate_prescription(limit: int | None = None) -> None:
#     """
#     Migrate BodyVitals_prescription rows to prescription table in new DB.
#     Scoped to prescriptions whose healthcase has appdetails_id = 42 (Narsala).

#     NOTE: These prescriptions have appdetails_id=23 in BodyVitals_prescription
#     but their healthcase_id links to HealthCase_healthcase with appdetails_id=42.
#     We filter by healthcase, not by prescription's own appdetails_id.

#     Prerequisites (must run in order):
#       1. migrate_patient_appid42.py
#       2. migrate_visit_appid42.py
#       3. this script

#     Resolution chain:
#       visit_id:       healthcase_id → visit.legacy_id → visit.id
#       doctor_user_id: doctor_id → Doctor_doctordetails → user name+phone match
#     """
#     legacy_engine = get_legacy_engine()
#     new_engine    = get_new_engine()

#     healthcase_id_to_visit_uuid = load_healthcase_id_to_visit_uuid(new_engine)
#     legacy_doctor_lookup        = load_legacy_doctor_lookup(legacy_engine)
#     doctor_name_phone_to_uuid   = load_doctor_name_phone_to_uuid(new_engine)

#     already_migrated: set[str] = get_migrated_legacy_ids(new_engine, "prescription")
#     log.info("Already migrated in new DB: %d prescription rows", len(already_migrated))

#     with legacy_engine.connect() as conn:
#         if limit is not None:
#             rows = conn.execute(
#                 _FETCH_PRESCRIPTIONS_LIMITED_SQL,
#                 {"target_appdetails_id": TARGET_APPDETAILS_ID, "lim": limit},
#             ).mappings().all()
#         else:
#             rows = conn.execute(
#                 _FETCH_PRESCRIPTIONS_SQL,
#                 {"target_appdetails_id": TARGET_APPDETAILS_ID},
#             ).mappings().all()

#     total = len(rows)

#     with legacy_engine.connect() as conn:
#         legacy_total = conn.execute(
#             text("""
#                 SELECT COUNT(*) FROM "BodyVitals_prescription" p
#                 WHERE p.healthcase_id IN (
#                     SELECT id FROM "HealthCase_healthcase"
#                     WHERE appdetails_id = :target_appdetails_id
#                 )
#             """),
#             {"target_appdetails_id": TARGET_APPDETAILS_ID},
#         ).scalar()

#     log.info(
#         "══ SOURCE ══  BodyVitals_prescription eligible (healthcase appdetails_id=%d)=%d | "
#         "fetched=%d (limit=%s)",
#         TARGET_APPDETAILS_ID,
#         legacy_total,
#         total,
#         limit if limit is not None else "ALL",
#     )

#     id_gen = SafeIDGenerator(new_engine, table="prescription")

#     batch: list[dict] = []
#     inserted = skipped_migrated = skipped_no_visit = skipped_no_doctor = errors = 0
#     with_doctor = with_followup = with_investigation = 0

#     for row in rows:
#         legacy_pres_id = str(row.get("id", ""))
#         healthcase_id  = str(row.get("healthcase_id", ""))

#         # 1. Already migrated in a previous run → skip.
#         if legacy_pres_id in already_migrated:
#             log.debug("SKIP (already migrated) prescription_id=%s", legacy_pres_id)
#             skipped_migrated += 1
#             continue

#         # 2. No matching visit in new DB → skip.
#         # Happens if visit was skipped during visit migration.
#         visit_uuid = healthcase_id_to_visit_uuid.get(healthcase_id)
#         if visit_uuid is None:
#             log.warning(
#                 "SKIP prescription_id=%s — no visit found for healthcase_id=%s",
#                 legacy_pres_id, healthcase_id,
#             )
#             skipped_no_visit += 1
#             continue

#         try:
#             doctor_uuid = resolve_doctor_uuid(
#                 row.get("doctor_id"),
#                 legacy_doctor_lookup,
#                 doctor_name_phone_to_uuid,
#             )

#             # 3. No doctor mapped → skip.
#             if doctor_uuid is None:
#                 log.warning(
#                     "SKIP prescription_id=%s — doctor_id=%s not mapped to any new DB user",
#                     legacy_pres_id, row.get("doctor_id"),
#                 )
#                 skipped_no_doctor += 1
#                 continue

#             new_id = id_gen.next()

#             record = build_prescription_record(
#                 row,
#                 new_id,
#                 visit_uuid,
#                 doctor_uuid,
#             )

#             if doctor_uuid is not None:
#                 with_doctor += 1
#             if record.get("follow_up_at") is not None:
#                 with_followup += 1
#             if record.get("investigation_notes") is not None:
#                 with_investigation += 1

#             batch.append(record)

#             if len(batch) >= BATCH_SIZE:
#                 _flush_batch(new_engine, batch)
#                 inserted += len(batch)
#                 log.info("  … %d / %d rows committed", inserted, total)
#                 batch.clear()

#         except Exception as e:
#             log.error("ERROR processing prescription_id=%s: %s", legacy_pres_id, e)
#             errors += 1
#             continue

#     if batch:
#         _flush_batch(new_engine, batch)
#         inserted += len(batch)

#     log.info(
#         "═══ Prescription migration complete (appdetails_id=%d) ═══  "
#         "limit=%s | eligible=%d | fetched=%d | inserted=%d | "
#         "skipped(re-run)=%d | skipped(no-visit)=%d | skipped(no-doctor)=%d | errors=%d | "
#         "with_doctor=%d | with_followup=%d | with_investigation=%d",
#         TARGET_APPDETAILS_ID,
#         limit if limit is not None else "ALL",
#         legacy_total,
#         total,
#         inserted,
#         skipped_migrated,
#         skipped_no_visit,
#         skipped_no_doctor,
#         errors,
#         with_doctor,
#         with_followup,
#         with_investigation,
#     )


# # ---------------------------------------------------------------------------
# # Entry point
# # ---------------------------------------------------------------------------

# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(
#         description=(
#             f"Migrate prescriptions for appdetails_id={TARGET_APPDETAILS_ID} "
#             "(DS-TMC-009 / Narsala) from legacy DB to new DB."
#         ),
#         formatter_class=argparse.RawDescriptionHelpFormatter,
#         epilog="""
# Examples:
#   python migrate_prescription_appid42.py              # migrate ALL
#   python migrate_prescription_appid42.py --limit 10   # migrate first 10
#   python migrate_prescription_appid42.py --preview    # preview only, no DB writes

# Prerequisites (run in order):
#   1. migrate_patient_appid42.py
#   2. migrate_visit_appid42.py
#   3. migrate_prescription_appid42.py  ← this script
#         """,
#     )
#     parser.add_argument(
#         "--limit",
#         type=int,
#         default=None,
#         metavar="N",
#         help="Migrate only the first N prescriptions. Omit to migrate ALL.",
#     )
#     parser.add_argument(
#         "--preview",
#         action="store_true",
#         help="Run preview only — no rows are written to the new DB.",
#     )
#     args = parser.parse_args()

#     preview_sample = args.limit if args.limit is not None else PREVIEW_SAMPLE_SIZE
#     preview_prescriptions(limit=preview_sample)

#     if args.preview:
#         log.info("--preview flag set — exiting without migrating.")
#         sys.exit(0)

#     limit_label = str(args.limit) if args.limit is not None else "ALL"
#     log.info(
#         "Review the PREVIEW logs above. "
#         "About to migrate %s prescription(s) with appdetails_id=%d. "
#         "Type 'yes' to proceed.",
#         limit_label,
#         TARGET_APPDETAILS_ID,
#     )
#     answer = input("Migrate to new DB? (yes/no): ").strip().lower()
#     if answer != "yes":
#         log.info("Migration cancelled — no data written to new DB.")
#         sys.exit(0)

#     migrate_prescription(limit=args.limit)



import json
import sys
import os
import argparse
from datetime import datetime, timezone
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.db import get_legacy_engine, get_new_engine
from config.config import BATCH_SIZE, DRY_RUN, PREVIEW_SAMPLE_SIZE
from utils.id_gen import SafeIDGenerator, get_migrated_legacy_ids
from utils.logger import get_logger


log = get_logger("migrate_prescription")

LEGACY_SOURCE_TABLE = "BodyVitals_prescription"
LEGACY_SOURCE       = "digiswasthya_database"

TARGET_APPDETAILS_ID: int = 42


_FETCH_PRESCRIPTIONS_SQL = text("""
    SELECT DISTINCT ON (p.healthcase_id)
        p.id,
        p.prescriptionid,
        p.prescription,
        p.investigation,
        p.clinicalnote,
        p.diagnosis,
        p.recommendation,
        p.datetime,
        p.followupdatetime,
        p.appdetails_id,
        p.doctor_id,
        p.person_id,
        p.healthcase_id
    FROM "BodyVitals_prescription" p
    WHERE p.healthcase_id IS NOT NULL
      AND p.person_id IS NOT NULL
      AND p.healthcase_id IN (
          SELECT id FROM "HealthCase_healthcase"
          WHERE appdetails_id = :target_appdetails_id
      )
    ORDER BY p.healthcase_id, p.id DESC
""")

_FETCH_PRESCRIPTIONS_LIMITED_SQL = text("""
    SELECT DISTINCT ON (p.healthcase_id)
        p.id,
        p.prescriptionid,
        p.prescription,
        p.investigation,
        p.clinicalnote,
        p.diagnosis,
        p.recommendation,
        p.datetime,
        p.followupdatetime,
        p.appdetails_id,
        p.doctor_id,
        p.person_id,
        p.healthcase_id
    FROM "BodyVitals_prescription" p
    WHERE p.healthcase_id IS NOT NULL
      AND p.person_id IS NOT NULL
      AND p.healthcase_id IN (
          SELECT id FROM "HealthCase_healthcase"
          WHERE appdetails_id = :target_appdetails_id
      )
    ORDER BY p.healthcase_id, p.id DESC
    LIMIT :lim
""")

_FETCH_DOCTOR_DETAILS_SQL = text("""
    SELECT
        d.doctorid,
        d.name   AS doctor_name,
        d.phone  AS doctor_phone
    FROM "Doctor_doctordetails" d
    WHERE d.doctorid IS NOT NULL
""")


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def load_healthcase_id_to_visit_uuid(new_engine: Any) -> dict[str, str]:
    """Load { legacy_healthcase_id → visit.id (UUID) } from new DB."""
    with new_engine.connect() as conn:
        rows = conn.execute(
            text("SELECT legacy_id, id::text FROM visit WHERE legacy_id IS NOT NULL")
        ).fetchall()
    return {row[0]: row[1] for row in rows}


def _normalize_phone(phone: str) -> str:
    if not phone:
        return ""
    digits = "".join(c for c in phone if c.isdigit())
    return digits[-10:] if len(digits) > 10 else digits


def load_legacy_doctor_lookup(legacy_engine: Any) -> dict[int, tuple[str, str]]:
    """Load { doctor_id → (name, phone) } from legacy Doctor_doctordetails."""
    with legacy_engine.connect() as conn:
        rows = conn.execute(_FETCH_DOCTOR_DETAILS_SQL).fetchall()
    return {
        int(doctorid): ((name or "").strip(), (phone or "").strip())
        for doctorid, name, phone in rows
        if doctorid is not None
    }


def load_doctor_name_phone_to_uuid(new_engine: Any) -> tuple[dict[tuple[str, str], str], dict[str, str]]:
    """
    Load two lookups for active DOCTOR role users in new DB:
      1. { (normalized_name_lower, phone_last10) → user.id }  — primary
      2. { phone_last10 → user.id }                           — fallback (handles name mismatches)
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
        norm_name  = (full_name or "").strip().lower()
        norm_phone = _normalize_phone(phone or "")
        if norm_name or norm_phone:
            name_phone_lookup[(norm_name, norm_phone)] = user_id
        if norm_phone:
            phone_only_lookup[norm_phone] = user_id
    return name_phone_lookup, phone_only_lookup


def resolve_doctor_uuid(
    doctor_id: Any,
    legacy_doctor_lookup: dict[int, tuple[str, str]],
    doctor_name_phone_to_uuid: tuple[dict[tuple[str, str], str], dict[str, str]],
) -> str | None:
    if doctor_id is None:
        return None
    try:
        did = int(doctor_id)
    except (TypeError, ValueError):
        return None

    doctor_info = legacy_doctor_lookup.get(did)
    if doctor_info is None:
        return None

    doctor_name, doctor_phone = doctor_info
    name_phone_lookup, phone_only_lookup = doctor_name_phone_to_uuid

    user_uuid = name_phone_lookup.get((doctor_name.lower(), _normalize_phone(doctor_phone)))
    if user_uuid is None:
        user_uuid = phone_only_lookup.get(_normalize_phone(doctor_phone))
    return user_uuid


# ---------------------------------------------------------------------------
# Field helpers
# ---------------------------------------------------------------------------

def build_assessment(clinicalnote: Any) -> str:
    return (clinicalnote or "").strip() or ""


def build_investigation_notes(investigation: Any) -> str | None:
    if not investigation or not str(investigation).strip():
        return None
    try:
        parsed = json.loads(str(investigation).strip())
        if not isinstance(parsed, list):
            return str(investigation).strip()
        names = [
            item["investigationname"].strip()
            for item in parsed
            if isinstance(item, dict) and item.get("investigationname", "").strip()
        ]
        return "\n".join(names) if names else None
    except (json.JSONDecodeError, TypeError):
        log.warning("Could not parse investigation JSON: %s", str(investigation)[:120])
        return str(investigation).strip()


def build_patient_instructions(prescription: Any) -> str | None:
    if not prescription or not str(prescription).strip():
        return None
    return str(prescription).strip()


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def build_prescription_record(
    row: Any,
    new_id: str,
    visit_uuid: str,
    doctor_uuid: str | None,
) -> dict:
    return {
        "id":                       new_id,
        "visit_id":                 visit_uuid,
        "doctor_user_id":           doctor_uuid,
        "assessment":               row.get("diagnosis"),
        "provisional_diagnosis":    row.get("diagnosis"),
        "confirmed_diagnosis":      None,
        "is_referral_recommended":  False,
        "referral_treatment_name":  None,
        "referral_specialist_name": None,
        "instructions":             row.get("recommendation"),
        "follow_up_at":             row.get("followupdatetime"),
        "created_at":               row.get("datetime"),
        "investigation_notes":      build_investigation_notes(row.get("investigation")),
        "deleted_at":               None,
        "patient_instructions":     build_assessment(row.get("clinicalnote")),
        "legacy_id":                str(row["id"]),
        "legacy_source":            LEGACY_SOURCE,
    }


# ---------------------------------------------------------------------------
# INSERT SQL
# ---------------------------------------------------------------------------

INSERT_SQL = text("""
    INSERT INTO prescription (
        id,
        visit_id,
        doctor_user_id,
        assessment,
        provisional_diagnosis,
        confirmed_diagnosis,
        is_referral_recommended,
        referral_treatment_name,
        referral_specialist_name,
        instructions,
        follow_up_at,
        created_at,
        investigation_notes,
        deleted_at,
        patient_instructions,
        legacy_id,
        legacy_source
    ) VALUES (
        :id,
        :visit_id,
        :doctor_user_id,
        :assessment,
        :provisional_diagnosis,
        :confirmed_diagnosis,
        :is_referral_recommended,
        :referral_treatment_name,
        :referral_specialist_name,
        :instructions,
        :follow_up_at,
        :created_at,
        :investigation_notes,
        :deleted_at,
        :patient_instructions,
        :legacy_id,
        :legacy_source
    )
    ON CONFLICT (id) DO NOTHING
""")


# ---------------------------------------------------------------------------
# Batch flush
# ---------------------------------------------------------------------------

def _flush_batch(new_engine: Any, batch: list[dict]) -> None:
    if DRY_RUN:
        log.info("[DRY RUN] Would insert %d prescription rows.", len(batch))
        return
    with Session(new_engine) as session:
        session.execute(INSERT_SQL, batch)
        session.commit()


# ---------------------------------------------------------------------------
# Preview
# ---------------------------------------------------------------------------

def preview_prescriptions(limit: int | None = None) -> None:
    if limit is None:
        limit = PREVIEW_SAMPLE_SIZE

    legacy_engine = get_legacy_engine()
    new_engine    = get_new_engine()

    healthcase_id_to_visit_uuid = load_healthcase_id_to_visit_uuid(new_engine)
    legacy_doctor_lookup        = load_legacy_doctor_lookup(legacy_engine)
    doctor_name_phone_to_uuid   = load_doctor_name_phone_to_uuid(new_engine)

    with legacy_engine.connect() as conn:
        rows = conn.execute(
            _FETCH_PRESCRIPTIONS_LIMITED_SQL,
            {"target_appdetails_id": TARGET_APPDETAILS_ID, "lim": limit},
        ).mappings().all()

    log.info("════════ PREVIEW: prescription migration appdetails_id=%d — %d row(s) ════════",
             TARGET_APPDETAILS_ID, len(rows))

    for i, row in enumerate(rows, start=1):
        legacy_pres_id = row.get("id")
        healthcase_id  = str(row.get("healthcase_id", ""))

        visit_uuid = healthcase_id_to_visit_uuid.get(healthcase_id)
        if visit_uuid is None:
            log.warning("PREVIEW [%d/%d] prescription_id=%s — WOULD SKIP: no visit for healthcase_id=%s",
                        i, len(rows), legacy_pres_id, healthcase_id)
            continue

        doctor_uuid = resolve_doctor_uuid(row.get("doctor_id"), legacy_doctor_lookup, doctor_name_phone_to_uuid)
        if doctor_uuid is None:
            log.warning("PREVIEW [%d/%d] prescription_id=%s — WOULD SKIP: doctor_id=%s not mapped",
                        i, len(rows), legacy_pres_id, row.get("doctor_id"))
            continue

        record = build_prescription_record(row, f"preview-{legacy_pres_id}", visit_uuid, doctor_uuid)
        log.info("PREVIEW [%d/%d] prescription_id=%s healthcase_id=%s\n%s",
                 i, len(rows), legacy_pres_id, healthcase_id,
                 json.dumps({k: v for k, v in record.items() if k != "patient_instructions"},
                            indent=2, default=str))

    log.info("════════ PREVIEW complete — no rows written to new DB ════════")


# ---------------------------------------------------------------------------
# Main migration
# ---------------------------------------------------------------------------

def migrate_prescription(limit: int | None = None) -> None:
    """
    Migrate BodyVitals_prescription rows to prescription table in new DB.
    Scoped to prescriptions whose healthcase has appdetails_id = 42 (Narsala).

    Prerequisites (run in order):
      1. migrate_patient_appid42.py
      2. migrate_visit_appid42.py
      3. this script
    """
    legacy_engine = get_legacy_engine()
    new_engine    = get_new_engine()

    healthcase_id_to_visit_uuid = load_healthcase_id_to_visit_uuid(new_engine)
    legacy_doctor_lookup        = load_legacy_doctor_lookup(legacy_engine)
    doctor_name_phone_to_uuid   = load_doctor_name_phone_to_uuid(new_engine)
    already_migrated: set[str]  = get_migrated_legacy_ids(new_engine, "prescription")

    with legacy_engine.connect() as conn:
        if limit is not None:
            rows = conn.execute(
                _FETCH_PRESCRIPTIONS_LIMITED_SQL,
                {"target_appdetails_id": TARGET_APPDETAILS_ID, "lim": limit},
            ).mappings().all()
        else:
            rows = conn.execute(
                _FETCH_PRESCRIPTIONS_SQL,
                {"target_appdetails_id": TARGET_APPDETAILS_ID},
            ).mappings().all()

    total = len(rows)
    log.info("Starting prescription migration — fetched=%d (limit=%s) | already_migrated=%d",
             total, limit if limit is not None else "ALL", len(already_migrated))

    id_gen = SafeIDGenerator(new_engine, table="prescription")
    batch: list[dict] = []
    inserted = skipped_migrated = skipped_no_visit = skipped_no_doctor = errors = 0
    with_followup = with_investigation = 0

    for row in rows:
        legacy_pres_id = str(row.get("id", ""))
        healthcase_id  = str(row.get("healthcase_id", ""))

        if legacy_pres_id in already_migrated:
            skipped_migrated += 1
            continue

        visit_uuid = healthcase_id_to_visit_uuid.get(healthcase_id)
        if visit_uuid is None:
            log.warning("SKIP prescription_id=%s — no visit for healthcase_id=%s",
                        legacy_pres_id, healthcase_id)
            skipped_no_visit += 1
            continue

        try:
            doctor_uuid = resolve_doctor_uuid(
                row.get("doctor_id"), legacy_doctor_lookup, doctor_name_phone_to_uuid
            )
            if doctor_uuid is None:
                log.warning("SKIP prescription_id=%s — doctor_id=%s not mapped",
                            legacy_pres_id, row.get("doctor_id"))
                skipped_no_doctor += 1
                continue

            record = build_prescription_record(row, id_gen.next(), visit_uuid, doctor_uuid)

            if record.get("follow_up_at") is not None:
                with_followup += 1
            if record.get("investigation_notes") is not None:
                with_investigation += 1

            batch.append(record)

            if len(batch) >= BATCH_SIZE:
                _flush_batch(new_engine, batch)
                inserted += len(batch)
                log.info("  … %d / %d committed", inserted, total)
                batch.clear()

        except Exception as e:
            log.error("ERROR prescription_id=%s: %s", legacy_pres_id, e)
            errors += 1

    if batch:
        _flush_batch(new_engine, batch)
        inserted += len(batch)

    log.info(
        "═══ Prescription migration complete (appdetails_id=%d) ═══  "
        "limit=%s | fetched=%d | inserted=%d | "
        "skipped(re-run)=%d | skipped(no-visit)=%d | skipped(no-doctor)=%d | errors=%d | "
        "with_followup=%d | with_investigation=%d",
        TARGET_APPDETAILS_ID,
        limit if limit is not None else "ALL",
        total,
        inserted,
        skipped_migrated,
        skipped_no_visit,
        skipped_no_doctor,
        errors,
        with_followup,
        with_investigation,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            f"Migrate prescriptions for appdetails_id={TARGET_APPDETAILS_ID} "
            "(DS-TMC-009 / Narsala) from legacy DB to new DB."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python migrate_prescription_appid42.py              # migrate ALL
  python migrate_prescription_appid42.py --limit 10   # migrate first 10
  python migrate_prescription_appid42.py --preview    # preview only, no DB writes

Prerequisites (run in order):
  1. migrate_patient_appid42.py
  2. migrate_visit_appid42.py
  3. migrate_prescription_appid42.py  ← this script
        """,
    )
    parser.add_argument("--limit", type=int, default=None, metavar="N",
                        help="Migrate only the first N prescriptions. Omit to migrate ALL.")
    parser.add_argument("--preview", action="store_true",
                        help="Run preview only — no rows are written to the new DB.")
    args = parser.parse_args()

    preview_sample = args.limit if args.limit is not None else PREVIEW_SAMPLE_SIZE
    preview_prescriptions(limit=preview_sample)

    if args.preview:
        log.info("--preview flag set — exiting without migrating.")
        sys.exit(0)

    limit_label = str(args.limit) if args.limit is not None else "ALL"
    log.info("About to migrate %s prescription(s) for appdetails_id=%d. Type 'yes' to proceed.",
             limit_label, TARGET_APPDETAILS_ID)
    if input("Migrate to new DB? (yes/no): ").strip().lower() != "yes":
        log.info("Migration cancelled.")
        sys.exit(0)

    migrate_prescription(limit=args.limit)