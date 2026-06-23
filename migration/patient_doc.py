"""
Migration: TWO source tables -> patientdocument  (+ S3 copy ds-prod-new -> digiaarogyasaarathifiles)

SOURCE 1 — BodyVitals_imagereportdetails  (legacy_source = "bodyvitals_imagereport")
  - reportimgurl  : semicolon-separated S3 keys (one patientdocument row per key)
  - person_id     : -> patient.legacy_id
  - appdetails_id : NOT the healthcase_id; ignored for visit linkage in this table
  - legacy_id     : "bir_{id}_{img_idx}"

SOURCE 2 — HealthCase_attachedreporthealthcase  (legacy_source = "healthcase_attached")
  - attachedreportid joins -> BodyVitals_imagereportdetails.imagereportid to get the S3 key
  - healthcase_id  : -> visit.legacy_id  (direct FK, always set)
  - person_id      : comes from the joined BodyVitals row
  - legacy_id      : "hca_{id}"   (one row per attached report — always single file)
  - source         : "coordinator_portal" or "doctor_portal" from uploaderrole

Join key:
  HealthCase_attachedreporthealthcase.attachedreportid
      = BodyVitals_imagereportdetails.imagereportid

Idempotent via legacy_id + legacy_source checked against patientdocument.
Respects DRY_RUN from config.config. Batch inserts matching visit.py pattern.
"""

import sys
import os
import argparse
from datetime import datetime, timezone
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import boto3
from botocore.exceptions import ClientError
from sqlalchemy import text
from sqlalchemy.orm import Session

from config.db import get_legacy_engine, get_new_engine
from config.config import BATCH_SIZE, DRY_RUN, PREVIEW_SAMPLE_SIZE
from utils.id_gen import SafeIDGenerator, get_migrated_legacy_ids
from utils.logger import get_logger

log = get_logger("migrate_documents")

OLD_S3_BUCKET      = os.environ.get("OLD_S3_BUCKET", "ds-prod-new")
NEW_S3_BUCKET      = os.environ.get("NEW_S3_BUCKET", "digiaarogyasaarathifiles")
AWS_REGION         = os.environ.get("AWS_REGION", "ap-south-1")
S3_PUBLIC_BASE_URL = os.environ.get(
    "S3_PUBLIC_BASE_URL",
    f"https://{NEW_S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com",
).rstrip("/")

CATEGORY      = "PATIENT_MEDICAL_RECORDS"
DOCUMENT_TYPE = "PRESCRIPTION"
VISIBILITY    = "['doctor', 'coordinator']"


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------

def get_s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )


def copy_s3_object(s3, old_key: str, new_key: str) -> bool:
    if DRY_RUN:
        log.info("[DRY RUN] S3 copy  %s/%s  ->  %s/%s", OLD_S3_BUCKET, old_key, NEW_S3_BUCKET, new_key)
        return True
    try:
        s3.copy_object(
            CopySource={"Bucket": OLD_S3_BUCKET, "Key": old_key},
            Bucket=NEW_S3_BUCKET,
            Key=new_key,
        )
        log.debug("S3 copied  %s  ->  %s", old_key, new_key)
        return True
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("NoSuchKey", "404"):
            log.warning("S3 source not found, skipping:  %s/%s", OLD_S3_BUCKET, old_key)
        else:
            log.error("S3 copy failed  %s -> %s: %s", old_key, new_key, e)
        return False
    except Exception as e:
        log.error("S3 copy failed  %s -> %s: %s", old_key, new_key, e)
        return False


# ---------------------------------------------------------------------------
# Key / URL helpers
# ---------------------------------------------------------------------------

def filename_from_key(old_key: str) -> str:
    """Return only the bare filename (last segment) — used for display name only."""
    return old_key.strip().split("/")[-1]


def build_new_s3_key(new_patient_id: str, old_key: str) -> str:
    """
    Flatten all images directly under the patient folder, stripping any
    subfolder segments from the old key.

    OLD:  ds-prod-new/2xsqqgw/image_1771653159275_pgn05i.jpg
    NEW:  digiaarogyasaarathifiles/documents/{new_patient_id}/image_1771653159275_pgn05i.jpg

    OLD:  ds-prod-new/65hge9/nyhz8dd/healthcase_nyhz8dd_person_65hge9_1763791423973.jpg
    NEW:  digiaarogyasaarathifiles/documents/{new_patient_id}/healthcase_nyhz8dd_person_65hge9_1763791423973.jpg
    """
    return f"documents/{new_patient_id}/{filename_from_key(old_key)}"


def build_storage_url(storage_key: str) -> str:
    return f"{S3_PUBLIC_BASE_URL}/{storage_key}"


def content_type_from_key(key: str) -> str:
    ext = key.rsplit(".", 1)[-1].lower() if "." in key else ""
    return {
        "jpg":  "image/jpeg",
        "jpeg": "image/jpeg",
        "png":  "image/png",
        "gif":  "image/gif",
        "webp": "image/webp",
        "pdf":  "application/pdf",
    }.get(ext, "application/octet-stream")


def source_from_uploaderrole(role: str | None) -> str:
    """Map old uploaderrole string to new source value."""
    if not role:
        return "MIGRATION"
    role_lower = (role or "").strip().lower()
    if role_lower == "coordinator":
        return "coordinator_portal"
    if role_lower == "doctor":
        return "doctor_portal"
    return "MIGRATION"


# ---------------------------------------------------------------------------
# Source SQL
# ---------------------------------------------------------------------------

# Source 1: BodyVitals_imagereportdetails
_BIR_FETCH_SQL = text("""
    SELECT
        ir.id,
        ir.imagereportid,
        ir.reportimgurl,
        ir.reporttitle,
        ir.datetime,
        ir.person_id
    FROM "BodyVitals_imagereportdetails" ir
    WHERE ir.reportimgurl IS NOT NULL
      AND ir.reportimgurl <> ''
      AND ir.person_id    IS NOT NULL
    ORDER BY ir.id ASC
""")

_BIR_FETCH_LIMITED_SQL = text("""
    SELECT
        ir.id,
        ir.imagereportid,
        ir.reportimgurl,
        ir.reporttitle,
        ir.datetime,
        ir.person_id
    FROM "BodyVitals_imagereportdetails" ir
    WHERE ir.reportimgurl IS NOT NULL
      AND ir.reportimgurl <> ''
      AND ir.person_id    IS NOT NULL
    ORDER BY ir.id ASC
    LIMIT :lim
""")

# Source 2: HealthCase_attachedreporthealthcase JOIN BodyVitals_imagereportdetails
# Join on attachedreportid = imagereportid to get the S3 key and person_id.
# healthcase_id -> visit.legacy_id directly.
_HCA_FETCH_SQL = text("""
    SELECT
        hca.id,
        hca.attachedreportid,
        hca.attachedreporttitle,
        hca.datetime,
        hca.uploaderrole,
        hca.healthcase_id,
        ir.reportimgurl,
        ir.person_id
    FROM "HealthCase_attachedreporthealthcase" hca
    INNER JOIN "BodyVitals_imagereportdetails" ir
        ON ir.imagereportid = hca.attachedreportid
    WHERE ir.reportimgurl IS NOT NULL
      AND ir.reportimgurl <> ''
      AND ir.person_id    IS NOT NULL
      AND hca.healthcase_id IS NOT NULL
    ORDER BY hca.id ASC
""")

_HCA_FETCH_LIMITED_SQL = text("""
    SELECT
        hca.id,
        hca.attachedreportid,
        hca.attachedreporttitle,
        hca.datetime,
        hca.uploaderrole,
        hca.healthcase_id,
        ir.reportimgurl,
        ir.person_id
    FROM "HealthCase_attachedreporthealthcase" hca
    INNER JOIN "BodyVitals_imagereportdetails" ir
        ON ir.imagereportid = hca.attachedreportid
    WHERE ir.reportimgurl IS NOT NULL
      AND ir.reportimgurl <> ''
      AND ir.person_id    IS NOT NULL
      AND hca.healthcase_id IS NOT NULL
    ORDER BY hca.id ASC
    LIMIT :lim
""")


# ---------------------------------------------------------------------------
# Lookups  (same pattern as visit.py)
# ---------------------------------------------------------------------------

def load_person_id_to_patient_uuid(new_engine: Any) -> dict[str, str]:
    with new_engine.connect() as conn:
        rows = conn.execute(
            text("SELECT legacy_id, id::text FROM patient WHERE legacy_id IS NOT NULL")
        ).fetchall()
    mapping = {str(row[0]): row[1] for row in rows}
    log.info("Loaded %d patient legacy_id -> UUID mappings", len(mapping))
    return mapping


def load_healthcase_id_to_visit_uuid(new_engine: Any) -> dict[str, str]:
    with new_engine.connect() as conn:
        rows = conn.execute(
            text("SELECT legacy_id, id::text FROM visit WHERE legacy_id IS NOT NULL")
        ).fetchall()
    mapping = {str(row[0]): row[1] for row in rows}
    log.info("Loaded %d visit legacy_id -> UUID mappings", len(mapping))
    return mapping


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def build_document_record(
    new_doc_id: str,
    new_patient_id: str,
    new_visit_id: str | None,
    storage_key: str,
    storage_url: str,
    content_type: str,
    name: str,
    source: str,
    created_at: Any,
    legacy_id: str,
    legacy_source: str,
) -> dict:
    return {
        "id":                  new_doc_id,
        "patient_id":          new_patient_id,
        "visit_id":            new_visit_id,
        "name":                name,
        "file_path":           storage_url,
        "storage_key":         storage_key,
        "storage_url":         storage_url,
        "content_type":        content_type,
        "category":            CATEGORY,
        "document_type":       DOCUMENT_TYPE,
        "visibility":          VISIBILITY,
        "is_system_generated": False,
        "is_pinned":           False,
        "file_size_bytes":     None,
        "source":              source,
        "triage_session_id":   None,
        "uploaded_by_user_id": None,
        "deleted_at":          None,
        "legacy_id":           legacy_id,
        "legacy_source":       'Digiswasthya Database',
        "created_at":          created_at,
    }


# ---------------------------------------------------------------------------
# INSERT SQL + flush  (same pattern as visit.py)
# ---------------------------------------------------------------------------

INSERT_SQL = text("""
    INSERT INTO patientdocument (
        id, patient_id, visit_id, name, file_path,
        storage_key, storage_url, content_type,
        category, document_type, visibility,
        is_system_generated, is_pinned, file_size_bytes,
        source, triage_session_id, uploaded_by_user_id,
        deleted_at, legacy_id, legacy_source, created_at
    ) VALUES (
        :id, :patient_id, :visit_id, :name, :file_path,
        :storage_key, :storage_url, :content_type,
        :category, :document_type, :visibility,
        :is_system_generated, :is_pinned, :file_size_bytes,
        :source, :triage_session_id, :uploaded_by_user_id,
        :deleted_at, :legacy_id, :legacy_source, :created_at
    )
    ON CONFLICT (id) DO NOTHING
""")


def _flush_batch(new_engine: Any, batch: list[dict]) -> None:
    if DRY_RUN:
        log.info("[DRY RUN] Would insert %d patientdocument rows.", len(batch))
        return
    with Session(new_engine) as session:
        session.execute(INSERT_SQL, batch)
        session.commit()


# ---------------------------------------------------------------------------
# Source 1 migration: BodyVitals_imagereportdetails
# ---------------------------------------------------------------------------

def migrate_bodyvitals(
    new_engine: Any,
    legacy_engine: Any,
    s3,
    id_gen: Any,
    person_to_patient: dict,
    already_migrated: set,
    limit: int | None,
) -> dict:
    with legacy_engine.connect() as conn:
        if limit is not None:
            rows = conn.execute(_BIR_FETCH_LIMITED_SQL, {"lim": limit}).mappings().all()
        else:
            rows = conn.execute(_BIR_FETCH_SQL).mappings().all()

    with legacy_engine.connect() as conn:
        total_in_db = conn.execute(
            text('SELECT COUNT(*) FROM "BodyVitals_imagereportdetails"')
        ).scalar()

    log.info(
        "== SOURCE 1 ==  BodyVitals_imagereportdetails total=%d | fetched=%d (limit=%s)",
        total_in_db, len(rows), limit if limit is not None else "ALL",
    )

    batch: list[dict] = []
    inserted = skipped_migrated = skipped_no_patient = s3_failures = errors = 0

    for row in rows:
        old_id    = row.get("id")
        person_id = str(row.get("person_id", ""))
        img_urls  = [k.strip() for k in (row.get("reportimgurl") or "").split(";") if k.strip()]
        fname_title = (row.get("reporttitle") or "").strip()

        new_patient_id = person_to_patient.get(person_id)
        if not new_patient_id:
            log.warning("SKIP [BIR] id=%s -- no patient for person_id=%s", old_id, person_id)
            skipped_no_patient += 1
            continue

        for idx, old_key in enumerate(img_urls):
            legacy_id = f"bir_{old_id}_{idx}"

            if legacy_id in already_migrated:
                log.debug("SKIP (already migrated) legacy_id=%s", legacy_id)
                skipped_migrated += 1
                continue

            try:
                new_s3_key   = build_new_s3_key(new_patient_id, old_key)
                storage_url  = build_storage_url(new_s3_key)
                fname        = filename_from_key(old_key)
                content_type = content_type_from_key(old_key)
                name         = fname_title or fname

                ok = copy_s3_object(s3, old_key, new_s3_key)
                if not ok:
                    s3_failures += 1
                    continue

                record = build_document_record(
                    new_doc_id=id_gen.next(),
                    new_patient_id=new_patient_id,
                    new_visit_id=None,   # BIR has no direct healthcase_id FK
                    storage_key=new_s3_key,
                    storage_url=storage_url,
                    content_type=content_type,
                    name=name,
                    source="MIGRATION",
                    created_at=row.get("datetime"),
                    legacy_id=legacy_id,
                    legacy_source=LEGACY_SOURCE_BIR,
                )
                batch.append(record)

                if len(batch) >= BATCH_SIZE:
                    _flush_batch(new_engine, batch)
                    inserted += len(batch)
                    log.info("  [BIR] ... %d rows committed", inserted)
                    batch.clear()

            except Exception as e:
                log.error("ERROR [BIR] id=%s img_idx=%d key=%s: %s", old_id, idx, old_key, e)
                errors += 1

    if batch:
        _flush_batch(new_engine, batch)
        inserted += len(batch)

    log.info(
        "=== Source 1 (BIR) done ===  fetched=%d | inserted=%d | "
        "skipped(re-run)=%d | skipped(no-patient)=%d | s3_fail=%d | errors=%d",
        len(rows), inserted, skipped_migrated, skipped_no_patient, s3_failures, errors,
    )
    return {"inserted": inserted, "skipped_migrated": skipped_migrated,
            "skipped_no_patient": skipped_no_patient, "s3_failures": s3_failures, "errors": errors}


# ---------------------------------------------------------------------------
# Source 2 migration: HealthCase_attachedreporthealthcase
# ---------------------------------------------------------------------------

def migrate_healthcase_attached(
    new_engine: Any,
    legacy_engine: Any,
    s3,
    id_gen: Any,
    person_to_patient: dict,
    healthcase_to_visit: dict,
    already_migrated: set,
    limit: int | None,
) -> dict:
    with legacy_engine.connect() as conn:
        if limit is not None:
            rows = conn.execute(_HCA_FETCH_LIMITED_SQL, {"lim": limit}).mappings().all()
        else:
            rows = conn.execute(_HCA_FETCH_SQL).mappings().all()

    with legacy_engine.connect() as conn:
        total_in_db = conn.execute(
            text('SELECT COUNT(*) FROM "HealthCase_attachedreporthealthcase"')
        ).scalar()

    log.info(
        "== SOURCE 2 ==  HealthCase_attachedreporthealthcase total=%d | fetched=%d (limit=%s)",
        total_in_db, len(rows), limit if limit is not None else "ALL",
    )

    batch: list[dict] = []
    inserted = skipped_migrated = skipped_no_patient = skipped_no_s3_key = s3_failures = errors = 0
    with_visit = 0

    for row in rows:
        old_id       = row.get("id")
        person_id    = str(row.get("person_id", ""))
        healthcase_id = str(row.get("healthcase_id", ""))
        legacy_id    = f"hca_{old_id}"

        if legacy_id in already_migrated:
            log.debug("SKIP (already migrated) legacy_id=%s", legacy_id)
            skipped_migrated += 1
            continue

        new_patient_id = person_to_patient.get(person_id)
        if not new_patient_id:
            log.warning(
                "SKIP [HCA] id=%s -- no patient for person_id=%s (healthcase=%s)",
                old_id, person_id, healthcase_id,
            )
            skipped_no_patient += 1
            continue

        # reportimgurl from the joined BIR row — take only the FIRST key
        # (HCA is always one-to-one with a BIR row via attachedreportid=imagereportid)
        img_urls = [k.strip() for k in (row.get("reportimgurl") or "").split(";") if k.strip()]
        if not img_urls:
            log.warning("SKIP [HCA] id=%s -- no S3 key found in joined BIR row", old_id)
            skipped_no_s3_key += 1
            continue
        old_key = img_urls[0]   # always single-file per attached report

        new_visit_id = healthcase_to_visit.get(healthcase_id)
        source       = source_from_uploaderrole(row.get("uploaderrole"))
        title        = (row.get("attachedreporttitle") or "").strip()
        fname        = filename_from_key(old_key)
        name         = title if title and title not in (".", "1", "P", "Q", "B", "T", "I") else fname

        try:
            new_s3_key   = build_new_s3_key(new_patient_id, old_key)
            storage_url  = build_storage_url(new_s3_key)
            content_type = content_type_from_key(old_key)

            ok = copy_s3_object(s3, old_key, new_s3_key)
            if not ok:
                s3_failures += 1
                continue

            record = build_document_record(
                new_doc_id=id_gen.next(),
                new_patient_id=new_patient_id,
                new_visit_id=new_visit_id,
                storage_key=new_s3_key,
                storage_url=storage_url,
                content_type=content_type,
                name=name,
                source=source,
                created_at=row.get("datetime"),
                legacy_id=legacy_id,
                legacy_source=LEGACY_SOURCE_HCA,
            )
            batch.append(record)

            if new_visit_id:
                with_visit += 1

            if len(batch) >= BATCH_SIZE:
                _flush_batch(new_engine, batch)
                inserted += len(batch)
                log.info("  [HCA] ... %d rows committed", inserted)
                batch.clear()

        except Exception as e:
            log.error("ERROR [HCA] id=%s healthcase=%s: %s", old_id, healthcase_id, e)
            errors += 1

    if batch:
        _flush_batch(new_engine, batch)
        inserted += len(batch)

    log.info(
        "=== Source 2 (HCA) done ===  fetched=%d | inserted=%d | "
        "skipped(re-run)=%d | skipped(no-patient)=%d | skipped(no-s3-key)=%d | "
        "s3_fail=%d | errors=%d | with_visit_id=%d",
        len(rows), inserted, skipped_migrated, skipped_no_patient,
        skipped_no_s3_key, s3_failures, errors, with_visit,
    )
    return {"inserted": inserted, "skipped_migrated": skipped_migrated,
            "skipped_no_patient": skipped_no_patient, "s3_failures": s3_failures,
            "errors": errors, "with_visit": with_visit}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def migrate_documents(limit: int | None = None) -> None:
    legacy_engine = get_legacy_engine()
    new_engine    = get_new_engine()

    person_to_patient   = load_person_id_to_patient_uuid(new_engine)
    healthcase_to_visit = load_healthcase_id_to_visit_uuid(new_engine)

    already_migrated: set[str] = get_migrated_legacy_ids(new_engine, "patientdocument")
    log.info("Already migrated in new DB: %d patientdocument rows", len(already_migrated))

    s3     = get_s3_client()
    id_gen = SafeIDGenerator(new_engine, table="patientdocument")

    r1 = migrate_bodyvitals(
        new_engine, legacy_engine, s3, id_gen,
        person_to_patient, already_migrated, limit,
    )
    r2 = migrate_healthcase_attached(
        new_engine, legacy_engine, s3, id_gen,
        person_to_patient, healthcase_to_visit, already_migrated, limit,
    )

    log.info(
        "======== TOTAL ========  inserted=%d | s3_fail=%d | errors=%d",
        r1["inserted"] + r2["inserted"],
        r1["s3_failures"] + r2["s3_failures"],
        r1["errors"] + r2["errors"],
    )


# ---------------------------------------------------------------------------
# Preview
# ---------------------------------------------------------------------------

def preview_documents(limit: int | None = None) -> None:
    if limit is None:
        limit = PREVIEW_SAMPLE_SIZE

    legacy_engine = get_legacy_engine()
    new_engine    = get_new_engine()

    person_to_patient   = load_person_id_to_patient_uuid(new_engine)
    healthcase_to_visit = load_healthcase_id_to_visit_uuid(new_engine)

    # Preview Source 1
    with legacy_engine.connect() as conn:
        bir_rows = conn.execute(_BIR_FETCH_LIMITED_SQL, {"lim": limit}).mappings().all()

    log.info("======== PREVIEW Source 1 (BIR) — %d rows ========", len(bir_rows))
    for i, row in enumerate(bir_rows, 1):
        person_id      = str(row.get("person_id", ""))
        new_patient_id = person_to_patient.get(person_id)
        img_urls       = [k.strip() for k in (row.get("reportimgurl") or "").split(";") if k.strip()]
        if not new_patient_id:
            log.warning("PREVIEW [BIR %d/%d] id=%s WOULD SKIP: no patient for person_id=%s",
                        i, len(bir_rows), row.get("id"), person_id)
            continue
        for idx, old_key in enumerate(img_urls):
            log.info("PREVIEW [BIR %d/%d] id=%s img=%d/%d  patient=%s\n"
                     "  old=%s/%s\n  new=%s/%s",
                     i, len(bir_rows), row.get("id"), idx+1, len(img_urls),
                     new_patient_id, OLD_S3_BUCKET, old_key,
                     NEW_S3_BUCKET, build_new_s3_key(new_patient_id, old_key))

    # Preview Source 2
    with legacy_engine.connect() as conn:
        hca_rows = conn.execute(_HCA_FETCH_LIMITED_SQL, {"lim": limit}).mappings().all()

    log.info("======== PREVIEW Source 2 (HCA) — %d rows ========", len(hca_rows))
    for i, row in enumerate(hca_rows, 1):
        person_id      = str(row.get("person_id", ""))
        new_patient_id = person_to_patient.get(person_id)
        healthcase_id  = str(row.get("healthcase_id", ""))
        new_visit_id   = healthcase_to_visit.get(healthcase_id)
        img_urls       = [k.strip() for k in (row.get("reportimgurl") or "").split(";") if k.strip()]
        old_key        = img_urls[0] if img_urls else None
        if not new_patient_id:
            log.warning("PREVIEW [HCA %d/%d] id=%s WOULD SKIP: no patient for person_id=%s",
                        i, len(hca_rows), row.get("id"), person_id)
            continue
        if not old_key:
            log.warning("PREVIEW [HCA %d/%d] id=%s WOULD SKIP: no S3 key", i, len(hca_rows), row.get("id"))
            continue
        log.info("PREVIEW [HCA %d/%d] id=%s  patient=%s  visit=%s  role=%s\n"
                 "  old=%s/%s\n  new=%s/%s",
                 i, len(hca_rows), row.get("id"),
                 new_patient_id, new_visit_id or "NULL", row.get("uploaderrole"),
                 OLD_S3_BUCKET, old_key,
                 NEW_S3_BUCKET, build_new_s3_key(new_patient_id, old_key))

    log.info("======== PREVIEW complete -- no rows written ========")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Migrate documents from BIR + HCA tables -> patientdocument (+ S3)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python migrate_documents.py              # migrate ALL
  python migrate_documents.py --limit 10   # first 10 rows per source table
  python migrate_documents.py --preview    # preview only, no writes
        """,
    )
    parser.add_argument("--limit",   type=int, default=None, metavar="N")
    parser.add_argument("--preview", action="store_true")
    args = parser.parse_args()

    preview_sample = args.limit if args.limit is not None else PREVIEW_SAMPLE_SIZE
    preview_documents(limit=preview_sample)

    if args.preview:
        log.info("--preview flag set -- exiting without migrating.")
        sys.exit(0)

    limit_label = str(args.limit) if args.limit is not None else "ALL"
    log.info(
        "Review the PREVIEW logs above. "
        "About to migrate %s document(s) from both source tables. Type 'yes' to proceed.",
        limit_label,
    )
    answer = input("Migrate to new DB? (yes/no): ").strip().lower()
    if answer != "yes":
        log.info("Migration cancelled -- no data written.")
        sys.exit(0)

    migrate_documents(limit=args.limit)