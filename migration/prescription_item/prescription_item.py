import json
import sys
import os
import argparse
from typing import Any
import re


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from sqlalchemy import text
from sqlalchemy.orm import Session

from config.db import get_legacy_engine, get_new_engine
from config.config import BATCH_SIZE, DRY_RUN, PREVIEW_SAMPLE_SIZE
from utils.id_gen import SafeIDGenerator
from utils.logger import get_logger


log = get_logger("migrate_prescription_item")

LEGACY_SOURCE_TABLE = "BodyVitals_prescription"
TARGET_APPDETAILS_ID: int = 42

_FETCH_SQL = text("""
    SELECT
        p.id           AS legacy_pres_id,
        p.prescription AS medicine_json
    FROM "BodyVitals_prescription" p
    WHERE p.healthcase_id IN (
        SELECT id FROM "HealthCase_healthcase"
        WHERE appdetails_id = :target_appdetails_id
    )
      AND p.prescription IS NOT NULL
      AND TRIM(p.prescription::text) NOT IN ('', '[]', 'null')
    ORDER BY p.id ASC
""")

_FETCH_LIMITED_SQL = text("""
    SELECT
        p.id           AS legacy_pres_id,
        p.prescription AS medicine_json
    FROM "BodyVitals_prescription" p
    WHERE p.healthcase_id IN (
        SELECT id FROM "HealthCase_healthcase"
        WHERE appdetails_id = :target_appdetails_id
    )
      AND p.prescription IS NOT NULL
      AND TRIM(p.prescription::text) NOT IN ('', '[]', 'null')
    ORDER BY p.id ASC
    LIMIT :lim
""")

INSERT_SQL = text("""
    INSERT INTO prescriptionitem (
        id,
        prescription_id,
        medication_name,
        dosage,
        frequency,
        duration,
        quantity,
        unit,
        notes,
        formulary_id,
        created_at
    ) VALUES (
        :id,
        :prescription_id,
        :medication_name,
        :dosage,
        :frequency,
        :duration,
        :quantity,
        :unit,
        :notes,
        :formulary_id,
        :created_at
    )
    ON CONFLICT (id) DO NOTHING
""")


def load_legacy_pres_id_to_new_uuid(new_engine: Any) -> dict[str, dict]:
    with new_engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT legacy_id, id::text, created_at
                FROM prescription
                WHERE legacy_id IS NOT NULL
                  AND legacy_source = 'digiswasthya_database'
            """)
        ).fetchall()
    return {
        row[0]: {"id": row[1], "created_at": row[2]}
        for row in rows
    }


def build_dosage(morning: Any, afternoon: Any, evening: Any) -> str:
    def flag(v: Any) -> str:
        return "1" if v else "0"
    return f"{flag(morning)}-{flag(afternoon)}-{flag(evening)}"


def parse_medicine_json(
    medicine_json: Any,
    new_prescription_id: str,
    prescription_created_at: Any,
    id_gen: SafeIDGenerator,
) -> list[dict]:
    if not medicine_json:
        return []

    raw = str(medicine_json).strip()
    if not raw or raw in ("[]", "null"):
        return []

    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        log.warning(
            "Could not parse medicine JSON for prescription_id=%s: %s",
            new_prescription_id, raw[:120],
        )
        return []

    if not isinstance(parsed, list):
        log.warning(
            "Medicine JSON is not a list for prescription_id=%s — skipping",
            new_prescription_id,
        )
        return []

    _UNIT_RE = re.compile(
        r'(\d+(?:\.\d+)?\s*(?:mg|mcg|ug|ml|g|tsf|tsp|iu|mmol))(?:\b|$)'
        r'|(\d+(?:\.\d+)?\s*%)',
        re.IGNORECASE,
    )

    _DOSING_KW_RE = re.compile(
        r'\b(bbf|b\.b\.f|bf|b\.f|af|a\.f|bd|b\.d|od|tds|hs|sos|stat|prn)\b',
        re.IGNORECASE,
    )

    _DURATION_RE = re.compile(
        r'(\d+)\s*(day|days|week|weeks|month|months)',
        re.IGNORECASE,
    )

    _TRAILING_INSTR_RE = re.compile(
        r'\s+(?:\d+(?:st|nd|rd|th)\s+day\b|into\b|from\b|without\s+gap\b|for\s+\d).*$',
        re.IGNORECASE,
    )

    _EMBEDDED_DOSAGE_RE = re.compile(r'\s+\d-\d-\d\s*$')

    def _norm_duration(m: re.Match | None) -> str | None:
        if m is None:
            return None
        num_str, word = m.group(1), m.group(2).lower()
        try:
            num = int(num_str)
        except ValueError:
            num = 2
        base = word.rstrip("s")
        unit_word = base if num == 1 else base + "s"
        return f"{num_str} {unit_word}"

    def _unit_from_match(m: re.Match | None) -> str:
        if m is None:
            return ""
        raw_match = (m.group(1) or m.group(2) or "").strip()
        return re.sub(r'(\d)(mg|mcg|ug|ml|g|tsf|tsp|iu|mmol)\b', r'\1 \2', raw_match, flags=re.IGNORECASE)

    items = []
    for entry in parsed:
        if not isinstance(entry, dict):
            continue

        medicine_name = (entry.get("medicine") or "").strip()
        if not medicine_name:
            continue

        instructions = (entry.get("instructions") or "").strip() or None

        dosage = build_dosage(
            entry.get("morning"),
            entry.get("afternoon"),
            entry.get("evening"),
        )

        clean_name = _TRAILING_INSTR_RE.sub("", medicine_name).strip()
        clean_name = _EMBEDDED_DOSAGE_RE.sub("", clean_name).strip()

        unit = ""
        unit_match = _UNIT_RE.search(clean_name)
        if unit_match:
            unit = _unit_from_match(unit_match)

        med_lower = medicine_name.lower()
        if re.search(r'\bbbf\b|\bb\.b\.f\b', med_lower):
            frequency = "Before Food"
        elif re.search(r'(?<![a-z])bf(?![a-z])|b\.f\.?', med_lower):
            frequency = "Before Food"
        elif re.search(r'(?<![a-z])af(?![a-z])|a\.f\.?', med_lower):
            frequency = "After Food"
        elif any(kw in med_lower for kw in ("cream", "ointment", "lotion", "gel", "local application")):
            frequency = "Apply to affected area"
        else:
            frequency = ""

        duration = _norm_duration(
            _DURATION_RE.search(clean_name)
            or (_DURATION_RE.search(instructions) if instructions else None)
        )

        if unit_match:
            matched_raw = (unit_match.group(1) or unit_match.group(2) or "").strip()
            clean_name = re.sub(
                r'\s*' + re.escape(matched_raw),
                "",
                clean_name,
                flags=re.IGNORECASE,
            )
        clean_name = _DOSING_KW_RE.sub("", clean_name)
        clean_name = re.sub(r'\s{2,}', " ", clean_name).strip()

        items.append({
            "id":              id_gen.next(),
            "prescription_id": new_prescription_id,
            "medication_name": clean_name,
            "dosage":          dosage,
            "frequency":       frequency,
            "duration":        duration,
            "quantity":        None,
            "unit":            unit or None,
            "notes":           instructions,
            "formulary_id":    None,
            "created_at":      prescription_created_at,
        })

    return items


def _flush_batch(new_engine: Any, batch: list[dict]) -> None:
    if DRY_RUN:
        return

    with Session(new_engine) as session:
        session.execute(INSERT_SQL, batch)
        session.commit()


def migrate_prescription_items(limit: int | None = None) -> None:
    legacy_engine = get_legacy_engine()
    new_engine    = get_new_engine()

    legacy_to_new = load_legacy_pres_id_to_new_uuid(new_engine)

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

    with new_engine.connect() as conn:
        already_done: set[str] = set(
            row[0] for row in conn.execute(
                text("SELECT DISTINCT prescription_id::text FROM prescriptionitem")
            ).fetchall()
        )

    id_gen = SafeIDGenerator(new_engine, table="prescriptionitem")

    batch: list[dict] = []
    prescriptions_processed = prescriptions_skipped = 0
    items_inserted = items_skipped_blank = errors = 0

    for row in rows:
        legacy_pres_id = str(row["legacy_pres_id"])
        pres_info      = legacy_to_new.get(legacy_pres_id)

        if pres_info is None:
            prescriptions_skipped += 1
            continue

        new_prescription_id     = pres_info["id"]
        prescription_created_at = pres_info["created_at"]

        if new_prescription_id in already_done:
            prescriptions_skipped += 1
            continue

        try:
            items = parse_medicine_json(
                row["medicine_json"],
                new_prescription_id,
                prescription_created_at,
                id_gen,
            )

            if not items:
                items_skipped_blank += 1
                continue

            prescriptions_processed += 1
            batch.extend(items)

            if len(batch) >= BATCH_SIZE:
                _flush_batch(new_engine, batch)
                items_inserted += len(batch)
                batch.clear()

        except Exception as e:
            log.error("ERROR processing legacy_pres_id=%s: %s", legacy_pres_id, e)
            errors += 1
            continue

    if batch:
        _flush_batch(new_engine, batch)
        items_inserted += len(batch)

    log.info(
        "═══ PrescriptionItem migration complete (appdetails_id=%d) ═══  "
        "limit=%s | prescriptions_fetched=%d | prescriptions_processed=%d | "
        "skipped(re-run+no-match)=%d | skipped(blank-json)=%d | "
        "items_inserted=%d | errors=%d",
        TARGET_APPDETAILS_ID,
        limit if limit is not None else "ALL",
        len(rows),
        prescriptions_processed,
        prescriptions_skipped,
        items_skipped_blank,
        items_inserted,
        errors,
    )


def preview_prescription_items(limit: int | None = None) -> None:
    if limit is None:
        limit = PREVIEW_SAMPLE_SIZE

    legacy_engine = get_legacy_engine()
    new_engine    = get_new_engine()

    with legacy_engine.connect() as conn:
        rows = conn.execute(
            _FETCH_LIMITED_SQL,
            {"target_appdetails_id": TARGET_APPDETAILS_ID, "lim": limit},
        ).mappings().all()

    legacy_to_new = load_legacy_pres_id_to_new_uuid(new_engine)
    id_gen = SafeIDGenerator(new_engine, table="prescriptionitem")

    for i, row in enumerate(rows, start=1):
        legacy_pres_id = str(row["legacy_pres_id"])
        pres_info      = legacy_to_new.get(legacy_pres_id)

        if pres_info is None:
            continue

        new_prescription_id     = pres_info["id"]
        prescription_created_at = pres_info["created_at"]

        items = parse_medicine_json(
            row["medicine_json"],
            new_prescription_id,
            prescription_created_at,
            id_gen,
        )

        if not items:
            continue

    log.info("════════ PREVIEW complete — no rows written to new DB ════════")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            f"Migrate prescription medicine items for appdetails_id={TARGET_APPDETAILS_ID} "
            "(DS-TMC-009 / Narsala) from legacy DB to prescriptionitem in new DB."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python migrate_prescription_item_appid42.py              # migrate ALL
  python migrate_prescription_item_appid42.py --limit 10   # migrate first 10 prescriptions
  python migrate_prescription_item_appid42.py --preview    # preview only, no DB writes

Prerequisites (run in order):
  1. migrate_patient_appid42.py
  2. migrate_visit_appid42.py
  3. migrate_prescription_appid42.py       ← must run before this
  4. migrate_prescription_item_appid42.py  ← this script
        """,
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Process only the first N legacy prescriptions. Omit to process ALL.",
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Run preview only — no rows are written to the new DB.",
    )
    args = parser.parse_args()

    preview_sample = args.limit if args.limit is not None else PREVIEW_SAMPLE_SIZE
    preview_prescription_items(limit=preview_sample)

    if args.preview:
        sys.exit(0)

    limit_label = str(args.limit) if args.limit is not None else "ALL"
    answer = input(f"Migrate {limit_label} prescriptionitem(s) for appdetails_id={TARGET_APPDETAILS_ID}? (yes/no): ").strip().lower()
    if answer != "yes":
        sys.exit(0)

    migrate_prescription_items(limit=args.limit)