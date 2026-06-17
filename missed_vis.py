import sys
import os
import argparse
import importlib.util


_THIS_DIR       = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT   = _THIS_DIR
_MIGRATION_DIR  = os.path.join(_THIS_DIR, "migration")

sys.path.insert(0, _PROJECT_ROOT)

from sqlalchemy import text

from config.db import get_legacy_engine, get_new_engine
from utils.logger import get_logger




def _load_module(module_name: str, file_path: str):
    if not os.path.isfile(file_path):
        raise FileNotFoundError(
            f"Expected migration module not found at: {file_path}\n"
            "Check _MIGRATION_DIR / folder names at the top of this script."
        )
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_visit_mod = _load_module(
    "backfill_visit_mod", os.path.join(_MIGRATION_DIR, "visit", "visit.py")
)
_prescription_mod = _load_module(
    "backfill_prescription_mod", os.path.join(_MIGRATION_DIR, "prescription", "prescription.py")
)
_prescription_item_mod = _load_module(
    "backfill_prescription_item_mod", os.path.join(_MIGRATION_DIR, "prescription_item", "prescription_item.py")
)

migrate_visit               = _visit_mod.migrate_visit
migrate_prescription        = _prescription_mod.migrate_prescription
migrate_prescription_items  = _prescription_item_mod.migrate_prescription_items


log = get_logger("backfill_appid42")

TARGET_APPDETAILS_ID: int = 42

_FETCH_ELIGIBLE_PERSON_IDS_SQL = text("""
    SELECT DISTINCT hc.person_id
    FROM "HealthCase_healthcase" hc
    WHERE hc.appdetails_id = :target_appdetails_id
      AND hc.person_id IS NOT NULL
""")



def find_patients_without_visit(legacy_engine, new_engine) -> list[dict]:
    with legacy_engine.connect() as conn:
        person_ids = [
            str(r[0]) for r in conn.execute(
                _FETCH_ELIGIBLE_PERSON_IDS_SQL,
                {"target_appdetails_id": TARGET_APPDETAILS_ID},
            ).fetchall()
        ]

    log.info(
        "Found %d distinct person_id(s) with healthcases under appdetails_id=%d",
        len(person_ids), TARGET_APPDETAILS_ID,
    )

    with new_engine.connect() as conn:
        patient_rows = conn.execute(
            text("SELECT legacy_id, id::text, full_name FROM patient WHERE legacy_id IS NOT NULL")
        ).fetchall()
    legacy_to_patient = {r[0]: {"id": r[1], "full_name": r[2]} for r in patient_rows}

    with new_engine.connect() as conn:
        visited_patient_ids = set(
            r[0] for r in conn.execute(
                text("SELECT DISTINCT patient_id::text FROM visit")
            ).fetchall()
        )

    no_visit_patients: list[dict] = []
    for pid in person_ids:
        info = legacy_to_patient.get(pid)
        if info is None:
            # Patient was never migrated at all — out of scope for this script.
            continue
        if info["id"] not in visited_patient_ids:
            no_visit_patients.append({
                "legacy_person_id": pid,
                "patient_id": info["id"],
                "full_name": info["full_name"],
            })

    return no_visit_patients


def run_backfill(limit: int | None = None) -> None:
    legacy_engine = get_legacy_engine()
    new_engine    = get_new_engine()

    no_visit_patients = find_patients_without_visit(legacy_engine, new_engine)

    log.info(
        "════════ Patients with NO visit yet in new DB (appdetails_id=%d) ════════",
        TARGET_APPDETAILS_ID,
    )
    for p in no_visit_patients:
        log.info(
            "  patient_id=%s legacy_person_id=%s full_name=%s",
            p["patient_id"], p["legacy_person_id"], p["full_name"],
        )
    log.info("Total patients without a visit: %d", len(no_visit_patients))

    if not no_visit_patients:
        log.info("Nothing to backfill — every patient already has a visit. Exiting.")
        return

    with new_engine.connect() as conn:
        visit_before = conn.execute(text("SELECT COUNT(*) FROM visit")).scalar()
        pres_before  = conn.execute(text("SELECT COUNT(*) FROM prescription")).scalar()
        item_before  = conn.execute(text("SELECT COUNT(*) FROM prescriptionitem")).scalar()

    log.info("Running visit migration (will pick up previously doctor-skipped rows)...")
    migrate_visit(limit=limit)

    log.info("Running prescription migration...")
    migrate_prescription(limit=limit)

    log.info("Running prescription_item migration...")
    migrate_prescription_items(limit=limit)

    with new_engine.connect() as conn:
        visit_after = conn.execute(text("SELECT COUNT(*) FROM visit")).scalar()
        pres_after  = conn.execute(text("SELECT COUNT(*) FROM prescription")).scalar()
        item_after  = conn.execute(text("SELECT COUNT(*) FROM prescriptionitem")).scalar()

    log.info(
        "═══ Backfill complete (appdetails_id=%d) ═══  "
        "patients_without_visit_found=%d | "
        "new_visits_added=%d | new_prescriptions_added=%d | new_prescription_items_added=%d",
        TARGET_APPDETAILS_ID,
        len(no_visit_patients),
        visit_after - visit_before,
        pres_after - pres_before,
        item_after - item_before,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            f"Backfill visit/prescription/prescription_item for appdetails_id={TARGET_APPDETAILS_ID} "
            "(DS-TMC-009 / Narsala) for patients that have NO visit yet in the new DB — "
            "now that previously-missing doctors have been added."
        ),
        epilog="""
Examples:
  python migrate_backfill_appid42.py            # backfill ALL eligible
  python migrate_backfill_appid42.py --limit 10 # pass-through limit to each step
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Pass-through limit to each underlying migration step. Omit to run ALL.",
    )
    args = parser.parse_args()

    legacy_engine = get_legacy_engine()
    new_engine    = get_new_engine()
    preview_patients = find_patients_without_visit(legacy_engine, new_engine)

    log.info("%d patient(s) currently have no visit in new DB.", len(preview_patients))
    for p in preview_patients:
        log.info(
            "  patient_id=%s legacy_person_id=%s full_name=%s",
            p["patient_id"], p["legacy_person_id"], p["full_name"],
        )

    if not preview_patients:
        log.info("Nothing to backfill — every patient already has a visit. Exiting.")
        sys.exit(0)

    answer = input(
        f"Proceed with backfill for {len(preview_patients)} patient(s) "
        f"under appdetails_id={TARGET_APPDETAILS_ID}? (yes/no): "
    ).strip().lower()
    if answer != "yes":
        sys.exit(0)

    run_backfill(limit=args.limit)