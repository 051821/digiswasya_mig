# # import json
# # import sys
# # import os
# # import uuid
# # sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
# # from sqlalchemy.orm import Session, sessionmaker
# # from sqlalchemy import text
# # from config.db import get_legacy_engine, get_new_engine



# # engine = get_legacy_engine()
# # SessionLocal = sessionmaker(bind=engine)

# # # from sqlalchemy import text

# # # with SessionLocal() as session:

# # #     tables = session.execute(text("""
# # #         SELECT table_name
# # #         FROM information_schema.tables
# # #         WHERE table_schema = 'public'
# # #           AND table_name LIKE 'BodyVitals%';
# # #     """)).fetchall()

# # #     for (table_name,) in tables:
# # #         print(f"\n=== {table_name} ===")

# # #         columns = session.execute(text("""
# # #             SELECT column_name
# # #             FROM information_schema.columns
# # #             WHERE table_schema = 'public'
# # #               AND table_name = :table_name
# # #             ORDER BY ordinal_position;
# # #         """), {"table_name": table_name}).fetchall()

# # #         total_rows = session.execute(
# # #             text(f'SELECT COUNT(*) FROM "{table_name}"')
# # #         ).scalar()

# # #         print(f"Total Rows: {total_rows}")

# # #         for (column_name,) in columns:
# # #             cnt = session.execute(
# # #                 text(
# # #                     f'SELECT COUNT("{column_name}") FROM "{table_name}"'
# # #                 )
# # #             ).scalar()

# # #             print(f"{column_name}: {cnt}")


# # from sqlalchemy import text

# # with SessionLocal() as session:

# #     tables = session.execute(text("""
# #         SELECT 
# #     hc.id AS healthcase_id,
# #     c.commentid,
# #     c.datetime,
# #     c.commenterrole,
# #     c.commenttext
# # FROM "HealthCase_healthcase" hc
# # LEFT JOIN "HealthCase_commentshealthcase" c 
# #     ON c.healthcase_id = hc.id
# # WHERE hc.appdetails_id = 42
# #   AND c.commenttext IS NOT NULL
# #   AND TRIM(c.commenttext) <> ''
# # ORDER BY hc.id ASC, c.datetime DESC limit 5;
# #     """)).fetchall()


# #     print(tables)

# #     # for (table_name,) in tables:
# #     #     print(f"\n{'='*80}")
# #     #     print(f"TABLE: {table_name}")
# #     #     print(f"{'='*80}")

# #     #     rows = session.execute(
# #     #         text(f'SELECT * FROM "{table_name}" LIMIT 5')
# #     #     ).mappings().all()

# #     #     for row in rows:
# #     #         print(dict(row))


# import json
# import sys
# import os
# import uuid
# sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
# from sqlalchemy.orm import Session, sessionmaker
# from sqlalchemy import text
# from config.db import get_legacy_engine, get_new_engine



# engine = get_legacy_engine()
# SessionLocal = sessionmaker(bind=engine)

# from sqlalchemy import text

# with SessionLocal() as session:

#     tables = session.execute(text("""
# SELECT 
#     p.id,
#     p.prescriptionid,
#     p.prescription,
#     p.investigation,
#     p.clinicalnote,
#     p.diagnosis,
#     p.recommendation,
#     p.datetime,
#     p.followupdatetime,
#     p.appdetails_id,
#     p.doctor_id,
#     p.person_id,
#     p.healthcase_id
# FROM "BodyVitals_prescription" p
# WHERE p.healthcase_id IN (
#     SELECT id FROM "HealthCase_healthcase"
#     WHERE appdetails_id = 42
# )
# ORDER BY p.id ASC
# LIMIT 10;
#      """)).fetchall()

#     print(tables)

# #     for (table_name,) in tables:
# #         print(f"\n=== {table_name} ===")

# #         columns = session.execute(text("""
# #             SELECT column_name
# #             FROM information_schema.columns
# #             WHERE table_schema = 'public'
# #               AND table_name = :table_name
# #             ORDER BY ordinal_position;
# #         """), {"table_name": table_name}).fetchall()

# #         total_rows = session.execute(
# #             text(f'SELECT COUNT(*) FROM "{table_name}"')
# #         ).scalar()

# #         print(f"Total Rows: {total_rows}")

# #         for (column_name,) in columns:
# #             cnt = session.execute(
# #                 text(
# #                     f'SELECT COUNT("{column_name}") FROM "{table_name}"'
# #                 )
# #             ).scalar()

# #             print(f"{column_name}: {cnt}")


# # from sqlalchemy import text
# # from sqlalchemy.orm import sessionmaker

# # legacy_engine = get_legacy_engine()
# # new_engine = get_new_engine()

# # LegacySession = sessionmaker(bind=legacy_engine)


# # def normalize_phone(phone):
# #     if not phone:
# #         return ""
# #     return "".join(ch for ch in str(phone) if ch.isdigit())


# # with LegacySession() as session:
# #     old_doctors = session.execute(text("""
# #         SELECT DISTINCT
# #             d.doctorid AS old_doctor_id,
# #             d.name     AS doctor_name,
# #             d.phone    AS doctor_phone
# #         FROM "Doctor_doctordetails" d
# #         ORDER BY d.name
# #     """)).mappings().all()


# # with new_engine.connect() as conn:
# #     new_users = conn.execute(text("""
# #         SELECT
# #             id::text AS user_id,
# #             LOWER(TRIM(
# #                 COALESCE(first_name, '') || ' ' || COALESCE(last_name, '')
# #             )) AS full_name,
# #             phone,
# #             is_active
# #         FROM "user"
# #         WHERE role = 'DOCTOR'
# #     """)).mappings().all()


# # lookup = {}

# # for row in new_users:
# #     key = (
# #         (row["full_name"] or "").strip().lower(),
# #         normalize_phone(row["phone"])
# #     )
# #     lookup[key] = row


# # print(
# #     f"{'OLD_DOCTOR_ID':<15} "
# #     f"{'NAME':<40} "
# #     f"{'PHONE':<15} "
# #     f"{'NEW_USER_ID':<40} "
# #     f"{'ACTIVE'}"
# # )

# # print("-" * 130)

# # for doctor in old_doctors:
# #     key = (
# #         (doctor["doctor_name"] or "").strip().lower(),
# #         normalize_phone(doctor["doctor_phone"])
# #     )

# #     match = lookup.get(key)

# #     if match:
# #         print(
# #             f"{doctor['old_doctor_id']:<15} "
# #             f"{doctor['doctor_name']:<40} "
# #             f"{str(doctor['doctor_phone']):<15} "
# #             f"{match['user_id']:<40} "
# #             f"{match['is_active']}"
# #         )
# #     else:
# #         print(
# #             f"{doctor['old_doctor_id']:<15} "
# #             f"{doctor['doctor_name']:<40} "
# #             f"{str(doctor['doctor_phone']):<15} "
# #             f"{'NOT_FOUND':<40} "
# #             f"{'N/A'}"
# #         )


# # with new_engine.connect() as conn:
# #  rows = conn.execute(text("""
# #     SELECT
# #         id AS doctor_id,
# #         CONCAT(
# #             COALESCE(first_name, ''),
# #             CASE
# #                 WHEN first_name IS NOT NULL AND last_name IS NOT NULL THEN ' '
# #                 ELSE ''
# #             END,
# #             COALESCE(last_name, '')
# #         ) AS doctor_name,
# #         phone,
# #         is_active
# #     FROM "user"
# #     WHERE role = 'DOCTOR'
# #     ORDER BY doctor_name
# # """)).fetchall()

# # for row in rows:
# #     print(row)

# # import sys
# # import os

# # sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# # from sqlalchemy import text
# # from sqlalchemy.orm import sessionmaker

# # from config.db import get_legacy_engine, get_new_engine


# # legacy_engine = get_legacy_engine()
# # new_engine = get_new_engine()

# # LegacySession = sessionmaker(bind=legacy_engine)


# # def normalize_name(name):
    
# #     return " ".join((name or "").strip().lower().split())


# # def normalize_phone(phone):
# #     digits = "".join(ch for ch in str(phone or "") if ch.isdigit())

# #     # keep only last 10 digits
# #     if len(digits) >= 10:
# #         return digits[-10:]

# #     return digits


# # # --------------------------------------------------
# # # OLD DB DOCTORS
# # # --------------------------------------------------
# # with LegacySession() as session:
# #     old_doctors = session.execute(text("""
# #         SELECT DISTINCT
# #             d.doctorid,
# #             d.name,
# #             d.phone
# #         FROM "Doctor_doctordetails" d
# #         WHERE d.doctorid IS NOT NULL
# #     """)).mappings().all()


# # # --------------------------------------------------
# # # NEW DB DOCTORS
# # # --------------------------------------------------
# # with new_engine.connect() as conn:
# #     new_doctors = conn.execute(text("""
# #         SELECT
# #             id::text AS user_id,
# #             CONCAT(
# #                 COALESCE(first_name, ''),
# #                 CASE
# #                     WHEN first_name IS NOT NULL
# #                      AND last_name IS NOT NULL
# #                     THEN ' '
# #                     ELSE ''
# #                 END,
# #                 COALESCE(last_name, '')
# #             ) AS doctor_name,
# #             phone,
# #             is_active
# #         FROM "user"
# #         WHERE role = 'DOCTOR'
# #     """)).mappings().all()


# # # --------------------------------------------------
# # # BUILD LOOKUP
# # # --------------------------------------------------
# # lookup = {}

# # for row in new_doctors:
# #     key = (
# #         normalize_name(row["doctor_name"]),
# #         normalize_phone(row["phone"])
# #     )

# #     lookup[key] = {
# #         "user_id": row["user_id"],
# #         "is_active": row["is_active"]
# #     }


# # # --------------------------------------------------
# # # PRINT RESULTS
# # # --------------------------------------------------
# # import pandas as pd

# # rows = []

# # for doctor in old_doctors:
# #     key = (
# #         normalize_name(doctor["name"]),
# #         normalize_phone(doctor["phone"])
# #     )

# #     match = lookup.get(key)

# #     rows.append({
# #         "OLD_DOCTOR_ID": doctor["doctorid"],
# #         "NAME": doctor["name"],
# #         "PHONE": normalize_phone(doctor["phone"]),
# #         "NEW_USER_ID": match["user_id"] if match else "NOT_FOUND",
# #         "ACTIVE": match["is_active"] if match else None,
# #         "STATUS": "MATCHED" if match else "NOT_FOUND"
# #     })

# # df = pd.DataFrame(rows)

# # df.to_csv("doctor_mapping.csv", index=False)

# # print(df.head())
# # print(f"\nCSV saved: doctor_mapping.csv")
# # print(f"TOTAL OLD DOCTORS : {len(df)}")
# # print(f"TOTAL NOT FOUND   : {(df['STATUS'] == 'NOT_FOUND').sum()}")
# # print(f"TOTAL MATCHED     : {(df['STATUS'] == 'MATCHED').sum()}")


import json
import sys
import os
import uuid
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import text
from config.db import get_legacy_engine, get_new_engine
import pandas as pd

engine = get_legacy_engine()
SessionLocal = sessionmaker(bind=engine)


def normalize_name(name):
    return " ".join((name or "").strip().lower().split())


def normalize_phone(phone):
    digits = "".join(ch for ch in str(phone or "") if ch.isdigit())
    # keep only last 10 digits
    if len(digits) >= 10:
        return digits[-10:]
    return digits


# --------------------------------------------------
# OLD DB DOCTORS
# --------------------------------------------------
with SessionLocal() as session:
    old_doctors = session.execute(text("""
        SELECT DISTINCT
            d.doctorid,
            d.name,
            d.phone
        FROM "Doctor_doctordetails" d
        WHERE d.doctorid IS NOT NULL
    """)).mappings().all()


# --------------------------------------------------
# NEW DB DOCTORS
# --------------------------------------------------
new_engine = get_new_engine()

with new_engine.connect() as conn:
    new_doctors = conn.execute(text("""
        SELECT
            id::text AS user_id,
            CONCAT(
                COALESCE(first_name, ''),
                CASE
                    WHEN first_name IS NOT NULL
                     AND last_name IS NOT NULL
                    THEN ' '
                    ELSE ''
                END,
                COALESCE(last_name, '')
            ) AS doctor_name,
            phone,
            is_active
        FROM "user"
        WHERE role = 'DOCTOR'
    """)).mappings().all()


# --------------------------------------------------
# BUILD LOOKUPS
#   1. name+phone → user  (existing, precise match)
#   2. phone-only → user  (fallback for name mismatches)
# --------------------------------------------------
name_phone_lookup: dict[tuple[str, str], dict] = {}
phone_only_lookup: dict[str, dict] = {}

for row in new_doctors:
    norm_name  = normalize_name(row["doctor_name"])
    norm_phone = normalize_phone(row["phone"])

    entry = {
        "user_id":   row["user_id"],
        "is_active": row["is_active"],
    }

    # Primary lookup: name + phone together
    if norm_name or norm_phone:
        name_phone_lookup[(norm_name, norm_phone)] = entry

    # Secondary lookup: phone only (last one wins for duplicates — acceptable
    # because two distinct doctors sharing a phone number is very unlikely)
    if norm_phone:
        phone_only_lookup[norm_phone] = entry


# --------------------------------------------------
# MATCH
# --------------------------------------------------
rows = []

for doctor in old_doctors:
    norm_name  = normalize_name(doctor["name"])
    norm_phone = normalize_phone(doctor["phone"])

    # 1st attempt: name + phone
    match = name_phone_lookup.get((norm_name, norm_phone))
    match_method = "NAME+PHONE" if match else None

    # 2nd attempt: phone only (catches "Dr." prefix, swapped first/last name, etc.)
    if match is None and norm_phone:
        match = phone_only_lookup.get(norm_phone)
        match_method = "PHONE_ONLY" if match else None

    rows.append({
        "OLD_DOCTOR_ID": doctor["doctorid"],
        "NAME":          doctor["name"],
        "PHONE":         normalize_phone(doctor["phone"]),
        "NEW_USER_ID":   match["user_id"]   if match else "NOT_FOUND",
        "ACTIVE":        match["is_active"] if match else None,
        "STATUS":        "MATCHED"          if match else "NOT_FOUND",
        "MATCH_METHOD":  match_method       if match else "NOT_FOUND",
    })

df = pd.DataFrame(rows)

df.to_csv("doctor_mapping.csv", index=False)

print(df.to_string(index=False))
print(f"\nCSV saved: doctor_mapping.csv")
print(f"TOTAL OLD DOCTORS : {len(df)}")
print(f"TOTAL NOT FOUND   : {(df['STATUS'] == 'NOT_FOUND').sum()}")
print(f"TOTAL MATCHED     : {(df['STATUS'] == 'MATCHED').sum()}")
print(f"  → via NAME+PHONE : {(df['MATCH_METHOD'] == 'NAME+PHONE').sum()}")
print(f"  → via PHONE_ONLY : {(df['MATCH_METHOD'] == 'PHONE_ONLY').sum()}")