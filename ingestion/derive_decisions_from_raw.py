import pandas as pd
import psycopg2

DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "database": "de_db",
    "user": "de_user",
    "password": "de_password",
}

RULE_ID = 1


def safe_int(x):
    if pd.isna(x):
        return None
    return int(x)


def safe_float(x):
    if pd.isna(x):
        return None
    return float(x)


# ---------------- CONNECT ----------------
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

# ---------------- STEP 1 — LOAD RAW ----------------
raw_df = pd.read_sql("""
SELECT loan_id, applicant_income, coapplicant_income,
       loan_amount, loan_amount_term,
       credit_history, property_area
FROM raw_loan_applications
""", conn)

raw_df = raw_df.where(pd.notnull(raw_df), None)

print("RAW rows:", len(raw_df))


# ---------------- STEP 2 — APPLY RULE ----------------
def apply_rule(row):
    if (
        row["credit_history"] == 1
        and row["applicant_income"] is not None
        and row["loan_amount"] is not None
        and row["loan_amount_term"] is not None
        and row["applicant_income"] >= 4000
        and row["loan_amount"] <= 250
        and row["loan_amount_term"] <= 360
    ):
        return "Y"
    return "N"


raw_df["loan_status"] = raw_df.apply(apply_rule, axis=1)


# ---------------- STEP 3 — FETCH CURRENT DECISIONS ----------------
latest_df = pd.read_sql("""
SELECT application_id, loan_status, decision_version
FROM fact_loan_decisions
WHERE is_current = TRUE
""", conn)

latest_map = {
    row.application_id: (row.loan_status, row.decision_version)
    for row in latest_df.itertuples()
}

records = []

for row in raw_df.itertuples(index=False):

    latest = latest_map.get(row.loan_id)

    if latest is None:
        new_version = 1
        should_insert = True
    else:
        last_status, last_version = latest
        if last_status == row.loan_status:
            should_insert = False
        else:
            should_insert = True
            new_version = last_version + 1

    if should_insert:
        records.append((
            row.loan_id,
            safe_float(row.applicant_income),
            safe_float(row.coapplicant_income),
            safe_float(row.loan_amount),
            safe_int(row.loan_amount_term),
            safe_int(row.credit_history),
            row.property_area,
            row.loan_status,
            new_version,
            RULE_ID,
        ))

print("New decisions to insert:", len(records))


# ---------------- STEP 4 — SCD2 INSERT ----------------
try:

    for record in records:

        application_id = record[0]

        # Close previous record
        cursor.execute("""
            UPDATE fact_loan_decisions
            SET is_current = FALSE,
                valid_to = NOW()
            WHERE application_id = %s
            AND is_current = TRUE
        """, (application_id,))

        # Insert new version
        cursor.execute("""
            INSERT INTO fact_loan_decisions(
                application_id,
                applicant_income,
                coapplicant_income,
                loan_amount,
                loan_term,
                credit_history,
                property_area,
                loan_status,
                decision_version,
                rule_id,
                valid_from,
                valid_to,
                is_current
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NULL,TRUE)
        """, record)

    conn.commit()
    print("Inserted SCD2 decisions:", len(records))

except Exception as e:
    conn.rollback()
    print("Decision pipeline failed:", e)
    raise

finally:
    cursor.close()
    conn.close()