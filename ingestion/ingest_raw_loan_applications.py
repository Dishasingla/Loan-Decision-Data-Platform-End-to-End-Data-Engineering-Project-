import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch

# -----------------------------------
# Configuration
# -----------------------------------
CSV_PATH = "/opt/project/data/loan_applications.csv"

DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "database": "de_db",
    "user": "de_user",
    "password": "de_password",
}

# -----------------------------------
# Helper functions (STRICT RAW)
# -----------------------------------
def safe_int(x):
    if pd.isna(x):
        return None
    return int(x)

def safe_float(x):
    if pd.isna(x):
        return None
    return float(x)

# -----------------------------------
# Read CSV (RAW ingestion)
# -----------------------------------
df = pd.read_csv(CSV_PATH)
df.columns = df.columns.str.strip()

print(f"📄 Rows read from CSV: {len(df)}")

# -----------------------------------
# Prepare SQL INSERT
# -----------------------------------
insert_sql = """
INSERT INTO raw_loan_applications (
    loan_id,
    gender,
    married,
    dependents,
    education,
    self_employed,
    applicant_income,
    coapplicant_income,
    loan_amount,
    loan_amount_term,
    credit_history,
    property_area
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

# -----------------------------------
# Build records (STRICT, NaN-safe)
# -----------------------------------
records = []

for row in df.itertuples(index=False):
    records.append((
        row.Loan_ID,
        row.Gender,
        row.Married,
        row.Dependents,
        row.Education,
        row.Self_Employed,
        safe_int(row.ApplicantIncome),
        safe_int(row.CoapplicantIncome),
        safe_float(row.LoanAmount),
        safe_int(row.Loan_Amount_Term),
        safe_int(row.Credit_History),
        row.Property_Area,
    ))

# -----------------------------------
# Batch insert with STOP & LOG
# -----------------------------------
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

try:
    execute_batch(cursor, insert_sql, records)
    conn.commit()
    print(f"✅ Successfully ingested {len(records)} raw rows")

except Exception as e:
    conn.rollback()
    print("❌ RAW INGESTION FAILED")
    print(f"➡ Rows attempted: {len(records)}")
    print("➡ Error:", str(e))
    raise

finally:
    cursor.close()
    conn.close()
