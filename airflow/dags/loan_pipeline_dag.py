from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.datasets import Dataset
default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": True,
    "email": ["dishasingla1969@gmail.com"],
}

with DAG(
    dag_id="loan_decision_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Core Data Warehouse Pipeline"
) as dag:

    dag.doc_md = """
    # Loan Decision Core Pipeline

    Steps:
    1. Raw ingestion
    2. Decision engine processing
    3. SCD2 fact updates
    4. MART build
    5. Data quality validation

    Architecture:
    CSV → Airflow → Postgres → Star Schema → MART → KPI → BI
    """

    raw_ingestion = BashOperator(
        task_id="raw_ingestion",
        bash_command="python /opt/project/ingestion/ingest_raw_loan_applications.py"
    )

    decision_engine = BashOperator(
        task_id="derive_decisions",
        bash_command="python /opt/project/ingestion/derive_decisions_from_raw.py",
        execution_timeout=timedelta(minutes=5)
    )

    mart_dataset = Dataset("mart_loan_decision_ready")
    mart_build = BashOperator(
        task_id="build_mart",
        bash_command="""
        export PGPASSWORD=de_password
        psql -h postgres -U de_user -d de_db \
        -f /opt/project/sql/mart/09_create_mart_loan_decision.sql
        """,
        outlets=[mart_dataset]
    )

    quality_check = BashOperator(
        task_id="data_quality_checks",
        bash_command="""
        export PGPASSWORD=de_password
        psql -h postgres -U de_user -d de_db \
        -f /opt/project/sql/quality/01_data_quality_checks.sql
        """
    )

    raw_ingestion >> decision_engine >> mart_build >> quality_check