from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.datasets import Dataset

mart_dataset = Dataset("mart_loan_decision_ready")

with DAG(
    dag_id="kpi_refresh_pipeline",
    schedule=[mart_dataset],   # ⭐ event based trigger
    start_date=datetime(2024,1,1),
    catchup=False,
    description="KPI Materialized View Refresh"
) as dag:

    refresh_kpis = BashOperator(
        task_id="refresh_kpi_materialized_views",
        bash_command="""
        export PGPASSWORD=de_password
        psql -h postgres -U de_user -d de_db \
        -f /opt/project/sql/kpi_mart/refresh_kpis.sql
        """
    )