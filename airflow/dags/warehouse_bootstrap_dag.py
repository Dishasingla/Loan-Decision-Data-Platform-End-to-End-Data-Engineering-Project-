from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="warehouse_bootstrap",
    start_date=datetime(2024,1,1),
    schedule=None,   # MANUAL ONLY
    catchup=False,
    description="Initial warehouse schema setup"
) as dag:

    init = BashOperator(
        task_id="init_schema",
        bash_command="""
        export PGPASSWORD=de_password
        psql -h postgres -U de_user -d de_db \
        -f /opt/project/sql/bootstrap/init_warehouse.sql
        """
    )