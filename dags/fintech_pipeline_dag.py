"""
End-to-end hybrid lakehouse pipeline (container side).

  extract_csv_to_bronze ─► spark_bronze_to_silver ─► gold_handoff (marker)

Airflow orchestrates the file-source bronze + the PySpark silver. The GOLD layer
(load silver -> SQL Server + dbt tests) runs on the WINDOWS HOST via run_gold.ps1,
because it uses Windows authentication to ahmed\\SQLEXPRESS, which a Linux
container can't do. The final task just signals that silver is ready for the host.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="fintech_lakehouse",
    description="8 CSV -> bronze -> silver (Spark); gold runs on host via run_gold.ps1",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["fintech", "medallion"],
) as dag:

    def _extract():
        import sys
        sys.path.insert(0, "/opt/airflow/ingestion")
        from extract_to_bronze import extract_csvs
        extract_csvs()

    extract_csv = PythonOperator(
        task_id="extract_csv_to_bronze",
        python_callable=_extract,
    )

    spark_silver = BashOperator(
        task_id="spark_bronze_to_silver",
        bash_command=(
            "spark-submit --driver-memory 4g "
            "--conf spark.local.dir=/opt/data/spark-tmp "
            "/opt/airflow/spark/bronze_to_silver.py"
        ),
    )

    gold_handoff = BashOperator(
        task_id="gold_handoff",
        bash_command=(
            'echo "Silver ready in /opt/data/lake/silver. '
            'Now run run_gold.ps1 on the Windows host to load gold + dbt test."'
        ),
    )

    extract_csv >> spark_silver >> gold_handoff
