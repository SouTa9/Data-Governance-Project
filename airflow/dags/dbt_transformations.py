"""
DBT Transformation Pipeline
Uses dbt build command


This DAG:
1. dbt build --select silver (builds silver models + runs tests)
2. dbt snapshot (captures SCD Type 2 history from silver layer)
3. dbt build --select gold (builds gold models + runs tests)
4. dbt docs generate

"""

from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

# dbt project path inside Docker container
# (mounted from ./dbt via docker-compose.yml)
DBT_PROJECT_DIR = "/opt/airflow/dbt"


@dag(
    dag_id="dbt_build_pipeline",
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
    tags=["dbt", "build", "snapshots", "silver", "gold", "scd2"],
    description="dbt pipeline with silver → snapshots → gold transformations",
    default_args={
        "owner": "data_engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    max_active_runs=1,
    doc_md=__doc__,
)
def dbt_build_dag():
    """
    Professional dbt pipeline using 'dbt build' command

    Pipeline Flow:
    1. Silver - Clean and standardize bronze data
    2. Snapshots - Capture SCD Type 2 history (reads from silver)
    3. Gold - Build star schema dimensions (reads from snapshots) and facts
    4. Docs - Generate dbt documentation

    Why dbt build?
    - Builds models AND runs tests in one command
    - Handles dependencies automatically
    """

    # Task 1: Build Silver layer (models + tests)
    build_silver = BashOperator(
        task_id="dbt_build_silver",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt build --select silver --profiles-dir {DBT_PROJECT_DIR}",
    )

    # Task 2: Run Snapshots (SCD Type 2 history tracking)
    # Snapshots read from silver models
    run_snapshots = BashOperator(
        task_id="dbt_snapshots",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt snapshot --profiles-dir {DBT_PROJECT_DIR}",
    )

    # Task 3: Build Gold layer (models + tests)
    # Gold dimensions read from snapshots for SCD2 history
    build_gold = BashOperator(
        task_id="dbt_build_gold",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt build --select gold --profiles-dir {DBT_PROJECT_DIR}",
    )

    # Task 4: Generate documentation
    generate_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt docs generate --profiles-dir {DBT_PROJECT_DIR}",
    )

    # Execution order: Silver → Snapshots → Gold → Docs
    build_silver >> run_snapshots >> build_gold >> generate_docs


# Instantiate the DAG
dbt_build_dag()
