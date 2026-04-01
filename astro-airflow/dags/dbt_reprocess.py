from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

DBT_PROJECT_PATH = Path("/usr/local/airflow/include/thelook_ecommerce")

DBT_CMD = (
    "dbt build"
    f" --project-dir {DBT_PROJECT_PATH}"
    f" --profiles-dir {DBT_PROJECT_PATH}"
    " --target prod"
    " --select {{ params.selector }}"
    " --threads {{ params.threads }}"
    ' --vars \'{"reprocess_start_date": "{{ params.start_date }}", "reprocess_end_date": "{{ params.end_date }}", "lookback_days": 0}\''
)


@dag(
    dag_id="dbt_reprocess",
    description="Surgical backfill — reprocess any selector over an explicit date window",
    schedule=None,        # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,    # no concurrent backfills on the same partitions
    default_args={"retries": 0},
    tags=["dbt", "backfill", "ops"],
    params={
        "selector":   Param("tag:daily_30d", type="string",  title="dbt selector — any valid dbt selector syntax"),
        "start_date": Param("2026-01-01",    type="string",  format="date", title="Start date (inclusive)"),
        "end_date":   Param("2026-02-01",    type="string",  format="date", title="End date (exclusive)"),
        "threads":    Param(4,               type="integer", minimum=1, maximum=32, title="dbt threads"),
    },
)
def dbt_reprocess():
    @task()
    def log_params(**context):
        p = context["params"]
        print("=" * 60)
        print(f"  selector  : {p['selector']}")
        print(f"  start_date: {p['start_date']}  (inclusive)")
        print(f"  end_date  : {p['end_date']}  (exclusive)")
        print(f"  threads   : {p['threads']}")
        print("=" * 60)

    preview = log_params()

    run = BashOperator(
        task_id="dbt_build_reprocess",
        bash_command=DBT_CMD,
    )

    preview >> run


dbt_reprocess()
