import json
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.constants import LoadMode

DBT_PROJECT_PATH = Path("/usr/local/airflow/include/thelook_ecommerce")

# Lookback tiers — models opt-in by tag; each tier runs as a separate pass
# Adding a new model to a tier = add the tag to its yml, DAG never changes
DAILY_TIERS = [
    ("tag:daily_1d",   {"lookback_days":  1}),
    ("tag:daily_3d",   {"lookback_days":  3}),
    ("tag:daily_7d",   {"lookback_days":  7}),
    ("tag:daily_30d",  {"lookback_days": 30}),
    ("tag:daily_60d",  {"lookback_days": 60}),
]

PROFILE_CONFIG = ProfileConfig(
    profile_name="thelook",
    target_name="prod",
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
)


# ── OPTION 1: BashOperator — one black-box task ────────────────────────────────
@dag(
    dag_id="dbt_daily_build_bashoperator",
    description="Daily build — single BashOperator task",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "daily", "BashOperator"],
)
def dbt_daily_build_bashoperator():
    tasks = [
        BashOperator(
            task_id=f"dbt_build_{selector.replace('tag:', '')}",
            bash_command=(
                f"dbt build "
                f"--select {selector} "
                f"--vars '{json.dumps(vars_)}' "
                f"--profiles-dir {DBT_PROJECT_PATH} "
                f"--target prod"
            ),
        )
        for selector, vars_ in DAILY_TIERS
    ]
    # tiers run sequentially: 1d → 3d → 7d → 30d → 60d
    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]


# ── OPTION 3: Cosmos — one task per model, full lineage in UI ─────────────────
@dag(
    dag_id="dbt_daily_build_cosmos",
    description="Daily build — Cosmos DbtTaskGroup per model",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "daily", "Cosmos"],
)
def dbt_daily_build_cosmos():
    for selector, vars_ in DAILY_TIERS:
        DbtTaskGroup(
            group_id=f"{selector.replace('tag:', '')}",
            project_config=ProjectConfig(DBT_PROJECT_PATH),
            profile_config=PROFILE_CONFIG,
            render_config=RenderConfig(
                select=[selector],
                load_method=LoadMode.DBT_LS,
            ),
            operator_args={"vars": vars_},
        )


dbt_daily_build_bashoperator()
dbt_daily_build_cosmos()