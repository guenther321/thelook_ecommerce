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
WEEKLY_TIERS = [
    ("tag:weekly_6m",   {"lookback_days": 180}),
    ("tag:weekly_12m",  {"lookback_days": 365}),
    ("tag:weekly_24m",  {"lookback_days": 730}),
]

PROFILE_CONFIG = ProfileConfig(
    profile_name="thelook",
    target_name="prod",
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
)


# ── shared helpers ─────────────────────────────────────────────────────────────
def _cosmos_task_group(selector: str, vars_: dict):
    return DbtTaskGroup(
        group_id=f"{selector.replace('tag:', '')}",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=PROFILE_CONFIG,
        render_config=RenderConfig(
            select=[selector],
            load_method=LoadMode.DBT_LS,
        ),
        operator_args={"vars": vars_},
    )


def _bash_task(selector: str, vars_: dict):
    return BashOperator(
        task_id=f"dbt_build_{selector.replace('tag:', '')}",
        bash_command=(
            f"dbt build "
            f"--select {selector} "
            f"--vars '{json.dumps(vars_)}' "
            f"--profiles-dir {DBT_PROJECT_PATH} "
            f"--target prod"
        ),
    )


# ── OPTION 1: BashOperator ─────────────────────────────────────────────────────
@dag(
    dag_id="dbt_weekly_build_bashoperator",
    description="Weekly build — single BashOperator task",
    schedule="0 4 * * 6",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "weekly", "BashOperator"],
)
def dbt_weekly_build_bashoperator():
    tasks = [_bash_task(sel, v) for sel, v in WEEKLY_TIERS]
    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]


# ── OPTION 3: Cosmos ───────────────────────────────────────────────────────────
@dag(
    dag_id="dbt_weekly_build_cosmos",
    description="[Option 3] Weekly build — Cosmos DbtTaskGroup per model",
    schedule="0 4 * * 6",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "weekly", "Cosmos"],
)
def dbt_weekly_build_cosmos():
    for sel, v in WEEKLY_TIERS:
        _cosmos_task_group(sel, v)


dbt_weekly_build_bashoperator()
dbt_weekly_build_cosmos()
