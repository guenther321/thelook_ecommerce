from datetime import datetime
from pathlib import Path

from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.constants import LoadMode

DBT_PROJECT_PATH = Path("/usr/local/airflow/include/thelook_ecommerce")
MANIFEST_PATH = DBT_PROJECT_PATH / "target" / "manifest.json"

# ── Lookback tiers ────────────────────────────────────────────────────────────
DAILY_TIERS = [
    ("tag:daily",      {"lookback_days":  0}),
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


@dag(
    dag_id="dbt_daily_build",
    description="Daily build — all daily models across all tiers",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "daily"],
)
def dbt_daily_build():
    for selector, vars_ in DAILY_TIERS:
        DbtTaskGroup(
            group_id=selector.replace("tag:", ""),
            project_config=ProjectConfig(DBT_PROJECT_PATH, manifest_path=MANIFEST_PATH),
            profile_config=PROFILE_CONFIG,
            render_config=RenderConfig(
                select=[selector],
                load_method=LoadMode.DBT_MANIFEST,
            ),
            operator_args={"vars": vars_},
        )


dbt_daily_build()
