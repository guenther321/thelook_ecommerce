from datetime import datetime
from pathlib import Path

from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.constants import LoadMode

DBT_PROJECT_PATH = Path("/usr/local/airflow/include/thelook_ecommerce")
MANIFEST_PATH = DBT_PROJECT_PATH / "target" / "manifest.json"

# ── Lookback tiers ────────────────────────────────────────────────────────────
WEEKLY_TIERS = [
    ("tag:weekly",      {"lookback_days":   0}),
    ("tag:weekly_6m",   {"lookback_days": 180}),
    ("tag:weekly_12m",  {"lookback_days": 365}),
    ("tag:weekly_24m",  {"lookback_days": 730}),
]

PROFILE_CONFIG = ProfileConfig(
    profile_name="thelook",
    target_name="prod",
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
)


@dag(
    dag_id="dbt_weekly_build",
    description="Weekly build — all weekly models across all tiers",
    schedule="0 4 * * 6",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "weekly"],
)
def dbt_weekly_build():
    for selector, vars_ in WEEKLY_TIERS:
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


dbt_weekly_build()
