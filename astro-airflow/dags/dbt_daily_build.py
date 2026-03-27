"""
DAG: dbt_daily_build
--------------------
Runs the thelook dbt pipeline daily at 6am UTC.

- Weekdays: 30-day lookback (fast incremental refresh)
- Saturdays: 365-day lookback (deep weekly backfill to catch late-arriving data)
- Runs every day at 6am UTC

Uses Cosmos to render each dbt model as its own Airflow task, giving
full visibility into model-level success/failure in the Airflow UI.
"""

from datetime import datetime
from pathlib import Path

from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping
from cosmos.constants import LoadMode

# ── paths ──────────────────────────────────────────────────────────────────────
DBT_PROJECT_PATH = Path("/usr/local/airflow/dbt/thelook_ecommerce")
DBT_PROFILES_PATH = DBT_PROJECT_PATH  # profiles.yml lives in project root

# ── lookback logic ─────────────────────────────────────────────────────────────
def get_lookback_days() -> int:
    """Return 365 on Saturdays for deep backfill, 30 otherwise."""
    return 365 if datetime.now().weekday() == 5 else 30


# ── DAG ────────────────────────────────────────────────────────────────────────
@dag(
    dag_id="dbt_daily_build",
    description="Daily dbt build for thelook eCommerce pipeline",
    schedule_interval="0 6 * * *",      # 6am UTC, every day
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dbt", "thelook"],
)
def dbt_daily_build():

    lookback_days = get_lookback_days()

    # ── intermediates (incremental, run first) ──────────────────────────────
    intermediates = DbtTaskGroup(
        group_id="intermediates",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=ProfileConfig(
            profile_name="thelook",
            target_name="prod",
            profiles_yml_filepath=DBT_PROFILES_PATH / "profiles.yml",
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path="/usr/local/airflow/.venv/bin/dbt",
        ),
        render_config=RenderConfig(
            select=["int_order_items_enriched", "int_orders_enriched"],
            load_method=LoadMode.DBT_LS,
        ),
        operator_args={
            "vars": {"lookback_days": lookback_days},
        },
    )

    # ── marts (table, run after intermediates) ──────────────────────────────
    marts = DbtTaskGroup(
        group_id="marts",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=ProfileConfig(
            profile_name="thelook",
            target_name="prod",
            profiles_yml_filepath=DBT_PROFILES_PATH / "profiles.yml",
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path="/usr/local/airflow/.venv/bin/dbt",
        ),
        render_config=RenderConfig(
            select=["orders"],
            load_method=LoadMode.DBT_LS,
        ),
    )

    # ── dependencies ────────────────────────────────────────────────────────
    intermediates >> marts


dbt_daily_build()
