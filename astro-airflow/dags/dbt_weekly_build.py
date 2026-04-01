from datetime import datetime
from pathlib import Path

from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.constants import LoadMode

DBT_PROJECT_PATH = Path("/usr/local/airflow/include/thelook_ecommerce")

# Lookback tiers — models opt-in by tag; each tier runs as a separate pass
# Adding a new model to a tier = add the tag to its yml, DAG never changes
WEEKLY_TIERS = [
    ("tag:weekly",      {"lookback_days":   0}),  # full rebuilds — table materialisation, no lookback
    ("tag:weekly_6m",   {"lookback_days": 180}),
    ("tag:weekly_12m",  {"lookback_days": 365}),
    ("tag:weekly_24m",  {"lookback_days": 730}),
]

PROFILE_CONFIG = ProfileConfig(
    profile_name="thelook",
    target_name="prod",
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
)


MANIFEST_PATH = DBT_PROJECT_PATH / "target" / "manifest.json"


def _make_tier_groups(domain: str | None = None):
    """Build one DbtTaskGroup per tier, optionally scoped to a domain tag."""
    for selector, vars_ in WEEKLY_TIERS:
        select = f"{selector},tag:domain:{domain}" if domain else selector
        DbtTaskGroup(
            group_id=f"{selector.replace('tag:', '')}",
            project_config=ProjectConfig(DBT_PROJECT_PATH, manifest_path=MANIFEST_PATH),
            profile_config=PROFILE_CONFIG,
            render_config=RenderConfig(
                select=[select],
                load_method=LoadMode.DBT_MANIFEST,
            ),
            operator_args={"vars": vars_},
        )


# ── All models — one DAG for everything ───────────────────────────────────────
@dag(
    dag_id="dbt_weekly_build",
    description="Weekly build — all models",
    schedule="0 4 * * 6",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "weekly"],
)
def dbt_weekly_build():
    _make_tier_groups()


# ── Domain: orders ─────────────────────────────────────────────────────────────
@dag(
    dag_id="dbt_weekly_orders",
    description="Weekly build — orders domain",
    schedule="0 4 * * 6",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "weekly", "domain:orders"],
)
def dbt_weekly_orders():
    _make_tier_groups(domain="orders")


# ── Domain: sessions ──────────────────────────────────────────────────────────
@dag(
    dag_id="dbt_weekly_sessions",
    description="Weekly build — sessions domain",
    schedule="0 4 * * 6",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "weekly", "domain:sessions"],
)
def dbt_weekly_sessions():
    _make_tier_groups(domain="sessions")


dbt_weekly_build()
dbt_weekly_orders()
dbt_weekly_sessions()
