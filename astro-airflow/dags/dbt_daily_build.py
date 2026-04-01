from datetime import datetime
from pathlib import Path

from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.constants import LoadMode

DBT_PROJECT_PATH = Path("/usr/local/airflow/include/thelook_ecommerce")

# Lookback tiers — models opt-in by tag; each tier runs as a separate pass
# Adding a new model to a tier = add the tag to its yml, DAG never changes
DAILY_TIERS = [
    ("tag:daily",      {"lookback_days":  0}),  # full rebuilds — table materialisation, no lookback
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


MANIFEST_PATH = DBT_PROJECT_PATH / "target" / "manifest.json"


def _make_tier_groups(domain: str | None = None):
    """Build one DbtTaskGroup per tier, optionally scoped to a domain tag."""
    for selector, vars_ in DAILY_TIERS:
        # intersect tier tag with domain tag if provided
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
    dag_id="dbt_daily_build",
    description="Daily build — all models",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "daily"],
)
def dbt_daily_build():
    _make_tier_groups()


# ── Domain: orders ─────────────────────────────────────────────────────────────
@dag(
    dag_id="dbt_daily_orders",
    description="Daily build — orders domain",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "daily", "domain:orders"],
)
def dbt_daily_orders():
    _make_tier_groups(domain="orders")


# ── Domain: sessions ──────────────────────────────────────────────────────────
@dag(
    dag_id="dbt_daily_sessions",
    description="Daily build — sessions domain",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "daily", "domain:sessions"],
)
def dbt_daily_sessions():
    _make_tier_groups(domain="sessions")


dbt_daily_build()
dbt_daily_orders()
dbt_daily_sessions()
