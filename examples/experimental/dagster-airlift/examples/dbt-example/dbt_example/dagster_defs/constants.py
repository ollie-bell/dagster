# Airflow instance running at localhost:8080
import os
from pathlib import Path

from dagster import AssetKey

AIRFLOW_BASE_URL = "http://localhost:8080"
AIRFLOW_INSTANCE_NAME = "my_airflow_instance"

# Authentication credentials (lol)
USERNAME = "admin"
PASSWORD = "admin"

ASSETS_PATH = Path(__file__).parent / "defs"
MIGRATION_STATE_PATH = Path(__file__).parent / "proxied"
DBT_DAG_ASSET_KEY = AssetKey(["airflow_instance", "dag", "dbt_dag"])


def dbt_project_path() -> Path:
    env_val = os.getenv("DBT_PROJECT_DIR")
    assert env_val
    return Path(env_val)


def dbt_manifest_path() -> Path:
    return dbt_project_path() / "target" / "manifest.json"
