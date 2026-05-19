import sys
from pathlib import Path
from datetime import datetime, date, timedelta, timezone
from airflow import DAG
from airflow.providers.standard.operators.python import ExternalPythonOperator

_FALLBACK_PATHS = [
    "/home/inter24/dags/dwh-socio-economic-etl",
    "/opt/airflow/dags/dwh-socio-economic-etl",
]
PROJECT_ROOT = _FALLBACK_PATHS[0]

try:
    _candidates = [
        str(Path(__file__).parent / "dwh-socio-economic-etl"),
        str(Path(__file__).parent.parent),
        str(Path(__file__).parent),
    ] + _FALLBACK_PATHS
except Exception:
    _candidates = _FALLBACK_PATHS

for _p in _candidates:
    if _p and (Path(_p) / "src").exists():
        PROJECT_ROOT = _p
        break

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

VENV_PYTHON = "/opt/airflow/.venv/bin/python"

default_args = {
    "owner": "socioeconomic-dwh",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def task_scrape(**kwargs):
    import sys
    from pathlib import Path
    from datetime import date, datetime, timezone

    candidates = [
        "/home/inter24/dags/dwh-socio-economic-etl",
        "/opt/airflow/dags/dwh-socio-economic-etl",
    ]
    try:
        candidates.insert(0, str(Path(__file__).resolve().parent / "dwh-socio-economic-etl"))
    except Exception:
        pass

    root = next((p for p in candidates if p and (Path(p) / "src").exists()), candidates[0])
    if root not in sys.path:
        sys.path.insert(0, root)

    from core import supabase, log
    from main import run_scrape_upload

    SCRAPE_YEARS = [2026, 2025, 2024, 2023]
    DAG_BASE_DATE = date(2023, 1, 1)

    # 1. Safely check if this is an explicit manual trigger via UI Configuration JSON
    dag_run = kwargs.get("dag_run")
    conf = getattr(dag_run, "conf", {}) or {}
    
    if conf and "year" in conf:
        year = int(conf["year"])
        log.info(f"[scrape] Manual Execution via UI Configuration - Explicitly requested year={year}")
        
    # 2. Fall back to automatic scheduled backfill date tracking
    else:
        ds = (kwargs.get("templates_dict") or {}).get("exec_date")
        d = date.fromisoformat(ds) if ds else DAG_BASE_DATE
        
        idx = (d - DAG_BASE_DATE).days
        idx = max(0, min(idx, len(SCRAPE_YEARS) - 1))
        year = SCRAPE_YEARS[idx]
        log.info(f"[scrape] Scheduled Automated Execution - ds={ds}, slot={idx}, targeted year={year}")

    since = f"{year}-01-01"
    until = f"{year + 1}-01-01"

    utc_now = datetime.now(timezone.utc)
    supabase.table("app_config").upsert(
        {"key": "last-updated", "value": utc_now.isoformat()},
        on_conflict="key",
    ).execute()

    total = run_scrape_upload(since=since, until=until)
    log.info(f"[scrape] finished {total} records - year={year} ({since} -> {until})")


with DAG(
    dag_id="socioeconomic_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for socioeconomic data - one run per scrape year",
    schedule="0 1 * * *",
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2023, 1, 4),
    catchup=True,
    max_active_runs=2,
    tags=["socioeconomic", "scrape", "etl", "raw-staging"],
) as dag:

    t1 = ExternalPythonOperator(
        task_id="scrape",
        python=VENV_PYTHON,
        python_callable=task_scrape,
        templates_dict={"exec_date": "{{ ds }}"},
        expect_airflow=False,
    )

    t1