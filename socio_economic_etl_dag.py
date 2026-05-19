import sys
from pathlib import Path
from datetime import datetime, timedelta
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
def task_scrape(exec_date: str):
    import sys
    from pathlib import Path
    from datetime import datetime, timezone

    candidates = [
        "/home/inter24/dags/dwh-socio-economic-etl",
        "/opt/airflow/dags/dwh-socio-economic-etl",
    ]
    try:
        dag_file = Path(__file__).resolve()
        candidates.insert(0, str(dag_file.parent / "dwh-socio-economic-etl"))
    except Exception:
        pass

    root = next((p for p in candidates if p and (Path(p) / "src").exists()), candidates[0])
    if root not in sys.path:
        sys.path.insert(0, root)

    from core import supabase, log
    from main import run_scrape_upload

    utc_now = datetime.now(timezone.utc)
    supabase.table("app_config").upsert(
        {"key": "last-updated", "value": utc_now.isoformat()},
        on_conflict="key",
    ).execute()

    # exec_date drives the year slot (see resolve_scrape_year in main.py)
    total = run_scrape_upload(exec_date=exec_date)

    log.info(f"[scrape] finished {total} records for exec_date={exec_date}")


with DAG(
    dag_id="socioeconomic_etl_pipeline",
    default_args=default_args,
    description="Yearly scrape ETL — one Airflow day = one full calendar year (2023-2026)",
    schedule="0 1 * * *",          # still daily, but only 4 runs total
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2023, 1, 4),  # inclusive; 4 runs
    catchup=True,                   # backfill all 4 slots
    max_active_runs=2,              # run at most 2 years concurrently
    tags=["socioeconomic", "scrape", "etl", "raw-staging"],
) as dag:

    t1 = ExternalPythonOperator(
        task_id="scrape",
        python=VENV_PYTHON,
        python_callable=task_scrape,
        op_kwargs={"exec_date": "{{ ds }}"},
        expect_airflow=False,
    )

    t1