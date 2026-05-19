import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import ExternalPythonOperator
from airflow.operators.python import BranchPythonOperator

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

    templates = kwargs.get("templates_dict") or {}
    manual_year_str = templates.get("manual_year")
    ds = templates.get("exec_date")
    
    if manual_year_str and manual_year_str.strip():
        year = int(manual_year_str)
        log.info(f"[scrape] Manual Execution via UI Configuration - Explicitly requested year={year}")
        
    else:
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

def task_transform(**kwargs):
    import sys
    from pathlib import Path

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

    from main import run_transform
    from core import log

    log.info("[transform] Starting Twitter transformation")
    run_transform(platform="twitter")

    log.info("[transform] Starting Reddit transformation")
    run_transform(platform="reddit")

    log.info("[transform] Transformation pipeline finished")

def task_load(**kwargs):
    import sys
    from pathlib import Path

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

    from main import run_load
    from core import log

    log.info("[load] Starting Twitter load to OLAP Schema")
    run_load(platform="twitter")

    log.info("[load] Starting Reddit load to OLAP Schema")
    run_load(platform="reddit")

    log.info("[load] Load pipeline finished")
    

def decide_branch(**kwargs):
    templates = kwargs.get("templates_dict") or {}
    val = templates.get("skip_scrape")
    
    if val is True or str(val).lower() == 'true':
        return 'transform'
    return 'scrape'

with DAG(
    dag_id="socioeconomic_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for socioeconomic data - one run per scrape year",
    schedule="0 1 * * *",
    start_date=datetime(2026, 5, 19),  # Today's date
    catchup=False,
    max_active_runs=2,
    tags=["socioeconomic", "scrape", "etl", "raw-staging"],
) as dag:
    branch_task = BranchPythonOperator(
        task_id="decide_branch",
        templates_dict={
            "skip_scrape": "{{ dag_run.conf.get('skip_scrape', False) }}"
        },
        python_callable=decide_branch,
    )

    t_scrape = ExternalPythonOperator(
        task_id="scrape",
        python=VENV_PYTHON,
        python_callable=task_scrape,
        templates_dict={
            "exec_date": "{{ ds }}",
            "manual_year": "{{ dag_run.conf.get('year', '') if dag_run and dag_run.conf else '' }}"
        },
        expect_airflow=False,
    )

    t_transform = ExternalPythonOperator(
        task_id="transform",
        python=VENV_PYTHON,
        python_callable=task_transform, 
        expect_airflow=False,
        trigger_rule='none_failed_min_one_success'
    )

    t_load = ExternalPythonOperator(
        task_id="load",
        python=VENV_PYTHON,
        python_callable=task_load,
        expect_airflow=False,
    )

    # Set the pipeline flow
    branch_task >> [t_scrape, t_transform]
    t_scrape >> t_transform >> t_load