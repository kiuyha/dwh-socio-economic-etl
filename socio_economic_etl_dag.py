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
    "retries": 2,
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
    from main import run_scrape

    utc_now = datetime.now(timezone.utc)
    supabase.table("app_config").upsert(
        {"key": "last-updated", "value": utc_now.isoformat()},
        on_conflict="key",
    ).execute()

    tweets, reddit = run_scrape()

    log.info(f"[scrape] done — tweets: {len(tweets)}, reddit: {len(reddit)}")
    return {"tweets": tweets, "reddit": reddit}

def task_preprocess_and_upload(exec_date: str, **context):
    import sys
    from pathlib import Path

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

    from core import log
    from main import run_preprocess_and_upload

    ti = context["ti"]
    scrape_result = ti.xcom_pull(task_ids="scrape")
    tweets = scrape_result["tweets"]
    reddit = scrape_result["reddit"]

    total = run_preprocess_and_upload(tweets, reddit)

    log.info(f"[preprocess_upload] processed {total} records")
    return total

with DAG(
    dag_id="socioeconomic_etl_pipeline",
    default_args=default_args,
    description="Daily scrape + preprocess/upload for socio-economic ETL",
    schedule="0 1 * * *",
    start_date=datetime(2026, 5, 19),
    catchup=False,
    tags=["socioeconomic", "scrape", "etl", "raw-staging"],
) as dag:

    t1 = ExternalPythonOperator(
        task_id="scrape",
        python=VENV_PYTHON,
        python_callable=task_scrape,
        op_kwargs={"exec_date": "{{ ds }}"},
        expect_airflow=False,
    )

    t2 = ExternalPythonOperator(
        task_id="preprocess_and_upload",
        python=VENV_PYTHON,
        python_callable=task_preprocess_and_upload,
        op_kwargs={"exec_date": "{{ ds }}"},
        expect_airflow=False,
    )

    t1 >> t2