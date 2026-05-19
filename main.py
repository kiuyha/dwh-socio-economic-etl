from datetime import datetime, date, timedelta, timezone

import numpy as np
import pandas as pd

from core import config, supabase, log
from src.scraper import scrap_nitter, scrape_reddit
from src.upload import upload_raw_tweets, upload_raw_reddit

SCRAPE_YEARS = [2023, 2024, 2025, 2026]
DAG_BASE_DATE = date(2023, 1, 1)
MANUAL_BASE_DATE = date(2026, 5, 19)

def resolve_scrape_year(exec_date: str | None) -> int:
    """Map an execution date (or today) to a scrape year.

    Airflow:  exec_date="2023-01-01" → 2023
              exec_date="2023-01-02" → 2024  ...
    Manual:   today (2026-05-19)     → 2023
              tomorrow (2026-05-20)  → 2024  ...

    The index is clamped so it never goes out of range.
    """
    if exec_date:
        d = datetime.strptime(exec_date, "%Y-%m-%d").date()
        idx = (d - DAG_BASE_DATE).days
    else:
        d = datetime.now(timezone.utc).date()
        idx = (d - MANUAL_BASE_DATE).days

    idx = max(0, min(idx, len(SCRAPE_YEARS) - 1))
    year = SCRAPE_YEARS[idx]
    log.info(f"Resolved scrape year: {year}  (slot index={idx}, source={'airflow' if exec_date else 'manual'})")
    return year


def run_scrape_upload(exec_date: str | None = None) -> int:
    year = resolve_scrape_year(exec_date)

    # Build a full-year date filter — Twitter syntax: since/until
    since = f"{year}-01-01"
    until = f"{year + 1}-01-01"
    date_filter = f" since:{since} until:{until}"
    log.info(f"Targeting full year window: {since} → {until}")

    new_tweets = []
    for search in config.scrape_config.get("nitter", []):
        if search.get("query"):
            dynamic_query = search["query"] + date_filter
            new_tweets.extend(
                scrap_nitter(
                    search_query=dynamic_query,
                    depth=search.get("depth") or -1,
                    time_budget=search.get("time_budget") or -1,
                )
            )
    log.info(f"[scrape] tweets found: {len(new_tweets)}")

    new_reddit = []
    for search in config.scrape_config.get("reddit", []):
        if search.get("query"):
            new_reddit.extend(
                scrape_reddit(
                    search_query=search["query"],
                    subreddit=search.get("subreddit"),
                    depth=search.get("depth") or -1,
                    time_budget=search.get("time_budget") or -1,
                )
            )
    log.info(f"[scrape] reddit posts found: {len(new_reddit)}")

    if not new_tweets and not new_reddit:
        log.info("Nothing to upload — exiting early.")
        return 0

    df_tweets = (
        pd.DataFrame(new_tweets).drop_duplicates(subset=["id"], keep="last")
        if new_tweets else pd.DataFrame()
    )
    df_reddit = (
        pd.DataFrame(new_reddit).drop_duplicates(subset=["id"], keep="last")
        if new_reddit else pd.DataFrame()
    )

    full_df = pd.concat(
        [d for d in (df_tweets, df_reddit) if not d.empty],
        ignore_index=True,
    )
    log.info(f"Total records to upload: {len(full_df)}")

    upload_raw_tweets(full_df)
    upload_raw_reddit(full_df)

    return len(full_df)


if __name__ == "__main__":
    utc_now = datetime.now(timezone.utc)
    supabase.table("app_config").upsert(
        {"key": "last-updated", "value": utc_now.isoformat()},
        on_conflict="key",
    ).execute()

    # exec_date=None → uses today relative to MANUAL_BASE_DATE
    total = run_scrape_upload(exec_date=None)
    log.info(f"Uploaded {total} raw records in {datetime.now(timezone.utc) - utc_now}")