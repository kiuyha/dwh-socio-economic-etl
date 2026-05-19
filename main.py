from datetime import datetime, timezone

import pandas as pd

from core import config, supabase, log
from src.scraper import scrap_nitter, scrape_reddit
from src.upload import upload_raw_tweets, upload_raw_reddit


def run_scrape_upload(since: str, until: str) -> int:
    """Scrape and upload all data within the given window.

    Args:
        since: Window start, e.g. "2026-01-01"
        until: Window end,   e.g. "2027-01-01"
    """
    date_filter = f" since:{since} until:{until}"
    log.info(f"Targeting window: {since} → {until}")

    new_tweets = [
        {**tweet, 'source_type': 'twitter'}
        
        for search in config.scrape_config['nitter']
        if search.get('query')
        for tweet in scrap_nitter(
            search_query=search['query'] + date_filter,
            depth=search.get('depth') or -1,
            time_budget=search.get('time_budget') or -1
        )
    ]
    log.info(f"[scrape] tweets found: {len(new_tweets)}")

    new_reddit = [
        {**comment, 'source_type': 'reddit'}
        for search in config.scrape_config['reddit']
        if search.get('query')
        for comment in scrape_reddit(
            search_query=search['query'],
            subreddit=search.get("subreddit"),
            since_date_str=since,
            until_date_str=until,
            depth=search.get('depth') or -1,
            time_budget=search.get('time_budget') or -1
        )
    ]
    
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
    import argparse

    parser = argparse.ArgumentParser(description="Scrape a specific year window manually.")
    parser.add_argument("--since", required=True, help="e.g. '2026-01-01'")
    parser.add_argument("--until", required=True, help="e.g. '2027-01-01'")
    args = parser.parse_args()

    utc_now = datetime.now(timezone.utc)
    supabase.table("app_config").upsert(
        {"key": "last-updated", "value": utc_now.isoformat()},
        on_conflict="key",
    ).execute()

    total = run_scrape_upload(since=args.since, until=args.until)
    log.info(f"Uploaded {total} raw records in {datetime.now(timezone.utc) - utc_now}")