from datetime import datetime, timezone

import numpy as np
import pandas as pd

from core import config, supabase, log
from src.scraper import scrap_nitter, scrape_reddit
from src.preprocess import processing_text
from src.upload import upload_raw_tweets, upload_raw_reddit

def run_scrape() -> tuple[list[dict], list[dict]]:
    new_tweets = [
        {**tweet, "source_type": "twitter"}
        for search in config.scrape_config["nitter"]
        if search.get("query")
        for tweet in scrap_nitter(
            search_query=search["query"],
            depth=search.get("depth") or -1,
            time_budget=search.get("time_budget") or -1,
        )
    ]
    log.info(f"[scrape] tweets found: {len(new_tweets)}")

    new_reddit = [
        {**post, "source_type": "reddit"}
        for search in config.scrape_config["reddit"]
        if search.get("query")
        for post in scrape_reddit(
            search_query=search["query"],
            depth=search.get("depth") or -1,
            time_budget=search.get("time_budget") or -1,
        )
    ]
    log.info(f"[scrape] reddit posts found: {len(new_reddit)}")

    return new_tweets, new_reddit


def run_preprocess_and_upload(new_tweets: list[dict], new_reddit: list[dict]) -> int:
    if not new_tweets and not new_reddit:
        log.info("Nothing to process — exiting early.")
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
    log.info(f"Combined records before filter: {len(full_df)}")

    def _source_text(row):
        if row.get("source_type") == "reddit":
            return str(row.get("title") or "") + " " + str(row.get("body") or "")
        return str(row.get("text_content") or "")

    full_df["processed_text"] = full_df.apply(_source_text, axis=1).apply(processing_text)
    full_df["processed_text"] = full_df["processed_text"].replace(r"^\s*$", np.nan, regex=True)
    full_df.dropna(subset=["processed_text"], inplace=True)
    log.info(f"Records after filter: {len(full_df)}")

    upload_raw_tweets(full_df)
    upload_raw_reddit(full_df)

    return len(full_df)


if __name__ == "__main__":
    utc_now = datetime.now(timezone.utc)
    supabase.table("app_config").upsert(
        {"key": "last-updated", "value": utc_now.isoformat()},
        on_conflict="key",
    ).execute()

    tweets, reddit = run_scrape()
    len_full_df = run_preprocess_and_upload(tweets, reddit)

    log.info(f"Processed {len_full_df} records in {datetime.now(timezone.utc) - utc_now}")