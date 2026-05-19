"""
upload.py — schema-aligned upload helpers for the raw staging layer.

Imported by main.py; never run directly.
"""

from datetime import datetime, timezone

import numpy as np
import pandas as pd

from core import log, supabase

_TWEET_COLUMNS = [
    "id",
    "fullname",
    "username",
    "text_content",
    "posted_at",
    "like_count",
    "comment_count",
    "retweet_count",
    "quote_count",
    "scraped_at",
]

_REDDIT_COLUMNS = [
    "id",
    "username",
    "title",
    "body",
    "subreddit",
    "posted_at",
    "score",
    "upvote_count",
    "downvote_count",
    "upvote_ratio",
    "comment_count",
    "permalink",
    "scraped_at",
]

_TWEET_INT_COLS  = ["like_count", "comment_count", "retweet_count", "quote_count"]
_REDDIT_INT_COLS = ["score", "upvote_count", "downvote_count", "comment_count"]


def _coerce_int_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Fill NaN with 0 and cast to int for NOT NULL INTEGER columns."""
    for col in cols:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype(int)
    return df


def _prepare(df: pd.DataFrame, allowed_cols: list[str], int_cols: list[str]) -> list[dict]:
    """Whitelist columns, coerce types, and convert to records."""
    if "scraped_at" not in df.columns:
        df = df.copy()
        df["scraped_at"] = datetime.now(timezone.utc).isoformat()

    cols_present = [c for c in allowed_cols if c in df.columns]
    df = df[cols_present].copy()
    df = _coerce_int_cols(df, int_cols)

    return (
        df.astype(object)
        .replace({pd.NA: None, np.nan: None})
        .to_dict(orient="records")
    )


def upload_raw_tweets(df: pd.DataFrame) -> None:
    """Upload Twitter records to raw_tweets."""
    source_df = df[df["source_type"] == "twitter"].copy()

    if source_df.empty:
        log.info("No twitter records to upload. Skipping.")
        return

    records = _prepare(source_df, _TWEET_COLUMNS, _TWEET_INT_COLS)
    log.info(f"Uploading {len(records)} records → raw_tweets …")
    supabase.table("raw_tweets").upsert(records, on_conflict="id,posted_at").execute()
    log.info("raw_tweets upload complete.")


def upload_raw_reddit(df: pd.DataFrame) -> None:
    """Upload Reddit records to raw_reddit.

    text_content is excluded because it is a GENERATED ALWAYS column
    (COALESCE(title,'') || ' ' || COALESCE(body,'')).
    The scraper must supply title and/or body instead.
    """
    source_df = df[df["source_type"] == "reddit"].copy()

    if source_df.empty:
        log.info("No reddit records to upload. Skipping.")
        return

    records = _prepare(source_df, _REDDIT_COLUMNS, _REDDIT_INT_COLS)
    log.info(f"Uploading {len(records)} records → raw_reddit …")
    supabase.table("raw_reddit").upsert(records, on_conflict="id,posted_at").execute()
    log.info("raw_reddit upload complete.")
