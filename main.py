from datetime import datetime, timezone
import pandas as pd
from core import config, log, supabase

def run_scrape_upload(since: str, until: str) -> int:
    """Scrape and upload all data within the given window.

    Args:
        since: Window start, e.g. "2026-01-01"
        until: Window end,   e.g. "2027-01-01"
    """

    from src.scraper import scrap_nitter, scrape_reddit
    from src.utils.upload import upload_raw_tweets, upload_raw_reddit

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

def run_transform(platform: str, batch_size: int = 1000):
    from src.preprocess import processing_text
    from src.models.sentiment import predict_sentiment_batch
    from src.models.topic import predict_topic

    while True:
        response = supabase.rpc('get_unprocessed_raw', {
            'p_platform': platform, 
            'p_limit': batch_size
        })

        if not response.data:
            break

        df = pd.DataFrame(response.data)
        log.info(f"Processing {len(df)} records for {platform}")

        # Preprocess text
        df['processed_text'] = df['text_content'].apply(processing_text)

        # Inference
        sentiment_res = pd.DataFrame(predict_sentiment_batch(df['processed_text']))
        topic_res = pd.DataFrame(predict_topic(df['processed_text']))

        # Align and join results
        df = df.reset_index(drop=True)
        df = pd.concat([df, sentiment_res.reset_index(drop=True), topic_res.reset_index(drop=True)], axis=1)

        columns_to_include = [
            'id', 'source_type', 'text_content', 'posted_at',
            'sentiment_label', 'sentiment_score', 'topic_label', 'topic_category',
            'comment_count', 'like_count', 'retweet_count', 'quote_count',
            'subreddit', 'username', 'score', 'upvote_count', 'downvote_count', 'permalink'
        ]

        # Select and convert to records
        records = df[columns_to_include].to_dict(orient='records')
        
        for r in records:
            r['source_type'] = 'twitter' if platform == 'twitter' else 'reddit'
        supabase.table("staging_transformed").insert(records).execute()
        
        log.info(f"Staged {len(records)} records to staging_transformed")

def run_load(platform: str, batch_size: int = 1000):
    response = supabase.table("staging_transformed").select("*").eq("source_type", platform).limit(batch_size).execute()
    
    if not response.data:
        log.info(f"No staging data found for {platform}. Load complete.")
        return
    
    df = pd.DataFrame(response.data)
    ids = df['id'].tolist()

    def fetch_dimension_maps():
        # Fetch platforms: mapping (platform_name, channel) -> platform_id
        plat_res = supabase.table("dim_platform").select("platform_id, platform_name, channel").execute()
        plat_map = {(r['platform_name'], r['channel']): r['platform_id'] for r in plat_res.data}
        
        # Fetch topics: mapping topic_label -> topic_id
        topic_res = supabase.table("dim_topic").select("topic_id, topic_label").execute()
        topic_map = {r['topic_label']: r['topic_id'] for r in topic_res.data}

        time_res = supabase.table("dim_time").select("time_id, full_date").execute()
        time_map = {r['full_date']: r['time_id'] for r in time_res.data}
        
        return plat_map, topic_map, time_map
    
    plat_map, topic_map, time_map = fetch_dimension_maps()
    
    records_to_insert = []

    unique_sentiments = df[['sentiment_label', 'sentiment_score']].drop_duplicates()
    sentiment_id_map = {}
    for _, s in unique_sentiments.iterrows():
        key = (s['sentiment_label'], round(float(s['sentiment_score']), 4))
        res = supabase.rpc('get_sentiment_id', {'p_label': key[0], 'p_score': key[1]})
        sentiment_id_map[key] = res.data[0] if res.data else None

    # Map data for Fact Table
    for _, row in df.iterrows():
        # Resolve Time ID (Lookup by date)
        time_id = time_map.get(row['posted_at'][:10])
        
        # Resolve Platform ID
        channel = 'X_global' if platform == 'twitter' else (row['subreddit'] or 'unknown')
        plat_key = ('X' if platform == 'twitter' else 'Reddit', channel)
        platform_id = plat_map.get(plat_key)
        
        # Resolve Sentiment ID
        sentiment_key = (row['sentiment_label'], round(float(row['sentiment_score']), 4))
        sentiment_id = sentiment_id_map.get(sentiment_key)
        
        # Resolve Topic ID
        topic_id = topic_map.get(row['topic_label'])
        
        # Build Fact Row
        records_to_insert.append({
            'source_id': row['id'],
            'time_id': time_id,
            'platform_id': platform_id,
            'topic_id': topic_id,
            'sentiment_id': sentiment_id,
            'like_count': row['like_count'],
            'comment_count': row['comment_count'],
            'quote_count': row['quote_count'],
            'retweet_count': row['retweet_count'],
            'upvote_count': row['upvote_count'],
            'downvote_count': row['downvote_count'],
            'sentiment_score': float(row['sentiment_score']),
            'posted_at': row['posted_at']
        })

    # Insert into fact_post
    if records_to_insert:
        supabase.table("fact_post").insert(records_to_insert).execute()
        
        # Cleanup Staging Table
        supabase.table("staging_transformed").delete().in_("id", ids).execute()
        
        # Mark Raw as processed
        rpc_platform = 'tweets' if platform == 'twitter' else 'reddit'
        supabase.rpc('mark_as_processed', {
            'p_platform': rpc_platform,
            'p_ids': ids
        })
        
        log.info(f"Successfully loaded {len(records_to_insert)} records for {platform}")

    # Refresh views
    supabase.rpc('refresh_all_views')

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