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

    offset = 0
    
    # Standardize RPC platform name
    rpc_platform = 'tweets' if platform == 'twitter' else 'reddit'

    while True:
        response = supabase.rpc('get_unprocessed_raw', {
            'p_platform': rpc_platform, 
            'p_limit': batch_size,
            'p_offset': offset
        })

        if not response.data:
            break

        df = pd.DataFrame(response.data)
        
        if 'extra' in df.columns:
            extra_df = pd.json_normalize(df['extra'])
            df = pd.concat([df.drop(columns=['extra']), extra_df], axis=1)

        df['source_type'] = platform
        log.info(f"Processing {len(df)} records for {platform}")

        # Preprocess text
        df['processed_text'] = df['text_content'].apply(processing_text)

        # Inference
        sentiment_res = pd.DataFrame(predict_sentiment_batch(df['processed_text']))
        sentiment_res = sentiment_res.rename(columns={
            "label": "sentiment_label", 
            "confidence": "sentiment_score"
        })
        
        # Rename topic output columns to match the database schema
        topic_res = pd.DataFrame(predict_topic(df['processed_text']))
        topic_res = topic_res.rename(columns={
            "label": "topic_label", 
            "category": "topic_category"
        })

        # Align and join results
        df = df.reset_index(drop=True)
        df = pd.concat([df, sentiment_res.reset_index(drop=True), topic_res.reset_index(drop=True)], axis=1)

        # Shared columns between both platforms
        base_columns = [
            'id', 'source_type', 'text_content', 'posted_at',
            'sentiment_label', 'sentiment_score', 'topic_label', 'topic_category', 
            'username'
        ]

        # Check against standard 'twitter' string
        if platform == 'twitter':
            columns_to_include = base_columns + [
                'comment_count', 'like_count', 'retweet_count', 'quote_count'
            ]
        elif platform == 'reddit':
            columns_to_include = base_columns + [
                'subreddit', 'score', 'upvote_count', 'downvote_count', 'permalink'
            ]
        else:
            raise ValueError(f"Unsupported platform: {platform}")

        # Select and convert to records
        records = df[columns_to_include].to_dict(orient='records')
        
        # Ensure source_type is mapped properly
        for r in records:
            r['source_type'] = platform 
            
        supabase.table("staging_transformed").insert(records).execute()
        
        log.info(f"Staged {len(records)} records to staging_transformed")
        offset += len(df)

def run_load(platform: str, batch_size: int = 1000):
    log.info(f"Initializing load phase for {platform}...")
    
    # 1. Fetch dimensions ONCE before the loop to save API calls
    def fetch_dimension_maps():
        # Fetch platforms
        plat_res = supabase.table("dim_platform").select("platform_id, platform_name, channel").execute()
        plat_map = {(r['platform_name'], r['channel']): r['platform_id'] for r in plat_res.data}
        
        # Fetch topics
        topic_res = supabase.table("dim_topic").select("topic_id, topic_label").execute()
        topic_map = {r['topic_label']: r['topic_id'] for r in topic_res.data}

        # Fetch time (Paginated to bypass the 1,000 row hard limit)
        time_data = []
        offset = 0
        limit = 1000
        while True:
            t_res = supabase.table("dim_time").select("time_id, full_date").limit(limit).offset(offset).execute()
            if not t_res.data:
                break
            
            time_data.extend(t_res.data)
            
            # If we received fewer rows than the limit, we've hit the end of the table
            if len(t_res.data) < limit:
                break
                
            offset += limit
            
        time_map = {r['full_date']: r['time_id'] for r in time_data}
        
        # Fetch sentiments
        sent_res = supabase.table("dim_sentiment").select("sentiment_id, sentiment_label, confidence_bucket").execute()
        sentiment_map = {(r['sentiment_label'], r['confidence_bucket']): r['sentiment_id'] for r in sent_res.data}
        
        return plat_map, topic_map, time_map, sentiment_map
    
    # Unpack the new map
    plat_map, topic_map, time_map, sentiment_map = fetch_dimension_maps()

    log.info(f"[{platform}] Dimension maps loaded successfully.")

    total_loaded = 0
    batch_num = 1

    # 2. Start the processing queue
    while True:
        log.info(f"[{platform}] Fetching batch {batch_num}...")
        
        # No offset needed: we delete rows at the end, so we always pull from the top
        response = supabase.table("staging_transformed").select("*").eq("source_type", platform).limit(batch_size).execute()
        
        if not response.data:
            log.info(f"[{platform}] No more staging data found. Emptying queue complete.")
            break
        
        df = pd.DataFrame(response.data)
        ids = df['id'].tolist()
        
        records_to_insert = []

        # Map data for Fact Table
        for _, row in df.iterrows():
            date_str = str(row['posted_at'])[:10]
            time_id = time_map.get(date_str)
            
            channel = 'X_global' if platform == 'twitter' else (f"r/{row['subreddit']}" or 'unknown')
            plat_key = ('X' if platform == 'twitter' else 'Reddit', channel)
            platform_id = plat_map.get(plat_key)
            
            topic_id = topic_map.get(row['topic_label'])
            
            # --- NEW: Local sentiment bucketing ---
            score = float(row['sentiment_score'])
            if score >= 0.80:
                bucket = 'High'
            elif score >= 0.50:
                bucket = 'Medium'
            else:
                bucket = 'Low'
                
            sentiment_id = sentiment_map.get((row['sentiment_label'], bucket))
            # --------------------------------------

            # --- SAFETY CHECKS ---
            if time_id is None:
                log.warning(f"Skipped {row['id']}: Date {date_str} not found in dim_time.")
                continue
                
            if platform_id is None:
                log.warning(f"Skipped {row['id']}: Channel '{channel}' not found in dim_platform.")
                continue

            # --- CONSTRUCT SOURCE URL ---
            if platform == 'twitter':
                username = row.get('username', 'unknown')
                source_url = f"https://x.com/{username}/status/{row['id']}"
            else:
                permalink = str(row.get('permalink', ''))
                if permalink.startswith('/'):
                    source_url = f"https://www.reddit.com{permalink}"
                else:
                    source_url = permalink
            
            # Build Fact Row
            records_to_insert.append({
                'source_id': row['id'],
                'source_url': source_url,
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
                'sentiment_score': score,
                'posted_at': row['posted_at']
            })

        # Insert and cleanup
        if records_to_insert:
            supabase.table("fact_post").insert(records_to_insert).execute()
            
            supabase.table("staging_transformed").delete().in_("id", ids).execute()
            
            rpc_platform = 'tweets' if platform == 'twitter' else 'reddit'
            supabase.rpc('mark_as_processed', {
                'p_platform': rpc_platform,
                'p_ids': ids
            })
            
            loaded_count = len(records_to_insert)
            total_loaded += loaded_count
            log.info(f"[{platform}] Batch {batch_num} success: Loaded {loaded_count} records. (Total: {total_loaded})")
            
        batch_num += 1

    # 3. Refresh views only once after all batches are finished
    if total_loaded > 0:
        log.info(f"[{platform}] Refreshing materialized views...")
        supabase.rpc('refresh_all_views')
        log.info(f"[{platform}] Load phase complete. {total_loaded} total records processed.")

if __name__ == "__main__":
    # import argparse

    # parser = argparse.ArgumentParser(description="Scrape a specific year window manually.")
    # parser.add_argument("--since", required=True, help="e.g. '2026-01-01'")
    # parser.add_argument("--until", required=True, help="e.g. '2027-01-01'")
    # args = parser.parse_args()

    # utc_now = datetime.now(timezone.utc)
    # supabase.table("app_config").upsert(
    #     {"key": "last-updated", "value": utc_now.isoformat()},
    #     on_conflict="key",
    # ).execute()

    # total = run_scrape_upload(since=args.since, until=args.until)
    # log.info(f"Uploaded {total} raw records in {datetime.now(timezone.utc) - utc_now}")

    # run_transform("twitter")
    # run_transform("reddit")
    run_load("twitter")
    run_load("reddit")