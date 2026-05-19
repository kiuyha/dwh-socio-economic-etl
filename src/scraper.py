import httpx
from lxml import html
import time
import random
from datetime import datetime, timezone
from urllib.parse import quote_plus
import requests
from core import log, env
from typing import Tuple, Optional
from src.utils.types import ScrapedTweetDict, ScrapedRedditDict
import itertools

def safetly_extract_text(element, xpath: str, attribute: Optional[str] = None) -> Optional[str]:
    try:
        if attribute:
            return element.xpath(xpath)[0].attrib.get(attribute).strip()
        else:
            return element.xpath(xpath)[0].text_content().strip()
    except Exception:
        return None


def exctract_id_tweet(tweet) -> Optional[str]:
    tweet_link = safetly_extract_text(tweet, './/a[contains(@class, "tweet-link")]', attribute='href')
    if not tweet_link:
        return None
    return tweet_link.split('/')[-1].split('#')[0]


def get_posted_at(tweet) -> Optional[datetime]:
    time_string = safetly_extract_text(tweet, './/span[contains(@class,"tweet-date")]/a', attribute='title')
    if not time_string:
        return None
    try:
        # Parse the string, then forcefully attach UTC timezone to prevent naive datetime corruption
        dt = datetime.strptime(time_string.replace('·', ''), "%b %d, %Y %I:%M %p %Z")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None

def extract_new_tweets_and_next_link(html_content: str) -> Tuple[list[ScrapedTweetDict], Optional[str]]:
    tree = html.fromstring(html_content)
    list_tweets = tree.xpath('.//div[contains(@class, "timeline-item")]')

    scraped_tweets: list[ScrapedTweetDict] = []
    for tweet in list_tweets:
        tweet_id = exctract_id_tweet(tweet)
        posted_at_dt = get_posted_at(tweet)
        
        if not tweet_id or not posted_at_dt:
            continue
            
        scraped_tweets.append({
            'id': tweet_id,
            'fullname': safetly_extract_text(tweet, './/a[contains(@class, "fullname")]'),
            'username': safetly_extract_text(tweet, './/a[contains(@class, "username")]'),
            'text_content': safetly_extract_text(tweet, './/div[contains(@class, "tweet-content")]'),
            'posted_at': posted_at_dt.isoformat(),
            'like_count': int((safetly_extract_text(tweet, './/span[contains(@class, "tweet-stat") and .//span[contains(@class, "icon-heart")]]') or '0').replace(',', '')),
            'comment_count': int((safetly_extract_text(tweet, './/span[contains(@class, "tweet-stat") and .//span[contains(@class, "icon-comment")]]') or '0').replace(',', '')),
            'retweet_count': int((safetly_extract_text(tweet, './/span[contains(@class, "tweet-stat") and .//span[contains(@class, "icon-retweet")]]') or '0').replace(',', '')),
            'quote_count': int((safetly_extract_text(tweet, './/span[contains(@class, "tweet-stat") and .//span[contains(@class, "icon-quote")]]') or '0').replace(',', '')),
        })

    links = tree.xpath('.//div[contains(@class, "show-more")]/a')
    if links:
        next_link = (links[1] if len(links) > 1 else links[0]).attrib.get('href')
    else:
        next_link = None

    return scraped_tweets, next_link


def extract_new_reddit_posts(posts: list) -> list[ScrapedRedditDict]:
    base_url = "https://www.reddit.com"

    scraped_posts: list[ScrapedRedditDict] = [
        {
            "id":            p.get("id"),
            "username":      p.get("author", ""),
            "title":         p.get("title") or None,
            "body":          p.get("selftext") or p.get("body") or None,
            "subreddit":     p.get("subreddit") or None,
            "posted_at":     datetime.fromtimestamp(int(p["created_utc"]), tz=timezone.utc).isoformat(),
            "score":         p.get("score", 0),
            "upvote_count":  p.get("ups", 0),
            "downvote_count": p.get("downs", 0),
            "upvote_ratio":  p.get("upvote_ratio") or None,
            "comment_count": p.get("num_comments", 0),
            "permalink":     f"{base_url}{p['permalink']}" if p.get("permalink") else None,
        }
        for p in posts
        if p.get("id") and p.get("created_utc")
    ]

    return scraped_posts

def scrap_nitter(search_query: str, depth: int = -1, time_budget: int = -1) -> list[ScrapedTweetDict]:
    if search_query is None:
        raise ValueError("search_query cannot be None")
    if not isinstance(search_query, str):
        raise TypeError("search_query must be a string")
    if not isinstance(depth, int):
        raise TypeError("depth must be an integer")
    if time_budget != -1 and not isinstance(time_budget, int):
        raise TypeError("time_budget must be an integer")

    log.info(f"Scraping tweets for query: {search_query!r}  depth={depth}  budget={time_budget}s")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.5',
        'Upgrade-Insecure-Requests': '1',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    }
    next_link = f'?f=tweets&q={quote_plus(search_query)}&f=tweets'
    scraped_data = []
    start_time = time.time()
    index = 1

    raw_proxies = env.get("WEBSHARE_PROXIES", "")
    proxy_list = [p.strip() for p in raw_proxies.split(",") if p.strip()]
    if not proxy_list:
        log.warning("No proxies found in WEBSHARE_PROXIES env var. Running without proxy.")
        proxy_list = [None]
    else:
        # Fix: ensure every proxy has an http:// scheme so httpx actually uses them
        proxy_list = [
            p if p.startswith("http://") or p.startswith("https://") else f"http://{p}"
            for p in proxy_list
        ]
        safe_proxies = [p.split('@')[-1] for p in proxy_list]
        log.info(f"Successfully loaded {len(proxy_list)} proxies from environment: {safe_proxies}")
        random.shuffle(proxy_list)

    def get_display(proxy):
        if proxy is None:
            return "Local_IP"
        return proxy.split('@')[-1]

    proxy_cycle = itertools.cycle(proxy_list)
    current_proxy = next(proxy_cycle)
    display_proxy = get_display(current_proxy)

    max_retries = len(proxy_list)
    retries = 0
    client = httpx.Client(headers=headers, http2=True, timeout=25.0, proxy=current_proxy)

    try:
        while True:
            if time_budget != -1 and (time.time() - start_time) > time_budget:
                log.info(f"Time budget of {time_budget}s exceeded. Stopping.")
                break
            if next_link is None:
                log.info("No more pages. Stopping.")
                break
            if depth != -1 and index > depth:
                log.info(f"Reached max depth {depth}. Stopping.")
                break

            try:
                response = client.get(f'https://nitter.net/search{next_link}')
                status_code = response.status_code

                if status_code == 200 and response.text:
                    new_tweets, next_link = extract_new_tweets_and_next_link(response.text)
                    scraped_data.extend(new_tweets)
                    log.info(f"page={index}  new={len(new_tweets)}  next={next_link}")
                    time.sleep(random.uniform(2, 5))
                    index += 1
                    retries = 0

                elif status_code == 200:
                    log.info(f"Empty response on page {index}. Stopping.")
                    break

                elif status_code in (429, 403):
                    log.warning(f"Blocked or Rate Limited ({status_code}), current proxy: {display_proxy}. Rotating to next proxy in cycle...")
                    retries += 1

                    if retries < max_retries:
                        # Still have proxies to try — rotate and continue
                        client.close()
                        current_proxy = next(proxy_cycle)
                        display_proxy = get_display(current_proxy)
                        client = httpx.Client(headers=headers, http2=True, timeout=25.0, proxy=current_proxy)
                        time.sleep(2)

                    elif status_code == 429:
                        # All proxies exhausted — last resort: honour Retry-After header
                        log.warning(f"All {max_retries} proxies exhausted. Checking Retry-After header as last resort...")
                        log.warning(f"Hit rate limit (429). Headers: {response.headers}")
                        wait_seconds = None

                        if 'retry-after' in response.headers:
                            retry_value = response.headers['retry-after']
                            if retry_value.isdigit():
                                wait_seconds = int(retry_value)
                            else:
                                try:
                                    retry_date = datetime.strptime(retry_value, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=timezone.utc)
                                    now = datetime.now(timezone.utc)
                                    wait_seconds = (retry_date - now).total_seconds()
                                except ValueError:
                                    log.error(f"Could not parse 'Retry-After' date: {retry_value}")
                                    break

                        if wait_seconds is None or wait_seconds < 0:
                            log.warning("No valid 'Retry-After' header found. Stopping.")
                            break

                        remaining_time = (time_budget - (time.time() - start_time)) if time_budget != -1 else float('inf')
                        if wait_seconds > remaining_time:
                            log.warning(
                                f"Wait time ({wait_seconds}s) exceeds remaining budget ({remaining_time:.0f}s). Stopping."
                            )
                            break

                        log.info(f"Waiting {wait_seconds:.2f}s as instructed by server, then resuming...")
                        time.sleep(wait_seconds)
                        # Reset retries so proxies get another round after the wait
                        retries = 0

                    else:
                        # 403 with all proxies exhausted — nothing more to try
                        log.error("Exhausted all available proxies on a 403. Stopping.")
                        break

                else:
                    log.info(f"Unexpected status {status_code}. Stopping.")
                    break

            except Exception as e:
                log.error(f"Request error: {e}, current proxy: {display_proxy}. Rotating to next proxy in cycle...")
                retries += 1
                if retries >= max_retries:
                    log.error("Exhausted all available proxies. Stopping.")
                    break
                client.close()
                current_proxy = next(proxy_cycle)
                display_proxy = get_display(current_proxy)
                client = httpx.Client(headers=headers, http2=True, timeout=25.0, proxy=current_proxy)
                time.sleep(2)

    finally:
        client.close()

    log.info(f"Scraped {len(scraped_data)} tweets total.")
    return scraped_data

def scrape_reddit(search_query: str, subreddit: Optional[str] = None, depth: int = -1, time_budget: int = -1) -> list[ScrapedRedditDict]:
    """Scrape Reddit posts via PullPush.

    Args:
        search_query: The query to search for.
        subreddit: Comma-separated list of subreddits (e.g., 'indonesia,investasi').
        depth: Max number of pages to fetch (-1 for unlimited).
        time_budget: Max seconds to run (-1 for unlimited).
    """
    if search_query is None:
        raise ValueError("search_query cannot be None")
    if not isinstance(search_query, str):
        raise TypeError("search_query must be a string")
    if not isinstance(depth, int):
        raise TypeError("depth must be an integer")
    if time_budget != -1 and not isinstance(time_budget, int):
        raise TypeError("time_budget must be an integer")

    log.info(f"Scraping Reddit for query: {search_query!r}  subreddit: {subreddit}  depth={depth}  budget={time_budget}s")

    scraped_data = []
    last_timestamp = None
    start_time = time.time()
    index = 1

    while True:
        if time_budget != -1 and (time.time() - start_time) > time_budget:
            log.info(f"Time budget of {time_budget}s exceeded. Stopping.")
            break
        if depth != -1 and index > depth:
            log.info(f"Reached max depth {depth}. Stopping.")
            break

        params = {"q": search_query, "size": 100, "sort": "desc"}
        
        # Add subreddit filter if provided
        if subreddit:
            params["subreddit"] = [sub.strip() for sub in subreddit.split(",")]
            
        if last_timestamp:
            params["before"] = last_timestamp

        try:
            response = requests.get("https://api.pullpush.io/reddit/search", params=params, timeout=10)
            response.raise_for_status()
            posts = response.json().get("data", [])
        except Exception as e:
            log.error(f"Request error: {e}")
            break

        if not posts:
            log.info("No more posts.")
            break

        new_posts = extract_new_reddit_posts(posts)
        if not new_posts:
            log.info("All posts already in DB. Stopping.")
            break

        scraped_data.extend(new_posts)
        log.info(f"page={index}  new={len(new_posts)}  url={response.url}")

        previous_timestamp = last_timestamp
        last_timestamp = posts[-1].get("created_utc")
        if last_timestamp == previous_timestamp:
            log.info("Timestamp didn't advance. Stopping.")
            break

        index += 1
        time.sleep(1)

    log.info(f"Scraped {len(scraped_data)} Reddit posts total.")
    return scraped_data