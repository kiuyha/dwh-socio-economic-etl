from typing import TypedDict, Optional

class ListSearchConfig(TypedDict):
    query: str
    depth: Optional[int]
    time_budget: Optional[int]

class SearchConfigDict(TypedDict):
    """A dictionary representing a single search configuration."""
    nitter: list[ListSearchConfig]
    reddit: list[ListSearchConfig]

class ScrapedTweetDict(TypedDict):
    """A dictionary representing a single tweet."""
    id: str
    text_content: Optional[str]
    fullname: Optional[str]
    posted_at: Optional[str]
    username: Optional[str]
    like_count: Optional[int]
    retweet_count: Optional[int]
    comment_count: Optional[int]
    quote_count: Optional[int]

class ScrapedRedditDict(TypedDict):
    """A dictionary representing a single reddit post."""
    id: str
    username: Optional[str]
    title: Optional[str]
    body: Optional[str]
    subreddit: Optional[str]
    posted_at: str
    score: int
    upvote_count: int
    downvote_count: int
    upvote_ratio: Optional[float]
    comment_count: int
    permalink: Optional[str]