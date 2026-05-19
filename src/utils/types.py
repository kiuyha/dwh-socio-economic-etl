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
    text_content: str
    subreddit: Optional[str]
    posted_at: str
    score: int
    upvote_count: int
    downvote_count: int
    permalink: Optional[str]

class SentimentDict(TypedDict):
    """A dictionary representing a single sentiment."""
    label: str
    confidence: float

class TopicDict(TypedDict):
    """A dictionary representing a single topic."""
    label: str
    category: str
    top_keywords: list[str]