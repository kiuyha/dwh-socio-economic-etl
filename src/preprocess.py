import requests
import unicodedata
import re
import html
from pathlib import Path
from core import log, config

_EMOJI_RE = re.compile(
    "["
    "\U0001F600-\U0001F64F"   # emoticons
    "\U0001F300-\U0001F5FF"   # symbols & pictographs
    "\U0001F680-\U0001F6FF"   # transport & map
    "\U0001F700-\U0001F77F"   # alchemical
    "\U0001F780-\U0001F7FF"   # geometric shapes extended
    "\U0001F800-\U0001F8FF"   # supplemental arrows
    "\U0001F900-\U0001F9FF"   # supplemental symbols & pictographs
    "\U0001FA00-\U0001FA6F"   # chess / symbols
    "\U0001FA70-\U0001FAFF"   # symbols & pictographs extended-A
    "\U00002702-\U000027B0"   # dingbats
    "\U000024C2-\U0001F251"   # enclosed characters
    "\U0001F1E0-\U0001F1FF"   # regional indicator (flags)
    "\u2600-\u26FF"           # misc symbols
    "\u2700-\u27BF"           # dingbats block
    "\uFE00-\uFE0F"           # variation selectors
    "\u200D"                  # zero-width joiner
    "\u20E3"                  # combining enclosing keycap
    "]+",
    flags=re.UNICODE,
)

_STOPWORDS_CACHE = Path(__file__).parent / ".cache" / "stopwords.txt"

_STOPWORD_URLS = [
    "https://raw.githubusercontent.com/stopwords-iso/stopwords-id/master/stopwords-id.txt",
    "https://raw.githubusercontent.com/stopwords-iso/stopwords-en/master/stopwords-en.txt",
]

def get_stopwords(force_refresh: bool = False) -> set[str]:
    """
    Returns a set of stopwords loaded from a local cache file.
    Downloads from stopwords-iso on the first run (or when force_refresh=True).
    No nltk required.
    """
    if _STOPWORDS_CACHE.exists() and not force_refresh:
        words = _STOPWORDS_CACHE.read_text(encoding="utf-8").splitlines()
        return {w.strip().lower() for w in words if w.strip()}

    _STOPWORDS_CACHE.parent.mkdir(parents=True, exist_ok=True)
    combined: list[str] = []

    for url in _STOPWORD_URLS:
        try:
            res = requests.get(url, timeout=10)
            res.raise_for_status()
            combined.extend(res.text.splitlines())
            log.info(f"Downloaded stopwords from {url}")
        except requests.RequestException as e:
            log.error(f"Failed to fetch stopwords from {url}: {e}")

    if combined:
        _STOPWORDS_CACHE.write_text("\n".join(combined), encoding="utf-8")

    return {w.strip().lower() for w in combined if w.strip()}

def get_tld_list(retry_count: int = 3) -> list:
    url = "https://data.iana.org/TLD/tlds-alpha-by-domain.txt"
    while retry_count > 0:
        try:
            response = requests.get(url)
            response.raise_for_status()
            tlds = response.text.strip().split('\n')
            log.info("Fetched TLD list successfully.")
            return [tld.lower() for tld in tlds if not tld.startswith('#')]
        except requests.exceptions.RequestException as e:
            log.error(f"Error fetching TLD list: {e} (Retrying...)")
            retry_count -= 1
    return []


def split_hashtag(tag: str) -> str:
    tag = tag.lstrip("#")
    tag = tag.replace("_", " ")
    return re.sub(r'([a-z])([0-9])', r'\1 \2', tag, flags=re.I)


tld_list = get_tld_list()

def processing_text(text: str) -> str:
    if not isinstance(text, str):
        return None

    # Remove HTML entities and tags
    previous_text = ""
    while text != previous_text:
        previous_text = text
        text = html.unescape(text)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = "".join(ch for ch in text if not unicodedata.category(ch).startswith('S'))
    text = re.sub(r'[<>\[\]]', '', text)

    # Normalize unicode (e.g. '𝖒𝖚𝖉𝖆𝖍' -> 'mudah')
    text = unicodedata.normalize('NFKC', text)

    # Remove URLs and emails
    tld_pattern_group = '|'.join(re.escape(tld) for tld in tld_list)
    url_pattern = re.compile(
        r'https?://\S+|www\.\S+|[\w\.-]+\.(?:' + tld_pattern_group + r')[\S]*'
    )
    text = url_pattern.sub('', text)
    text = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b').sub('', text)

    # Handle hashtags
    for tag in re.findall(r"#\w+", text):
        replacement = "" if tag.lower() in config.unnecessary_hashtags else split_hashtag(tag)
        text = text.replace(tag, replacement)

    # Strip '@' from mentions but keep the name
    text = re.compile(r'@(\w+)').sub(r'\1', text)

    # Slang normalisation
    text = " ".join(config.slang_mapping.get(w, w) for w in text.split())

    # Remove emojis
    text = _EMOJI_RE.sub('', text)

    # Collapse whitespace
    text = re.sub(r'\s+', ' ', text).strip()

    return text