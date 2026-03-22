import atexit
import threading
from collections import Counter, defaultdict
from urllib.parse import urlparse

from ..common.paths import ANALYTICS_DIR
from .crawl_constants import STOP_WORDS, DOMAIN_STOP_WORDS
from .url_filters import canonicalize_for_count
from .text_utils import tokenize, tokenize_with_stopwords

_ANALYTICS_LOCK = threading.Lock()

UNIQUE_PAGES = set()
LONGEST_PAGE_URL = None
LONGEST_PAGE_WORDS = 0
WORD_FREQ = Counter()
STOPWORD_FREQ = Counter()
SUBDOMAIN_PAGES = defaultdict(set)

def update_analytics(url: str, text: str):
    """
    Updates:
      1) UNIQUE_PAGES count
      2) longest page by word count
      3) top content words (non-stopwords)
      4) top stopwords
      5) subdomain counts under uci.edu
    """
    global LONGEST_PAGE_URL, LONGEST_PAGE_WORDS

    canon = canonicalize_for_count(url)
    parsed = urlparse(canon)
    host = (parsed.hostname or "").lower()

    if host.startswith("www."):
        host = host[4:]

    content_tokens = tokenize(text)
    all_tokens = tokenize_with_stopwords(text)
    word_count = len(all_tokens)

    with _ANALYTICS_LOCK:
        if canon in UNIQUE_PAGES:
            return

        UNIQUE_PAGES.add(canon)

        if word_count > LONGEST_PAGE_WORDS:
            LONGEST_PAGE_WORDS = word_count
            LONGEST_PAGE_URL = canon

        for t in content_tokens:
            if len(t) >= 2 and t not in DOMAIN_STOP_WORDS:
                WORD_FREQ[t] += 1

        for t in all_tokens:
            if t in STOP_WORDS:
                STOPWORD_FREQ[t] += 1

        if host.endswith(".uci.edu") and host != "uci.edu":
            SUBDOMAIN_PAGES[host].add(canon)


def dump_analytics():
    """
    Write crawl analytics summary to ANALYTICS_DIR/crawl_analytics.txt at program exit.
    """
    ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = ANALYTICS_DIR / "crawl_analytics.txt"

    with open(output_path, "w") as f:
        f.write(f"Unique pages: {len(UNIQUE_PAGES)}\n")
        f.write(f"Longest page ({LONGEST_PAGE_WORDS} words):\n{LONGEST_PAGE_URL}\n\n")

        f.write("Top 50 content words (non-stopwords):\n")
        for w, c in WORD_FREQ.most_common(50):
            f.write(f"{w}, {c}\n")

        f.write("\nTop 50 stopwords:\n")
        for w, c in STOPWORD_FREQ.most_common(50):
            f.write(f"{w}, {c}\n")

        f.write("\nSubdomains under uci.edu:\n")
        for sd in sorted(SUBDOMAIN_PAGES):
            f.write(f"{sd}, {len(SUBDOMAIN_PAGES[sd])}\n")


atexit.register(dump_analytics)
