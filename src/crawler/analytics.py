import atexit
import threading
from collections import Counter, defaultdict
from urllib.parse import urlparse

from ..common.paths import ANALYTICS_DIR
from .url_filters import canonicalize_for_count
from .text_utils import tokenize, tokenize_with_stopwords
from .crawl_constants import STOP_WORDS, DOMAIN_STOP_WORDS

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

    # Canonicalize URL for counting unique pages
    canon = canonicalize_for_count(url)
    # Parse canonical URL to extract host information
    parsed = urlparse(canon)
    # Normalize host to lowercase; avoid None
    host = (parsed.hostname or "").lower()

    # For subdomain reporting, treat www.* as the same subdomain
    if host.startswith("www."):
        host = host[4:]

    # Tokens excluding stopwords (for "top content words")
    content_tokens = tokenize(text)
    # Tokens including stopwords (for longest page + stopword frequency)
    all_tokens = tokenize_with_stopwords(text)
    # Word count is token count including stopwords (excluding pure digits)
    word_count = len(all_tokens)

    # Lock analytics so updates are thread-safe and UNIQUE_PAGES behaves correctly
    with _ANALYTICS_LOCK:
        # If we already counted this canonical URL, don't double-count or re-add frequencies
        if canon in UNIQUE_PAGES:
            return

        # Record this page as unique
        UNIQUE_PAGES.add(canon)

        # Update longest page tracking if this page is bigger
        if word_count > LONGEST_PAGE_WORDS:
            LONGEST_PAGE_WORDS = word_count
            LONGEST_PAGE_URL = canon

        # content words
        for t in content_tokens:
            # Ignore very short tokens and domain-noise words
            if len(t) >= 2 and t not in DOMAIN_STOP_WORDS:
                # Increment frequency for content word reporting
                WORD_FREQ[t] += 1

        # stopwords
        for t in all_tokens:
            # Only count if token is in STOP_WORDS list
            if t in STOP_WORDS:
                # Increment stopword frequency
                STOPWORD_FREQ[t] += 1

        # uci.edu subdomains
        if host.endswith(".uci.edu") and host != "uci.edu":
            # Track which unique pages were found under each subdomain
            SUBDOMAIN_PAGES[host].add(canon)


def dump_analytics():
    """
    Write crawl analytics summary to crawl_analytics.txt at program exit.
    """
    # Open output file for writing (overwrites prior run)
    with open("crawl_analytics.txt", "w") as f:
        # Write unique page count
        f.write(f"Unique pages: {len(UNIQUE_PAGES)}\n")
        # Write longest page info
        f.write(f"Longest page ({LONGEST_PAGE_WORDS} words):\n{LONGEST_PAGE_URL}\n\n")

        # Write most common content words (non-stopwords)
        f.write("Top 50 content words (non-stopwords):\n")
        for w, c in WORD_FREQ.most_common(50):
            f.write(f"{w}, {c}\n")

        # Write most common stopwords
        f.write("\nTop 50 stopwords:\n")
        for w, c in STOPWORD_FREQ.most_common(50):
            f.write(f"{w}, {c}\n")

        # Write subdomain list and page counts per subdomain
        f.write("\nSubdomains under uci.edu:\n")
        for sd in sorted(SUBDOMAIN_PAGES):
            f.write(f"{sd}, {len(SUBDOMAIN_PAGES[sd])}\n")


# Register analytics dump so it runs automatically when the process exits normally
atexit.register(dump_analytics)