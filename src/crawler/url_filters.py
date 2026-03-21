import threading
from collections import defaultdict
from urllib.parse import urldefrag, urlsplit, urlunsplit, parse_qsl, urlencode

from .crawl_constants import (
    TRAP_QUERY_KEYS,
    TRAP_PATH_SUBSTRINGS,
    MAX_PARAMS,
    MAX_QUERY_LEN,
    MAX_VARIANTS_PER_PATH,
)

_VARIANTS_LOCK = threading.Lock()
PATH_QUERY_SEEN = defaultdict(set)
BAD_URLS = set()

def host_allowed(host: str, allowed_domains : set) -> bool:
    """
    Return True if host matches or is a subdomain of one of ALLOWED_DOMAINS.
    """
    # Accept exact match or any subdomain that ends with ".<allowed_domain>"
    return any(
        host == d or host.endswith("." + d)
        for d in allowed_domains
    )

def normalize_url(url: str) -> str:
    """
    Strips fragments, lowercases scheme+host, trims trailing slash, drops trap query keys,
    sorts remaining query params, caps count/length.
    """
    # Remove fragment (#...) part so same page with different fragments collapses
    clean, _ = urldefrag(url)
    # Split URL into (scheme, netloc, path, query, fragment)
    s = urlsplit(clean)

    # Normalize scheme and host casing
    scheme = (s.scheme or "").lower()
    netloc = (s.netloc or "").lower()

    # remove default ports
    if netloc.endswith(":80") and scheme == "http":
        # Drop :80 for http URLs
        netloc = netloc[:-3]
    elif netloc.endswith(":443") and scheme == "https":
        # Drop :443 for https URLs
        netloc = netloc[:-4]

    # Normalize path: remove trailing slash except keep "/" as root
    path = s.path.rstrip("/") if s.path and s.path != "/" else "/"

    # Collect safe query params (dropping trap keys)
    params = []
    for k, v in parse_qsl(s.query, keep_blank_values=False):
        # Lowercase key for consistent filtering/deduping
        lk = k.lower()
        # Keep only non-trap query keys
        if lk not in TRAP_QUERY_KEYS:
            params.append((lk, v))

    # Sort params for canonical ordering (a=1&b=2 == b=2&a=1)
    params.sort()
    # Cap number of query params to avoid query explosion
    params = params[:MAX_PARAMS]

    # Re-encode query string
    query = urlencode(params, doseq=True)
    # If query is too long, drop it entirely to avoid trap variants
    if len(query) > MAX_QUERY_LEN:
        query = ""

    # Return fully normalized URL without fragment
    return urlunsplit((scheme, netloc, path, query, ""))

def canonicalize_for_count(url: str) -> str:
    """
    Canonical form for UNIQUE_PAGES: uses normalize_url so we don't treat
    trivial query order / tracking params as different pages.
    """
    try:
        # Preferred canonicalization strategy: reuse normalize_url
        return normalize_url(url)
    except Exception:
        # Fallback: do best-effort canonicalization without the full normalize_url logic
        clean, _ = urldefrag(url)
        s = urlsplit(clean)
        scheme = s.scheme.lower()
        netloc = s.netloc.lower()
        path = s.path.rstrip("/") if s.path != "/" else "/"
        return urlunsplit((scheme, netloc, path, s.query, ""))
    
def has_trap_query(parsed) -> bool:
    """
    Return True if any query key is in TRAP_QUERY_KEYS.
    """
    # Parse query into (key, value) pairs and check keys against TRAP_QUERY_KEYS
    return any(k.lower() in TRAP_QUERY_KEYS for k, _ in parse_qsl(parsed.query))


def has_trap_path(path: str) -> bool:
    """
    Return True if the path contains any known trap substrings.
    """
    # Normalize path to lowercase; handle None safely
    p = (path or "").lower()
    # If any trap substring appears in the path, treat as trap
    return any(t in p for t in TRAP_PATH_SUBSTRINGS)


def too_many_variants(url: str) -> bool:
    """
    Limits pages that keep producing new queries for same path.
    Must be thread-safe.
    """
    # Split URL so we can access netloc/path/query
    s = urlsplit(url)
    # If there is no query string, variant control doesn't apply
    if not s.query:
        return False

    # Use (host, path) as the bucket key
    key = (s.netloc.lower(), s.path.lower())
    # Protect shared PATH_QUERY_SEEN structure across threads
    with _VARIANTS_LOCK:
        # Record this specific query string for that (host, path)
        PATH_QUERY_SEEN[key].add(s.query)
        # If we've seen too many distinct queries for that path, treat as trap
        return len(PATH_QUERY_SEEN[key]) > MAX_VARIANTS_PER_PATH

def mark_bad_url(url: str) -> None:
    """
    Record a URL as "bad" so we can refuse it later.
    URL should already be normalized/defragmented by caller.
    """
    # Only record non-empty URLs
    if url:
        # Add to the global set of bad URLs
        BAD_URLS.add(url)

def is_bad_url(url: str) -> bool:
    """
    Return True if URL was previously recorded as bad.
    URL should already be normalized/defragmented by caller.
    """
    # Membership test in BAD_URLS set
    return url in BAD_URLS