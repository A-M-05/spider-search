from urllib.parse import urldefrag, urlsplit, urlunsplit, urljoin

def normalize_url(url: str) -> str:
    """
    Normalize a URL into a canonical representation.

    This ensures consistent indexing and prevents duplicates caused by
    formatting variations. The normalization includes:
    - removing fragments
    - lowercasing scheme and hostname
    - removing default ports
    - trimming trailing slashes (except root)

    :param url: Raw URL string.
    :type url: str
    :return: Canonicalized URL or empty string if invalid.
    :rtype: str
    """

    if not url:
        return ""

    # Remove fragment and safely split into components.
    try:
        clean_url, _ = urldefrag(url)
        split = urlsplit(clean_url)
    except Exception:
        return ""

    scheme = (split.scheme or "").lower()
    netloc = (split.netloc or "").lower()
    path = split.path or "/"
    query = split.query

    # Remove default ports to avoid duplicate representations.
    if scheme == "http" and netloc.endswith(":80"):
        netloc = netloc[:-3]
    elif scheme == "https" and netloc.endswith(":443"):
        netloc = netloc[:-4]

    # Normalize trailing slash (but preserve root).
    if path != "/":
        path = path.rstrip("/") or "/"

    return urlunsplit((scheme, netloc, path, query, ""))

def resolve_and_normalize_url(base_url: str, raw_url: str) -> str:
    if not raw_url:
        return ""
    absolute = urljoin(base_url, raw_url)
    return normalize_url(absolute)
