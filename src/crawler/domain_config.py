from urllib.parse import urlparse

def is_valid_seed_url(url: str) -> bool:
    try:
        parsed = urlparse(url.strip())
        return parsed.scheme in {"http", "https"} and bool(parsed.netloc)
    except Exception:
        return False


def get_allowed_domains(seed_urls):
    domains = set()
    for url in seed_urls:
        parsed = urlparse(url)
        if parsed.netloc:
            domains.add(parsed.netloc.lower())
    return domains
