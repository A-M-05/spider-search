import json
import hashlib
from pathlib import Path

from ..common.paths import RAW_DATA_DIR

def _safe_filename(url: str) -> str:
    """
    Generate a deterministic filename from a URL using SHA-1.
    Prevents filesystem issues with long/invalid URLs.
    """
    return hashlib.sha1(url.encode("utf-8")).hexdigest() + ".json"


def write_document(url: str, content: str):
    """
    Persist a crawled page to disk in JSON format.

    Format:
    {
        "url": "...",
        "content": "<html>...</html>"
    }
    """

    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    filename = _safe_filename(url)
    file_path = RAW_DATA_DIR / filename

    data = {
        "url": url,
        "content": content,
    }

    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception as e:
        print(f"[WRITE ERROR] {url}: {e}")
