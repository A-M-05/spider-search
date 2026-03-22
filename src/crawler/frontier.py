import os
import json
from threading import Lock
from pathlib import Path

from .framework import get_logger, get_urlhash, normalize_url
from .scraper import is_valid


class Frontier(object):
    """
    Thread-safe crawl frontier using in-memory data structures.

    Replaces the original shelve-based implementation which breaks on
    Python 3.14 because its SQLite backend forbids cross-thread access.

    State is persisted to a JSON file so crawls can be resumed after
    interruption. All shared state is protected by a single threading.Lock.
    """

    def __init__(self, config, restart, allowed_domains=None):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.allowed_domains = allowed_domains or set()
        self._lock = Lock()

        # save_file is used as a JSON path for persistence
        self.save_path = Path(self.config.save_file).with_suffix(".json")

        # seen: set of urlhashes we have ever added (prevents re-queuing)
        # completed: set of urlhashes that are done
        # to_be_downloaded: list of raw URLs still to fetch
        self._seen = set()
        self._completed = set()
        self.to_be_downloaded = []

        if restart:
            self.logger.info("Restart=True: clearing frontier state.")
            if self.save_path.exists():
                self.save_path.unlink()
            for url in self.config.seed_urls:
                self._add_url_unsafe(url)
        else:
            if self.save_path.exists():
                self._load()
                self.logger.info(
                    f"Resumed frontier: {len(self.to_be_downloaded)} pending, "
                    f"{len(self._completed)} completed.")
            else:
                self.logger.info("No save file found, starting from seed.")
                for url in self.config.seed_urls:
                    self._add_url_unsafe(url)

    # ------------------------------------------------------------------
    # Internal helpers (call only when lock is already held or during init)
    # ------------------------------------------------------------------

    def _add_url_unsafe(self, url):
        """Add a URL without acquiring the lock (used during init)."""
        url = normalize_url(url)
        urlhash = get_urlhash(url)
        if urlhash not in self._seen:
            self._seen.add(urlhash)
            self.to_be_downloaded.append(url)

    def _save(self):
        """Persist frontier state to JSON (call while holding lock)."""
        self.save_path.parent.mkdir(parents=True, exist_ok=True)
        state = {
            "seen": list(self._seen),
            "completed": list(self._completed),
            "to_be_downloaded": self.to_be_downloaded,
        }
        tmp = self.save_path.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(state, f)
        tmp.replace(self.save_path)

    def _load(self):
        """Load frontier state from JSON (called during init, no lock needed)."""
        try:
            with open(self.save_path, "r") as f:
                state = json.load(f)
            self._seen = set(state.get("seen", []))
            self._completed = set(state.get("completed", []))
            # Re-validate URLs against current allowed_domains on resume
            self.to_be_downloaded = [
                url for url in state.get("to_be_downloaded", [])
                if is_valid(url, self.allowed_domains)
            ]
        except Exception as e:
            self.logger.error(f"Failed to load frontier state: {e}. Starting fresh.")
            for url in self.config.seed_urls:
                self._add_url_unsafe(url)

    # ------------------------------------------------------------------
    # Public thread-safe interface
    # ------------------------------------------------------------------

    def get_tbd_url(self):
        with self._lock:
            if self.to_be_downloaded:
                return self.to_be_downloaded.pop()
            return None

    def add_url(self, url):
        url = normalize_url(url)
        urlhash = get_urlhash(url)
        with self._lock:
            if urlhash not in self._seen:
                self._seen.add(urlhash)
                self.to_be_downloaded.append(url)
                self._save()

    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        with self._lock:
            self._completed.add(urlhash)
            self._save()
