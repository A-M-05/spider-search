import os
import shelve
from threading import Lock

from .framework import get_logger, get_urlhash, normalize_url
from .scraper import is_valid


class Frontier(object):
    def __init__(self, config, restart, allowed_domains=None):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.allowed_domains = allowed_domains or set()
        self.to_be_downloaded = list()
        self._lock = Lock()  # Protects both self.save and self.to_be_downloaded

        if not os.path.exists(self.config.save_file) and not restart:
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            # shelve may create multiple files (.db, .dir, .bak etc.)
            # remove all of them
            for suffix in ["", ".db", ".dir", ".bak", ".dat"]:
                path = self.config.save_file + suffix
                if os.path.exists(path):
                    os.remove(path)

        self.save = shelve.open(self.config.save_file)

        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        with self._lock:
            total_count = len(self.save)
            tbd_count = 0
            for url, completed in self.save.values():
                if not completed and is_valid(url, self.allowed_domains):
                    self.to_be_downloaded.append(url)
                    tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):
        with self._lock:
            try:
                return self.to_be_downloaded.pop()
            except IndexError:
                return None

    def add_url(self, url):
        url = normalize_url(url)
        urlhash = get_urlhash(url)
        with self._lock:
            if urlhash not in self.save:
                self.save[urlhash] = (url, False)
                self.save.sync()
                self.to_be_downloaded.append(url)

    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        with self._lock:
            if urlhash not in self.save:
                self.logger.error(
                    f"Completed url {url}, but have not seen it before.")
            self.save[urlhash] = (url, True)
            self.save.sync()
