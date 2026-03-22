from threading import Thread
import time

from .framework.download import download
from .framework import get_logger
from .text_utils import to_text
from ..io.dataset_writer import write_document
from . import scraper


class Worker(Thread):
    def __init__(self, worker_id, config, frontier, allowed_domains=None):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        self.allowed_domains = allowed_domains or set()
        self.worker_id = worker_id
        self.pages_crawled = 0
        super().__init__(daemon=True)

    def run(self):
        print(f"[Worker-{self.worker_id}] Started.")

        while True:
            tbd_url = self.frontier.get_tbd_url()

            if not tbd_url:
                print(f"[Worker-{self.worker_id}] Frontier empty. Stopping. "
                      f"(crawled {self.pages_crawled} pages)")
                break

            resp = download(tbd_url, self.config, self.logger)
            status = resp.status
            self.pages_crawled += 1

            print(f"[Worker-{self.worker_id}] [{self.pages_crawled}] "
                  f"status={status} {tbd_url}")

            if status == 200 and resp.raw_response is not None:
                # Save the raw HTML to disk so the indexer can find it later
                html = to_text(resp.raw_response.content)
                write_document(tbd_url, html)

                # Extract outgoing links and add them to the frontier
                scraped_urls = scraper.scraper(tbd_url, resp, self.allowed_domains)
                new_urls = 0
                for scraped_url in scraped_urls:
                    self.frontier.add_url(scraped_url)
                    new_urls += 1

                if new_urls:
                    print(f"[Worker-{self.worker_id}]   -> found {new_urls} new URLs")
            else:
                # Still run scraper for bad-URL recording side effects
                scraper.scraper(tbd_url, resp, self.allowed_domains)

            self.frontier.mark_url_complete(tbd_url)
            time.sleep(self.config.time_delay)
