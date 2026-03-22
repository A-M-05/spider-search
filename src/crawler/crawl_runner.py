import configparser
from pathlib import Path

from .framework.config import Config
from .frontier import Frontier
from .worker import Worker
from .domain_config import get_allowed_domains, is_valid_seed_url
from ..common.paths import CRAWL_STATE_DIR

CONFIG_PATH = Path("config/crawler.ini")


def run_crawler(seed_url=None, restart=True):
    """
    Run the web crawler.

    :param seed_url: Override seed URL from config (optional).
    :param restart: If True, clears existing crawl state and starts fresh.
                    Defaults to True so repeated runs don't get stuck on
                    an already-completed frontier.
    """
    print("=== STARTING CRAWLER ===")

    parser = configparser.ConfigParser()
    parser.read(CONFIG_PATH)
    config = Config(parser)

    # Runtime seed URL overrides config file seed URLs
    if seed_url is not None:
        if not is_valid_seed_url(seed_url):
            raise ValueError(f"Invalid seed URL: {seed_url}")
        config.seed_urls = [seed_url]

    if not config.seed_urls:
        raise ValueError(
            "No seed URL provided. Pass a URL as an argument:\n"
            "  python -m src.main crawl https://example.com\n"
            "Or set SEEDURL in config/crawler.ini"
        )

    allowed_domains = get_allowed_domains(config.seed_urls)
    print(f"Seed URLs:       {config.seed_urls}")
    print(f"Allowed domains: {allowed_domains}")
    print(f"Threads:         {config.threads_count}")
    print(f"Politeness:      {config.time_delay}s delay between requests")
    print(f"Restart:         {restart}")
    print()

    CRAWL_STATE_DIR.mkdir(parents=True, exist_ok=True)

    # restart=True clears the old frontier so we always start fresh.
    # This prevents the crawler from silently doing nothing because the
    # previous frontier save file already marked everything as complete.
    frontier = Frontier(config=config, restart=restart, allowed_domains=allowed_domains)

    workers = []
    for i in range(config.threads_count):
        worker = Worker(
            worker_id=i,
            config=config,
            frontier=frontier,
            allowed_domains=allowed_domains,
        )
        workers.append(worker)

    for w in workers:
        w.start()

    for w in workers:
        w.join()

    total = sum(w.pages_crawled for w in workers)
    print()
    print(f"=== CRAWL COMPLETE — {total} pages fetched ===")
