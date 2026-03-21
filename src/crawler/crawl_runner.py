import configparser
from pathlib import Path

from .framework.config import Config
from .framework.server_registration import get_cache_server
from .frontier import Frontier
from .worker import Worker
from .domain_config import get_allowed_domains, is_valid_seed_url
from ..common.paths import CRAWL_STATE_DIR

CONFIG_PATH = Path("config/crawler.ini")


def run_crawler(seed_url=None):
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
        raise ValueError("No seed URL provided.")

    allowed_domains = get_allowed_domains(config.seed_urls)
    print(f"Seed URLs: {config.seed_urls}")
    print(f"Allowed domains: {allowed_domains}")

    CRAWL_STATE_DIR.mkdir(parents=True, exist_ok=True)

    cache_server = get_cache_server(config, restart=False)

    frontier = Frontier(config=config, restart=False)

    workers = []
    for i in range(config.threads_count):
        worker = Worker(
            worker_id=i,
            config=config,
            frontier=frontier,
            cache_server=cache_server,
            allowed_domains=allowed_domains,
        )
        workers.append(worker)

    for w in workers:
        w.start()

    for w in workers:
        w.join()

    print("=== CRAWL COMPLETE ===")
