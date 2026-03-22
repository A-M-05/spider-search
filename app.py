"""
Flask web interface for the search engine.
Serves the crawl stage and search stage as a single-page app.

Run with:
    python app.py
Then open http://127.0.0.1:5000 in your browser.
"""

import threading
import queue
import time
from pathlib import Path
from flask import Flask, render_template, request, jsonify, Response, stream_with_context

app = Flask(__name__, template_folder="templates")

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------

# Queue that worker threads push progress messages into.
# The SSE endpoint drains this and streams it to the browser.
_progress_queue = queue.Queue()

# Set to True to request a crawl stop.
_stop_requested = threading.Event()

# Tracks whether the pipeline has finished and search is ready.
_search_ready = threading.Event()

# The searcher instance, created after the pipeline finishes.
_searcher = None
_searcher_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/crawl", methods=["POST"])
def start_crawl():
    """Start the full pipeline (crawl → index → merge → graph → pagerank) in a background thread."""
    data = request.get_json()
    url = (data or {}).get("url", "").strip()

    if not url:
        return jsonify({"error": "No URL provided"}), 400

    # Clear state from any previous run
    _stop_requested.clear()
    _search_ready.clear()
    with _searcher_lock:
        global _searcher
        _searcher = None

    # Drain any leftover messages
    while not _progress_queue.empty():
        try:
            _progress_queue.get_nowait()
        except queue.Empty:
            break

    thread = threading.Thread(target=_run_pipeline, args=(url,), daemon=True)
    thread.start()

    return jsonify({"status": "started"})


@app.route("/stop", methods=["POST"])
def stop_crawl():
    """Signal the crawler to stop after the current page."""
    _stop_requested.set()
    return jsonify({"status": "stop requested"})


@app.route("/progress")
def progress():
    """Server-Sent Events endpoint — streams crawl progress to the browser."""
    def generate():
        while True:
            try:
                msg = _progress_queue.get(timeout=30)
                yield f"data: {msg}\n\n"
                if msg.startswith("DONE") or msg.startswith("ERROR"):
                    break
            except queue.Empty:
                # Send a keepalive comment so the connection stays open
                yield ": keepalive\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        }
    )


@app.route("/search")
def search():
    """Run a search query and return ranked URLs."""
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify({"results": []})

    with _searcher_lock:
        if _searcher is None:
            return jsonify({"error": "Search index not ready"}), 503

        try:
            results = _searcher.search(query, top_k=15)
            return jsonify({"results": results})
        except Exception as e:
            return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Pipeline runner (runs in background thread)
# ---------------------------------------------------------------------------

def _push(msg: str):
    """Push a progress message to the SSE queue."""
    _progress_queue.put(msg)


def _run_pipeline(seed_url: str):
    """Run crawl → build-index → merge → graph → pagerank, streaming progress."""
    global _searcher

    try:
        # ── CRAWL ──────────────────────────────────────────────────────────
        _push("STAGE:Crawling")
        _push(f"INFO:Starting crawl of {seed_url}")

        import configparser
        from src.crawler.framework.config import Config
        from src.crawler.frontier import Frontier
        from src.crawler.domain_config import get_allowed_domains, is_valid_seed_url
        from src.crawler.framework.download import download
        from src.crawler.text_utils import to_text
        from src.io.dataset_writer import write_document
        from src.crawler import scraper
        from src.common.paths import CRAWL_STATE_DIR, RAW_DATA_DIR

        if not is_valid_seed_url(seed_url):
            _push(f"ERROR:Invalid URL: {seed_url}")
            return

        parser = configparser.ConfigParser()
        parser.read("config/crawler.ini")
        config = Config(parser)
        config.seed_urls = [seed_url]

        allowed_domains = get_allowed_domains(config.seed_urls)
        _push(f"INFO:Allowed domains: {allowed_domains}")

        CRAWL_STATE_DIR.mkdir(parents=True, exist_ok=True)
        RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

        frontier = Frontier(config=config, restart=True, allowed_domains=allowed_domains)

        pages_crawled = 0
        politeness = config.time_delay

        while True:
            if _stop_requested.is_set():
                _push(f"INFO:Stop requested — crawled {pages_crawled} pages.")
                break

            url = frontier.get_tbd_url()
            if url is None:
                _push(f"INFO:Frontier empty — crawled {pages_crawled} pages.")
                break

            # Minimal logger shim
            class _Log:
                def warning(self, m): _push(f"WARN:{m}")
                def error(self, m): _push(f"WARN:{m}")

            resp = download(url, config, _Log())
            status = resp.status
            pages_crawled += 1

            _push(f"PAGE:{pages_crawled}:{status}:{url}")

            if status == 200 and resp.raw_response is not None:
                html = to_text(resp.raw_response.content)
                write_document(url, html)
                scraped_urls = scraper.scraper(url, resp, allowed_domains)
                for scraped_url in scraped_urls:
                    frontier.add_url(scraped_url)

            frontier.mark_url_complete(url)
            time.sleep(politeness)

        if pages_crawled == 0:
            _push("ERROR:No pages were crawled. Check the URL and try again.")
            return

        # ── BUILD INDEX ────────────────────────────────────────────────────
        _push("STAGE:Building index")
        from src.indexing.build_index import build_index
        build_index()
        _push("INFO:Index built.")

        # ── MERGE ──────────────────────────────────────────────────────────
        _push("STAGE:Merging index")
        from src.indexing.merge_partials import merge_partials
        merge_partials()
        _push("INFO:Merge complete.")

        # ── GRAPH ──────────────────────────────────────────────────────────
        _push("STAGE:Building link graph")
        from src.ranking.link_graph_builder import build_graph
        build_graph()
        _push("INFO:Link graph complete.")

        # ── PAGERANK ───────────────────────────────────────────────────────
        _push("STAGE:Computing PageRank")
        from src.ranking.pagerank import build_pagerank
        build_pagerank()
        _push("INFO:PageRank complete.")

        # ── LOAD SEARCHER ──────────────────────────────────────────────────
        _push("STAGE:Loading search index")
        from src.search.searcher import Searcher
        with _searcher_lock:
            _searcher = Searcher()
        _search_ready.set()
        _push("DONE:Search ready")

    except Exception as e:
        import traceback
        _push(f"ERROR:{traceback.format_exc()}")

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)