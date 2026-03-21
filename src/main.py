import sys

from .pipeline.pipeline import main as run_pipeline
from .indexing.build_index import build_index
from .indexing.merge_partials import merge_partials
from .ranking.link_graph_builder import build_graph
from .ranking.pagerank import build_pagerank
# from .ui.search_gui import launch_gui
from .crawler.crawl_runner import run_crawler


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m src.main [crawl|build-index|merge|graph|pagerank|pipeline|gui]")
        return

    command = sys.argv[1]

    if command == "crawl":
        run_crawler()
    elif command == "build-index":
        build_index()
    elif command == "merge":
        merge_partials()
    elif command == "graph":
        build_graph()
    elif command == "pagerank":
        build_pagerank()
    elif command == "pipeline":
        run_pipeline()
    # elif command == "gui":
        # launch_gui()
    else:
        print(f"Unknown command: {command}")