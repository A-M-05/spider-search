from ..indexing.build_index import build_index
from ..ranking.link_graph_builder import build_graph
from ..ranking.pagerank import build_pagerank
from ..search.run_search import run_search

from ..common.paths import (
    DOC_TABLE_PATH,
    GRAPH_EDGES_PATH,
    PAGERANK_PATH,
)

def file_ready(path):
    return path.exists() and path.stat().st_size > 0


def main():
    print("=== PIPELINE START ===")

    # ---------------- BUILD INDEX ----------------
    if file_ready(DOC_TABLE_PATH):
        print("Index already exists. Skipping build_index.")
    else:
        print("=== BUILDING INDEX ===")
        build_index()

    # ---------------- LINK GRAPH ----------------
    if file_ready(GRAPH_EDGES_PATH):
        print("Graph already exists. Skipping link_graph_builder.")
    else:
        print("=== BUILDING LINK GRAPH ===")
        build_graph()

    # ---------------- PAGERANK ----------------
    if file_ready(PAGERANK_PATH):
        print("PageRank already exists. Skipping pagerank.")
    else:
        print("=== RUNNING PAGERANK ===")
        build_pagerank()

    # ---------------- SEARCH ----------------
    print("=== STARTING SEARCH ===")
    run_search()

    print("=== PIPELINE DONE ===")


if __name__ == "__main__":
    main()
