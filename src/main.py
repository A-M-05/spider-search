import sys


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m src.main [command] [args]")
        print()
        print("Commands:")
        print("  crawl [url]   Crawl a domain (url overrides config/crawler.ini)")
        print("  build-index   Build inverted index from crawled pages")
        print("  merge         Merge partial index files")
        print("  graph         Build hyperlink graph")
        print("  pagerank      Compute PageRank scores")
        print("  search        Interactive command-line search")
        print("  pipeline      Run build-index → merge → graph → pagerank → search")
        print("  gui           Launch graphical search interface")
        return

    command = sys.argv[1]

    if command == "crawl":
        from .crawler.crawl_runner import run_crawler
        seed_url = sys.argv[2] if len(sys.argv) > 2 else None
        run_crawler(seed_url=seed_url, restart=True)

    elif command == "build-index":
        print("=== BUILDING INDEX ===")
        from .indexing.build_index import build_index
        build_index()

    elif command == "merge":
        print("=== MERGING PARTIALS ===")
        from .indexing.merge_partials import merge_partials
        merge_partials()

    elif command == "graph":
        print("=== BUILDING LINK GRAPH ===")
        from .ranking.link_graph_builder import build_graph
        build_graph()
        print("Link graph complete.")

    elif command == "pagerank":
        print("=== COMPUTING PAGERANK ===")
        from .ranking.pagerank import build_pagerank
        build_pagerank()

    elif command == "search":
        print("=== STARTING SEARCH ===")
        from .search.run_search import run_search
        run_search()

    elif command == "pipeline":
        print("=== RUNNING PIPELINE ===")
        from .pipeline.pipeline import main as run_pipeline
        run_pipeline()

    elif command == "gui":
        print("=== LAUNCHING GUI ===")
        import tkinter as tk
        from .ui.search_gui import SearchApp
        root = tk.Tk()
        app = SearchApp(root)
        root.mainloop()

    else:
        print(f"Unknown command: '{command}'")
        print("Run 'python -m src.main' with no arguments to see usage.")


if __name__ == "__main__":
    main()
