from .searcher import Searcher

def run_search():
    """
    Run an interactive command-line search session.

    This function initializes the Searcher (which loads all index
    structures into memory), then repeatedly prompts the user for
    queries until they choose to exit. Each query is normalized and
    scored using the retrieval pipeline, and the top-ranked results
    are displayed in ranked order.

    Behavior:
    - Empty queries are ignored.
    - Typing 'quit' exits the loop.
    - Up to 15 results are returned per query.

    Resources used by the Searcher (already built beforehand):
    - dictionary + postings
    - document table
    - document lengths
    - collection statistics
    - PageRank scores
    - bigram index

    :return: None
    """

    # Initialize the Searcher once at startup. This loads all required
    # index structures from disk into memory so queries can be answered
    # quickly without repeated file I/O.
    searcher = Searcher()

    print("Type 'quit' to quit")
    
    # Main interactive loop: continuously prompt the user for queries
    # until they explicitly choose to exit.
    while True:
        query = input("Query: ").strip()

        # Ignore empty input so accidental Enter presses do not trigger
        # unnecessary search calls.
        if not query:
            continue
        
        # Allow users to exit cleanly using a simple command.
        if query.lower() == 'quit':
            break

        # Execute ranked retrieval using the search pipeline.
        # top_k controls how many results are returned.
        results = searcher.search(query, top_k = 15)

        # Display results in ranked order. If nothing matches,
        # print a clear message so the user understands.
        if not results:
            print('No results.\n')
        else:
            print(f'Top {len(results)} results: ')
            for i in range(len(results)):
                print(f'.  {i+1}. {results[i]}')
            print()
    
    # Always close the searcher at exit to release open file handles
    # (especially postings file handles used during retrieval).
    searcher.close()
