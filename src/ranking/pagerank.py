from ..common.paths import (
    GRAPH_EDGES_PATH, COLLECTION_STATS_PATH, PAGERANK_PATH
)

from ..common.search_constants import (
    PAGERANK_ALPHA, PAGERANK_ITERATIONS
)

def load_total_docs():
    """
    Load the total number of indexed documents from collection statistics.

    The collection statistics file is produced during indexing and stores
    summary metadata such as the total number of documents in the final
    indexed collection. PageRank needs this value to initialize scores
    and apply the damping formula correctly.

    :return: Total number of indexed documents.
    :rtype: int
    :raises FileNotFoundError: If the collection statistics file is missing.
    :raises ValueError: If the total document count cannot be found.
    """
    
    # PageRank cannot be computed unless the indexing stage already wrote
    # collection-level metadata, so fail immediately if that file is missing.
    if not COLLECTION_STATS_PATH.exists():
        raise FileNotFoundError

    # Scan the stats file until the total_docs entry is found.
    with open(COLLECTION_STATS_PATH, "r") as collection_stats:
        for line in collection_stats:
            line = line.strip()

            if not line:
                continue

            parts = line.split("\t", 1)
            if len(parts) != 2:
                continue

            key, value = parts[0], parts[1]

            if key == "total_docs":
                return int(value)

    # If the file exists but the expected key was never found, treat that
    # as an invalid stats file.
    raise ValueError


def load_graph(n: int):
    """
    Load the hyperlink graph used for PageRank computation.

    The graph file contains directed edges of the form:
        source_doc_id<TAB>target_doc_id

    This function reconstructs:
    - outgoing adjacency sets for each document
    - outdegree counts for each document

    Duplicate edges are ignored, and malformed or out-of-range edges
    are skipped.

    :param n: Total number of indexed documents.
    :type n: int
    :return: Tuple of (outgoing adjacency list, outdegree list).
    :rtype: tuple[list[set[int]], list[int]]
    :raises FileNotFoundError: If the graph edge file is missing.
    """

    # The hyperlink graph must already be built before PageRank runs.
    if not GRAPH_EDGES_PATH.exists():
        raise FileNotFoundError

    # outgoing[u] contains the set of documents that document u links to.
    # outdegree[u] stores the number of unique outgoing links from u.
    outgoing = [set() for _ in range(n)]
    outdegree = [0] * n

    # Read the graph edge file and populate adjacency information.
    with open(GRAPH_EDGES_PATH, "r") as graph_edges:
        for line in graph_edges:
            line = line.strip()

            if not line:
                continue

            parts = line.split("\t", 1)
            if len(parts) != 2:
                continue

            source_doc_id, target_doc_id = int(parts[0]), int(parts[1])

            # Ignore malformed edges that reference nonexistent docIDs.
            if not ((0 <= source_doc_id < n) and (0 <= target_doc_id < n)):
                continue

            # Since outgoing is a set, duplicate edges are naturally ignored.
            if target_doc_id not in outgoing[source_doc_id]:
                outgoing[source_doc_id].add(target_doc_id)
                outdegree[source_doc_id] += 1

    return (outgoing, outdegree)


def compute_pagerank(n: int, outgoing, outdegree):
    """
    Compute PageRank scores over the document graph.

    This implementation uses the standard iterative PageRank update with:
    - damping factor alpha
    - uniform initialization
    - dangling-node redistribution

    Each iteration distributes score mass along outgoing links, while
    dangling nodes (pages with no outgoing links) contribute their mass
    uniformly across the full collection.

    :param n: Total number of indexed documents.
    :type n: int
    :param outgoing: Outgoing adjacency list for each document.
    :type outgoing: list[set[int]]
    :param outdegree: Number of outgoing links for each document.
    :type outdegree: list[int]
    :return: Final PageRank score for every document.
    :rtype: list[float]
    """

    alpha = PAGERANK_ALPHA
    iterations = PAGERANK_ITERATIONS

    # Start with a uniform probability distribution across all documents.
    rank = [1.0 / n] * n

    # Repeatedly refine PageRank scores using the damping formula.
    for _ in range(iterations):
        # Base teleportation probability applied to every document.
        new_rank = [(1.0 - alpha) / n] * n

        # Accumulate score mass from dangling nodes so it can later be
        # redistributed uniformly.
        dangling_mass = 0.0

        for doc_u in range(n):
            if outdegree[doc_u] == 0:
                dangling_mass += rank[doc_u]
            else:
                contribution = alpha * rank[doc_u] / outdegree[doc_u]

                # Distribute document u's share equally across all pages it
                # links to.
                for target_v in outgoing[doc_u]:
                    new_rank[target_v] += contribution

        # Spread dangling-node mass evenly over the whole graph.
        dangling_share = alpha * dangling_mass / n

        for doc_d in range(n):
            new_rank[doc_d] += dangling_share

        rank = new_rank

    return rank


def write_pagerank(rank: list[float]):
    """
    Write final PageRank scores to disk.

    Output format:
        doc_id<TAB>pagerank_score

    This file is later loaded during retrieval so PageRank can be combined
    with content-based ranking signals.

    :param rank: Final PageRank scores for all documents.
    :type rank: list[float]
    :return: None
    """

    # Ensure the output directory exists before writing the results.
    PAGERANK_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Write one score per docID so retrieval can load scores by index.
    with open(PAGERANK_PATH, "w") as pagerank_table:
        for doc_id in range(len(rank)):
            pagerank_table.write(f"{doc_id}\t{rank[doc_id]}\n")


def build_pagerank():
    """
    Build PageRank scores from the indexed hyperlink graph.

    This function orchestrates the full PageRank pipeline:
    1. load collection size
    2. load graph structure
    3. compute iterative PageRank scores
    4. write scores to disk
    5. print summary statistics

    :return: None
    """

    # Load the number of indexed documents and reconstruct the hyperlink
    # graph over those documents.
    n = load_total_docs()
    outgoing, outdegree = load_graph(n)

    # Count unique edges for summary reporting.
    total_edges = sum(len(neighbors) for neighbors in outgoing)

    # Compute PageRank scores and persist them for retrieval-time use.
    rank = compute_pagerank(n, outgoing, outdegree)
    write_pagerank(rank)

    # The final score distribution should sum to approximately 1.
    rank_sum = sum(rank)

    # Print summary information so the PageRank build can be sanity-checked.
    print("PageRank Summary:")
    print(f"\tTotal Documents:     {n}")
    print(f"\tTotal Edges:         {total_edges}")
    print(f"\tAlpha:               {PAGERANK_ALPHA}")
    print(f"\tIterations:          {PAGERANK_ITERATIONS}")
    print(f"\tOutput Path:         {PAGERANK_PATH}")
    print(f"\tPageRank Scores Sum: {rank_sum}")


if __name__ == "__main__":
    build_pagerank()
