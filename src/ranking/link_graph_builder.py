from ..common.paths import DOC_TABLE_PATH, GRAPH_EDGES_PATH, RAW_DATA_DIR
from ..io.dataset_reader import iterate_documents
from ..text.html_extractor import extract_fields

def load_doc_table():
    """
    Load the document table into memory as a URL-to-docID mapping.

    The document table is produced during indexing and stores the final
    mapping from each indexed page's normalized URL to its assigned docID.
    This mapping is required for graph construction so that hyperlinks
    discovered in pages can be converted into edges between document IDs.

    :return: Mapping from normalized URL to indexed document ID.
    :rtype: dict[str, int]
    :raises FileNotFoundError: If the document table does not exist.
    """
    
    # Store the final indexed mapping:
    # key   -> normalized URL
    # value -> docID assigned during indexing
    url_to_doc_id = {}

    # The graph cannot be built unless the index has already produced a
    # doc table, so fail immediately if the file is missing.
    if not DOC_TABLE_PATH.exists():
        raise FileNotFoundError
    
    # Read each line of the doc table and reconstruct the URL -> docID map
    # used later to convert hyperlinks into graph edges.
    with open(DOC_TABLE_PATH, "r") as doc_table:
        for line in doc_table:
            line = line.strip()
            if not line:
                continue
            l = line.split('\t', 1)
            doc_id, url = int(l[0]), l[1]
            url_to_doc_id[url] = doc_id
    return url_to_doc_id


def build_graph():
    """
    Build the internal hyperlink graph over indexed documents.

    This function scans the crawled dataset again, extracts outbound links
    from each page, and converts valid links into edges between indexed
    document IDs. Only edges where both the source page and target page
    exist in the final indexed collection are written to the graph file.

    Duplicate edges are removed so each directed source-target pair appears
    only once in the output.

    Output format:
    - graph_edges.tsv containing lines of the form:
      source_doc_id<TAB>target_doc_id

    :return: None
    """

    # Load the final set of indexed URLs so graph edges are only created
    # between pages that actually survived indexing and were assigned docIDs.
    url_to_doc_id = load_doc_table()

    # Track edges already written so repeated hyperlinks between the same
    # source and target do not create duplicate graph entries.
    seen_edges = set()

    # Ensure the graph output directory exists before writing.
    GRAPH_EDGES_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(GRAPH_EDGES_PATH, "w") as graph_edges:
        # Re-scan the dataset page by page so we can recover outbound links
        # from each source page's HTML.
        for _, source_url, html in iterate_documents(RAW_DATA_DIR):
            # If the source page was not kept in the final index, it does not
            # belong in the graph used for PageRank, so skip it.
            if source_url not in url_to_doc_id:
                continue

            source_doc_id = url_to_doc_id[source_url]

            # We only need the extracted links here. The textual fields are
            # ignored because graph building is based solely on hyperlink
            # structure.
            _, links = extract_fields(source_url, html)

            # Convert each target URL into a target docID if that target page
            # also exists in the indexed collection.
            for target_url, _ in links:
                if target_url in url_to_doc_id:
                    target_doc_id = url_to_doc_id[target_url]
                    
                    # Ignore self-links and skip edges already written.
                    if target_doc_id != source_doc_id:
                        edge = (source_doc_id, target_doc_id)
                        if edge in seen_edges:
                            continue
                        else:
                            seen_edges.add(edge)
                            graph_edges.write(f"{source_doc_id}\t{target_doc_id}\n")
