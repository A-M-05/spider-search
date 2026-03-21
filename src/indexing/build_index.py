from ..common.paths import (
    DOC_TABLE_PATH, PARTIAL_DIR, RAW_DATA_DIR,
    DOC_LENGTHS_PATH, COLLECTION_STATS_PATH, BIGRAM_INDEX_PATH, EXACT_DUPLICATES_PATH,
    NEAR_DUPLICATES_PATH
)

from ..common.search_constants import (
    FIELD_WEIGHTS, 
)
from ..indexing.accumulator import IndexAccumulator
from ..io.dataset_reader import iterate_documents
from ..text.html_extractor import extract_fields
from ..text.tokstem import normalize
from ..quality.duplicate_detector import DuplicateDetector
import math

def build_index():
    """
    Build the inverted index from the document collection.

    This function iterates through the dataset, assigns document IDs,
    extracts structured text fields from HTML, normalizes tokens, and
    constructs postings data used for ranked retrieval. It applies field
    weighting, tracks positional information for phrase scoring, and
    computes document vector lengths for cosine normalization.

    The function also collects and applies anchor text, generates bigrams
    for proximity boosting, and performs both exact and near-duplicate
    detection. Postings are accumulated in memory and periodically flushed
    to disk as partial index files for later merging.

    Outputs written to disk include:
    - document table (docID → URL mapping)
    - document length table
    - partial postings files
    - collection statistics
    - bigram index
    - exact duplicate log
    - near-duplicate log

    A summary of indexing statistics is printed after completion.

    :return: None
    """

    # Core bookkeeping for the indexing pass:
    # - seen_urls prevents indexing the same normalized URL twice
    # - doc_count tracks how many documents were actually indexed
    # - new_doc_id is the next docID assigned to a kept document
    # - partial_num tracks how many partial index files were written
    # - total_postings_written is a rough summary statistic for the build
    # - duplicate_detector handles exact and near-duplicate checks
    # - accumulator stores postings in memory until they are flushed to disk
    seen_urls = set()
    doc_count = 0
    new_doc_id = 0
    partial_num = 0
    total_postings_written = 0
    duplicate_detector = DuplicateDetector()
    accumulator = IndexAccumulator()

    # Anchor text is collected while scanning pages and temporarily stored
    # by target URL. Later, when the target page itself is indexed, these
    # anchor terms are folded into that page's term counts.
    # Key: normalized target URL
    # Value: list of anchor text strings pointing to that target
    anchor_map = {}

    # Make sure every output directory exists before writing files.
    # This prevents file-open failures when the pipeline is run on a clean
    # repository with no pre-created output folders.
    DOC_TABLE_PATH.parent.mkdir(parents=True, exist_ok=True)
    PARTIAL_DIR.mkdir(parents=True, exist_ok=True)
    DOC_LENGTHS_PATH.parent.mkdir(parents=True, exist_ok=True)
    COLLECTION_STATS_PATH.parent.mkdir(parents=True, exist_ok=True)
    BIGRAM_INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    EXACT_DUPLICATES_PATH.parent.mkdir(parents=True, exist_ok=True)
    NEAR_DUPLICATES_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(DOC_TABLE_PATH, "w") as doc_table, \
         open(DOC_LENGTHS_PATH, "w") as doc_lengths, \
         open(COLLECTION_STATS_PATH, "w") as collection_stats, \
         open(BIGRAM_INDEX_PATH, "w") as bigram_index, \
         open(EXACT_DUPLICATES_PATH, "w") as exact_duplicates, \
         open(NEAR_DUPLICATES_PATH, "w") as near_duplicates:

        # Scan every JSON document in the dataset. iterate_documents yields:
        # (original_doc_id, normalized_url, html_content)
        # We ignore the original dataset doc id because this pipeline assigns
        # its own contiguous docIDs only to pages that survive filtering.
        for _, url, html in iterate_documents(RAW_DATA_DIR):

            # URL-level deduplication is the cheapest possible filter, so it
            # happens first. If the same normalized URL was already indexed,
            # there is no reason to reprocess the page at all.
            if url in seen_urls:
                continue
            
            # Extract structured text fields plus outbound links from the raw
            # HTML. The fields drive indexing and duplicate detection; the
            # links are used for anchor text collection and later graph-based
            # analysis such as PageRank
            fields, links = extract_fields(url, html)

            # ------------ ANCHOR TEXT COLLECTION ------------
            # Collect anchor text now, before any duplicate-based skipping.
            # Even if the current page is later skipped as duplicate content,
            # its outgoing anchor text can still be useful to the true target
            # pages in the collection.
            for target_url, anchor_text in links:
                if target_url not in anchor_map:
                    anchor_map[target_url] = []
                anchor_map[target_url].append(anchor_text)
            # --------------------------------------------------------

            # ------------ EXACT DUPLICATE PAGE DETECTION ------------
            # Exact duplicate detection compares the normalized extracted text
            # of the current page against previously accepted pages. If the
            # page is a duplicate, we log the relationship and skip indexing
            # it entirely so it does not pollute postings, doc lengths, or
            # ranking behavior.
            is_exact_dup, kept_doc_id, kept_url = duplicate_detector.check_exact(
                url = url,
                fields = fields,
                current_doc_id = new_doc_id
            )

            if is_exact_dup:
                exact_duplicates.write(f"{url}\t{kept_doc_id}\t{kept_url}\n")
                continue
            # --------------------------------------------------------

            # At this point the page has survived URL and exact-content
            # filtering, so it becomes a real indexed document. Record the
            # final docID → URL mapping used later during retrieval.
            doc_table.write(f"{new_doc_id}\t{url}\n")
            seen_urls.add(url)

            # local_counts stores weighted term frequencies for the document.
            # local_positions stores token positions for each term so phrase
            # and adjacency-style matching can be supported later.
            local_counts = dict()
            local_positions = dict()
            position_counter = 0

            # Build the document representation from the extracted fields.
            # Each field contributes tokens with a configurable weight
            # (title/headings/bold/body), while positions are tracked in one
            # unified running sequence for positional retrieval.
            for field_name in ["title", "headings", "bold", "body"]:
                text = fields[field_name]
                tokens = normalize(text)
                weight = FIELD_WEIGHTS.get(field_name, 1.0)

                for token in tokens:
                    local_counts[token] = local_counts.get(token, 0) + weight

                    # Track position for each token
                    if token not in local_positions:
                        local_positions[token] = []
                    local_positions[token].append(position_counter)
                    position_counter += 1

            # ------------ ANCHOR TEXT PROCESSING ------------
            # If other pages linked to this page, incorporate those anchor
            # words into the target page's term counts. This gives the target
            # page extra evidence from how other pages describe it. After the
            # anchor text is applied, remove it from anchor_map to free memory.
            if url in anchor_map:
                anchor_weight = FIELD_WEIGHTS.get("anchor", 2.0)
                for anchor_text in anchor_map[url]:
                    anchor_tokens = normalize(anchor_text)
                    for token in anchor_tokens:
                        local_counts[token] = local_counts.get(token, 0) + anchor_weight
                
                del anchor_map[url]
            # --------------------------------------------------------

            # ------------ NEAR DUPLICATE PAGE DETECTION ------------
            # Near-duplicate detection is looser than exact duplicate
            # detection. These pages are not removed automatically here;
            # instead, they are logged so the system can inspect or use the
            # information later without aggressively discarding content.
            near_matches = duplicate_detector.check_near(
                url=url,
                current_doc_id=new_doc_id,
                local_counts=local_counts
            )

            for prior_doc_id, prior_url, distance in near_matches:
                near_duplicates.write(
                    f"{new_doc_id}\t{url}\t{prior_doc_id}\t{prior_url}\t{distance}\n"
                )
            # --------------------------------------------------------

            # Build a simple bigram file from the body text. This supports
            # query-time proximity/phrase-style boosting by allowing the
            # searcher to quickly check whether adjacent normalized terms
            # appear together in a document.
            body_tokens = normalize(fields['body'])
            for i in range(len(body_tokens) - 1):
                bigram = body_tokens[i] + '_' + body_tokens[i + 1]
                bigram_index.write(f"{new_doc_id}\t{bigram}\n")

            # Compute the document vector length used for cosine-style score
            # normalization at query time. The weighting scheme matches the
            # retrieval model: document term weight is 1 + log10(weighted_tf).
            sum_sq = 0
            for weighted_tf in local_counts.values():
                if weighted_tf > 0:
                    # w_td = 1 + log10(tf)
                    w_td = 1 + math.log10(weighted_tf)
                    sum_sq += w_td ** 2

            doc_length = math.sqrt(sum_sq)

            # Persist the document length so retrieval can normalize scores
            # without recomputing document vectors from scratch.
            doc_lengths.write(f"{new_doc_id}\t{doc_length}\n")

            # Add the accepted document's positional postings to the in-memory
            # accumulator. The accumulator groups terms until a configured
            # threshold is reached, then flushes them as a sorted partial
            # index on disk.
            accumulator.add_document(doc_id=new_doc_id, local_positions=local_positions)
            doc_count += 1
            new_doc_id += 1

            # When the accumulator becomes too large, flush it to a partial
            # index file. This keeps memory usage bounded and allows the
            # overall index build to scale beyond RAM.
            if accumulator.should_flush():
                total_postings_written += accumulator.postings_count
                accumulator.flush(PARTIAL_DIR / f"partial_{partial_num}.tsv")
                partial_num += 1
        
        # After the full dataset scan is done, there may still be postings
        # left in memory that were never large enough to trigger a flush.
        # Flush one final partial file so no indexed content is lost.
        if not accumulator.is_empty():
            total_postings_written += accumulator.postings_count
            accumulator.flush(PARTIAL_DIR / f"partial_{partial_num}.tsv")
            partial_num += 1

        # Write collection-level statistics needed during retrieval, such as
        # the total number of indexed documents used in IDF computation.
        collection_stats.write(f"total_docs\t{doc_count}\n")

    # Print a compact summary so the build can be sanity-checked quickly
    # without opening output files manually.
    print("--------------------------------------------------")
    print("Finished building the inverted index.")
    print(f"\tNumber of documents processed: {doc_count}")
    print(f"\tNumber of partials created:    {partial_num}")
    print(f"\tNumber of postings written:    {total_postings_written}")
    print(f"\tRemaining orphan anchors:      {len(anchor_map)}") 
    print("--------------------------------------------------")

if __name__ == "__main__":
    build_index()