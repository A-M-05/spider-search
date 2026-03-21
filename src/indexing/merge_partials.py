from ..indexing.merger import Merger
from ..common.paths import MERGED_POSTINGS_PATH, DICTIONARY_PATH, DOC_TABLE_PATH


def merge_partials() -> None:
    """
    Execute the final merge step of the indexing pipeline.

    This function calls the Merger to combine all partial index files into
    a single merged postings file and dictionary. After the merge completes,
    it computes and prints the on-disk sizes of the merged postings file,
    dictionary file, and document table to summarize total index storage.

    Files involved:
    - MERGED_POSTINGS_PATH: final postings file produced by merging
    - DICTIONARY_PATH: final dictionary mapping terms to metadata
    - DOC_TABLE_PATH: document ID to URL mapping table

    Output:
    - Prints index size statistics (bytes, KB, MB)

    :return: None
    """
    
    # Run the merge
    Merger().merge_partials()

    # After merge completes, compute on-disk sizes
    postings_size = MERGED_POSTINGS_PATH.stat().st_size if MERGED_POSTINGS_PATH.exists() else 0
    dictionary_size = DICTIONARY_PATH.stat().st_size if DICTIONARY_PATH.exists() else 0
    doc_table_size = DOC_TABLE_PATH.stat().st_size if DOC_TABLE_PATH.exists() else 0

    total_size = postings_size + dictionary_size + doc_table_size

    print("Finished merging.")
    print(f"\tPostings size (bytes): {postings_size}")
    print(f"\tDictionary size (bytes): {dictionary_size}")
    print(f"\tDoc table size (bytes): {doc_table_size}")
    print(f"\tTotal index size (bytes): {total_size}")
    print(f"\tTotal index size (KB): {total_size / 1024:.2f}")
    print(f"\tTotal index size (MB): {total_size / (1024 * 1024):.2f}")
