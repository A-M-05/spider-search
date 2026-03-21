from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw" / "pages"
CRAWL_STATE_DIR = DATA_DIR / "crawl_state"
ANALYTICS_DIR = DATA_DIR / "analytics"
INDEX_DIR = DATA_DIR / "index"

PARTIAL_DIR = INDEX_DIR / "partial"
MERGED_DIR = INDEX_DIR / "merged"
EXTRAS_DIR = INDEX_DIR / "extras"

DOC_TABLE_PATH = INDEX_DIR / "doc_table.tsv"
MERGED_POSTINGS_PATH = MERGED_DIR / "postings.tsv"
DICTIONARY_PATH = MERGED_DIR / "dictionary.tsv"
DOC_LENGTHS_PATH = MERGED_DIR / "doc_lengths.tsv"
COLLECTION_STATS_PATH = MERGED_DIR / "collection_stats.tsv"
BIGRAM_INDEX_PATH = MERGED_DIR / "bigram_index.tsv"

EXACT_DUPLICATES_PATH = EXTRAS_DIR / "exact_duplicates.tsv"
NEAR_DUPLICATES_PATH = EXTRAS_DIR / "near_duplicates.tsv"
GRAPH_EDGES_PATH = EXTRAS_DIR / "graph_edges.tsv"
PAGERANK_PATH = EXTRAS_DIR / "pagerank.tsv"
