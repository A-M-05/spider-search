from pathlib import Path
from ..common.search_constants import ACCUMULATOR_THRESHOLD
from collections import defaultdict


class IndexAccumulator:
    """
    Accumulate in-memory postings before flushing partial index files to disk.

    This class stores term postings for a batch of indexed documents so the
    index builder can write partial inverted indexes incrementally instead of
    keeping the full collection in memory at once.

    In this version, postings are positional:
    - each term maps to a dictionary of docIDs
    - each docID maps to a list of token positions

    The accumulator supports:
    - adding a document's positional postings
    - checking whether memory should be flushed
    - writing sorted partial postings files to disk
    """

    def __init__(self):
        """
        Initialize the in-memory accumulator.

        - index_map stores positional postings grouped by term and document
        - postings_count tracks the number of unique term-document postings
        - threshold controls when the accumulator should be flushed to disk

        :return: None
        """

        self.index_map = {}
        self.postings_count = 0
        self.threshold = ACCUMULATOR_THRESHOLD

    def define_threshold(self, threshold: int) -> None:
        """
        Override the accumulator flush threshold.

        This can be useful for testing or tuning memory usage during indexing.

        :param threshold: Maximum number of term-document postings allowed
                          before a flush is triggered.
        :type threshold: int
        :return: None
        """

        self.threshold = threshold

    def add_document(self, doc_id: int, local_positions: dict[str, list[int]]) -> None:
        """
        Add a document's positional postings into the accumulator.

        For each term in the document, this method inserts or updates the
        postings list entry for that docID and extends the stored list of
        positions. A new term-document pair increases postings_count.

        :param doc_id: Unique docID assigned to the current indexed document.
        :type doc_id: int
        :param local_positions: Mapping from term to list of positions where
                                that term appears in the document.
        :type local_positions: dict[str, list[int]]
        :return: None
        """

        # Merge this document's positional postings into the global in-memory
        # structure. Each new (term, doc_id) pair counts as one posting.
        for term, positions in local_positions.items():
            if term not in self.index_map:
                self.index_map[term] = {}

            if doc_id not in self.index_map[term]:
                self.index_map[term][doc_id] = []
                self.postings_count += 1

            self.index_map[term][doc_id].extend(positions)

    def unique_terms(self) -> int:
        """
        Return the number of distinct terms currently stored.

        :return: Number of unique terms in the accumulator.
        :rtype: int
        """

        return len(self.index_map)

    def build_positions(self, tokens):
        """
        Build a positional index for a token sequence from one document.

        This helper converts an ordered token list into a mapping from
        term -> list of positions where the term appears.

        :param tokens: Ordered list of normalized tokens from a document.
        :type tokens: list[str]
        :return: Mapping from each term to its positions.
        :rtype: dict[str, list[int]]
        """

        # Use a defaultdict so positions can be appended naturally during a
        # single pass over the token sequence.
        positions = defaultdict(list)
        for i, token in enumerate(tokens):
            positions[token].append(i)
        return dict(positions)

    def should_flush(self) -> bool:
        """
        Determine whether the accumulator has reached its flush threshold.

        Once postings_count reaches or exceeds the configured threshold,
        the current in-memory postings should be written to a partial
        index file on disk.

        :return: True if the accumulator should be flushed, else False.
        :rtype: bool
        """

        return self.postings_count >= self.threshold

    def flush(self, output_path: Path):
        """
        Write the current accumulator contents to a partial index file.

        Terms are written in sorted order, and within each term, docIDs are
        also written in sorted order. Positional postings are serialized as:

            term<TAB>doc_id:pos1,pos2,pos3 doc_id:pos1,pos2 ...

        After writing, the accumulator is cleared so indexing can continue
        with a fresh in-memory batch.

        :param output_path: Destination path for the partial index file.
        :type output_path: Path
        :return: None
        """

        # If nothing is stored, there is nothing to write.
        if len(self.index_map) == 0:
            return

        with open(output_path, "w") as out:
            # Terms must be sorted so later merge stages can perform an
            # efficient multi-way merge over all partial files.
            terms = sorted(self.index_map.keys())

            for term in terms:
                postings_dict = self.index_map[term]

                # DocIDs are sorted for stable postings order and easier
                # downstream retrieval processing.
                doc_ids = sorted(postings_dict.keys())

                parts = []
                for d in doc_ids:
                    # Serialize positional postings as a comma-separated
                    # positions list attached to the docID.
                    positions = postings_dict[d]
                    pos_string = ",".join(str(p) for p in positions)
                    parts.append(f"{d}:{pos_string}")

                postings_string = " ".join(parts)
                out.write(f"{term}\t{postings_string}\n")

        # Clear all in-memory data after writing the partial file so memory
        # usage stays bounded across the indexing run.
        self.index_map.clear()
        self.postings_count = 0

    def is_empty(self) -> bool:
        """
        Check whether the accumulator currently holds any postings.

        :return: True if no postings are stored, else False.
        :rtype: bool
        """

        return len(self.index_map) == 0
