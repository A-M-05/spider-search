from ..common.paths import BIGRAM_INDEX_PATH


class BigramReader:
    """
    Load and query the bigram index used for candidate filtering.

    The bigram index maps:
        bigram -> set(doc_id)

    It is typically used during search preprocessing to reduce the
    candidate document set before full scoring.
    """

    def __init__(self):
        """
        Initialize the reader and load the bigram index into memory.

        :return: None
        """

        # Internal mapping: bigram -> set of docIDs containing it.
        self._index = {}
        self._load()

    def _load(self):
        """
        Load the bigram index from disk into memory.

        Expected file format (one entry per line):
            doc_id<TAB>bigram

        Invalid lines are skipped silently to keep loading robust.

        :raises FileNotFoundError: If the bigram index file is missing.
        :return: None
        """

        # Stop immediately if the index does not exist.
        if not BIGRAM_INDEX_PATH.exists():
            raise FileNotFoundError(f"[DEBUG] Bigram index not found at {BIGRAM_INDEX_PATH}")

        # Read line-by-line to avoid unnecessary memory overhead.
        with open(BIGRAM_INDEX_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                parts = line.split("\t", 1)
                if len(parts) != 2:
                    continue

                doc_id = int(parts[0])
                bigram = parts[1]

                # Insert into the in-memory inverted mapping.
                if bigram not in self._index:
                    self._index[bigram] = set()

                self._index[bigram].add(doc_id)

    def get_docs(self, bigram: str) -> set[int]:
        """
        Retrieve all documents containing a given bigram.

        :param bigram: Bigram string to look up.
        :type bigram: str
        :return: Set of docIDs containing the bigram.
        :rtype: set[int]
        """
        
        # Return empty set instead of None to simplify caller logic.
        return self._index.get(bigram, set())
