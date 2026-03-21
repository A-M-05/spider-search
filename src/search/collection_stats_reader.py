from ..common.paths import COLLECTION_STATS_PATH


class CollectionStatsReader:
    """
    Load global collection statistics required during retrieval.

    These statistics are computed during indexing and stored on disk.
    Currently the only required value is the total number of documents
    in the collection, which is needed for scoring formulas such as IDF.
    """

    def __init__(self):
        """
        Initialize the reader and load statistics from disk.

        :return: None
        """

        self._total_docs = 0
        self._load()

    def _load(self):
        """
        Load collection statistics from the stats file.

        Expected file format:
            key<TAB>value

        :raises FileNotFoundError: If the stats file does not exist.
        :return: None
        """

        if not COLLECTION_STATS_PATH.exists():
            raise FileNotFoundError(
                f"[DEBUG] Collection stats file not found at {COLLECTION_STATS_PATH}"
            )

        with open(COLLECTION_STATS_PATH, "r") as collection:
            for line in collection:
                line = line.strip()
                if not line:
                    continue

                parts = line.split("\t")

                # Extract the total number of indexed documents.
                if parts[0] == "total_docs":
                    self._total_docs = int(parts[1])

    def total_docs(self) -> int:
        """
        Return the total number of documents in the collection.

        This value is used by ranking algorithms that rely on
        global corpus statistics.

        :return: Total number of indexed documents.
        :rtype: int
        """
        
        return self._total_docs
