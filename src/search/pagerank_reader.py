from ..common.paths import PAGERANK_PATH

class PageRankReader:
    """
    Load and provide access to PageRank scores for documents.

    The PageRank file maps:
        doc_id -> pagerank_score

    Scores are stored in a list so that doc_id can be used directly
    as an index for constant-time lookup during ranking.
    """

    def __init__(self):
        """
        Initialize the PageRank reader and load scores into memory.

        :return: None
        """

        self._scores = []
        self._file = PAGERANK_PATH
        self.load()

    def load(self) -> None:
        """
        Load PageRank scores from disk.

        Expected file format:
            doc_id<TAB>score

        Malformed lines are skipped silently.

        :raises FileNotFoundError: If the PageRank file does not exist.
        :return: None
        """

        if not self._file.exists():
            raise FileNotFoundError(f"PageRank file not found at {self._file}")

        with open(self._file, "r", encoding="utf-8") as pagerank_file:
            for line in pagerank_file:
                line = line.strip()
                if not line:
                    continue

                parts = line.split("\t", 1)
                if len(parts) != 2:
                    continue

                doc_id = int(parts[0])
                score = float(parts[1])

                # Expand the list if needed so doc_id can be used directly
                # as an index into the PageRank array.
                if doc_id >= len(self._scores):
                    self._scores.extend([0.0] * (doc_id + 1 - len(self._scores)))

                self._scores[doc_id] = score

    def get_score(self, doc_id: int) -> float:
        """
        Retrieve the PageRank score for a document.

        :param doc_id: Document identifier.
        :type doc_id: int
        :return: PageRank score, or 0.0 if the doc_id is missing.
        :rtype: float
        """
        
        if 0 <= doc_id < len(self._scores):
            return self._scores[doc_id]

        return 0.0
