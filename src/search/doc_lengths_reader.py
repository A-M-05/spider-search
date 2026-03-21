from ..common.paths import DOC_LENGTHS_PATH

class DocLengthsReader:
    """
    Load and provide access to precomputed document length values.

    The doc lengths file maps:
        doc_id -> normalized document length

    These values are typically used during ranking (e.g., cosine or BM25)
    to normalize term contributions across documents.
    """

    def __init__(self):
        """
        Initialize the reader and load document lengths into memory.

        :return: None
        """

        # Internal structure where index = doc_id and value = length.
        self._lengths = []
        self._load()

    def _load(self):
        """
        Load document lengths from disk.

        Expected file format:
            doc_id<TAB>length

        Missing or malformed lines are skipped silently.

        :raises FileNotFoundError: If the doc lengths file does not exist.
        :return: None
        """

        if not DOC_LENGTHS_PATH.exists():
            raise FileNotFoundError(
                f"[DEBUG] Doc lengths file not found at {DOC_LENGTHS_PATH}"
            )

        with open(DOC_LENGTHS_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                parts = line.split("\t", 1)
                if len(parts) != 2:
                    continue

                doc_id = int(parts[0])
                doc_length = float(parts[1])

                # Expand list if needed so index matches doc_id directly.
                while len(self._lengths) <= doc_id:
                    self._lengths.append(1.0)

                self._lengths[doc_id] = doc_length

    def get_length(self, doc_id: int) -> float:
        """
        Retrieve the normalized length for a given document.

        A fallback value of 1.0 is returned if the doc_id is missing to
        avoid divide-by-zero issues during ranking.

        :param doc_id: Document identifier.
        :type doc_id: int
        :return: Document length.
        :rtype: float
        """

        if 0 <= doc_id < len(self._lengths):
            return self._lengths[doc_id]

        return 1.0