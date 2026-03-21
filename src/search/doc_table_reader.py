from ..common.paths import DOC_TABLE_PATH


class DocTableReader:
    """
    Load and provide access to the document table.

    The doc table maps:
        doc_id -> URL

    A list is used instead of a dictionary for faster access since docIDs
    are integers and can directly index into the structure.
    """

    def __init__(self):
        """
        Initialize the reader and load the doc table into memory.

        :return: None
        """

        # Internal list where index = doc_id and value = URL.
        self._urls = []
        self._load()

    def _load(self):
        """
        Load the document table from disk.

        Expected file format:
            doc_id<TAB>url

        Malformed lines are skipped silently.

        :raises FileNotFoundError: If the doc table file does not exist.
        :return: None
        """

        if not DOC_TABLE_PATH.exists():
            raise FileNotFoundError(f"Doc table not found at {DOC_TABLE_PATH}")

        with open(DOC_TABLE_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                parts = line.split("\t", 1)
                if len(parts) != 2:
                    continue

                doc_id = int(parts[0])
                url = parts[1]

                # Expand list so doc_id can be used directly as an index.
                # This also handles gaps or out-of-order docIDs safely.
                while len(self._urls) <= doc_id:
                    self._urls.append("")

                self._urls[doc_id] = url

    def get_url(self, doc_id: int) -> str:
        """
        Retrieve the URL for a given document.

        :param doc_id: Document identifier.
        :type doc_id: int
        :return: URL if present, otherwise empty string.
        :rtype: str
        """

        if 0 <= doc_id < len(self._urls):
            return self._urls[doc_id]

        return ""
