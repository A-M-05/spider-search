from ..common.paths import MERGED_POSTINGS_PATH

class PostingsReader:
    """
    Read postings lists from the merged postings file.

    This reader performs direct disk access using byte offsets stored in
    the dictionary. Instead of scanning the entire postings file, the
    search engine jumps directly to the correct location using seek().
    """

    def __init__(self):
        """
        Initialize the postings reader and open the merged postings file.

        The file is opened in binary mode so byte offsets from the
        dictionary correspond exactly to positions in the file.

        :raises FileNotFoundError: If the postings file does not exist.
        :return: None
        """

        if not MERGED_POSTINGS_PATH.exists():
            raise FileNotFoundError(f"Postings file not found at {MERGED_POSTINGS_PATH}")

        # Binary mode ensures byte offsets remain accurate.
        self._file = open(MERGED_POSTINGS_PATH, "rb")

    def get_postings(self, offset: int):
        """
        Retrieve the postings list for a term using its byte offset.

        The dictionary provides the offset, allowing the reader to jump
        directly to the correct position in the postings file and read
        exactly one line.

        Expected line format:
            term<TAB>doc_id:pos1,pos2,... doc_id:pos1,pos2,...

        :param offset: Byte offset where the postings line begins.
        :type offset: int
        :return: List of postings tuples (doc_id, positions).
        :rtype: list[tuple[int, list[int]]]
        """

        # Jump directly to the correct byte position in the file.
        self._file.seek(offset)

        # Read the full postings line for the term.
        line = self._file.readline().decode("utf-8").strip()

        if not line:
            return []

        parts = line.split("\t", 1)
        if len(parts) < 2:
            return []

        postings = []

        # Parse each posting entry of the form: doc_id:pos1,pos2,...
        for item in parts[1].split():
            doc_id_str, pos_str = item.split(":")
            doc_id = int(doc_id_str)

            # Convert position string into a list of integers.
            positions = [int(p) for p in pos_str.split(",")]

            postings.append((doc_id, positions))

        return postings

    def close(self):
        """
        Close the postings file.

        This should be called when the search engine shuts down to
        release the file handle.

        :return: None
        """

        self._file.close()
