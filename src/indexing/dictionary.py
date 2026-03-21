from ..common.paths import DICTIONARY_PATH

class DictionaryWrite:
    """
    Write the final dictionary file used during retrieval.

    The dictionary stores one entry per term and provides metadata needed
    to locate that term's postings efficiently during search.

    Each entry has the format:
        term doc_count offset

    Where:
    - term is the token string
    - doc_count is the number of documents containing the term (df)
    - offset is the byte position of the term's postings in the merged file
    """

    def __init__(self):
        """
        Initialize the dictionary writer.

        This ensures the output directory exists and opens the dictionary
        file in write mode so any previous build is overwritten.

        :return: None
        """

        # Ensure the output directory exists before opening the file.
        DICTIONARY_PATH.parent.mkdir(parents=True, exist_ok=True)

        # Open once and reuse the handle during merge writing.
        self.file = open(DICTIONARY_PATH, "w")

    def write_entry(self, term: str, doc_count: int, offset: int) -> None:
        """
        Write a single dictionary entry to disk.

        This metadata allows the searcher to jump directly to a term's
        postings list using file seek operations instead of scanning.

        :param term: Token stored in the inverted index.
        :type term: str
        :param doc_count: Number of documents containing the term (df).
        :type doc_count: int
        :param offset: Byte offset of the term's postings in the merged file.
        :type offset: int
        :return: None
        """

        # Write one dictionary entry per line using space-delimited format.
        line = f"{term} {doc_count} {offset}\n"
        self.file.write(line)

    def close(self) -> None:
        """
        Close the dictionary file after writing completes.

        This should always be called after merging finishes to ensure
        buffered data is fully written to disk.

        :return: None
        """

        self.file.close()
