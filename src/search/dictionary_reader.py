from ..common.paths import DICTIONARY_PATH

class DictionaryReader:
    """
    Load and query the dictionary file used during retrieval.

    The dictionary maps:
        term -> (document_frequency, byte_offset)

    - document_frequency (df): number of documents containing the term
    - byte_offset: location in the merged postings file where the term's
      postings list begins
    """

    def __init__(self):
        """
        Initialize the dictionary reader and load the dictionary file.

        :return: None
        """

        self._dict = {}
        self._load()

    def _load(self):
        """
        Load the dictionary file into memory.

        Expected dictionary format:
            term doc_freq byte_offset

        Example:
            machine 132 904522

        :raises FileNotFoundError: If the dictionary file does not exist.
        :return: None
        """

        if not DICTIONARY_PATH.exists():
            raise FileNotFoundError(f"Dictionary not found at {DICTIONARY_PATH}")

        with open(DICTIONARY_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                parts = line.split(" ")
                if len(parts) != 3:
                    continue

                term, doc_freq, byte_offset = parts

                # Store metadata needed for direct postings lookup.
                # The byte offset allows the search engine to jump directly
                # to the postings list in the merged postings file using seek().
                self._dict[term] = (int(doc_freq), int(byte_offset))

    def lookup(self, term: str):
        """
        Retrieve dictionary metadata for a given term.

        :param term: Token to look up.
        :type term: str
        :return: Tuple (doc_freq, byte_offset) if the term exists,
                 otherwise None.
        :rtype: tuple[int, int] | None
        """

        return self._dict.get(term, None)

    def __contains__(self, term: str) -> bool:
        """
        Check whether a term exists in the dictionary.

        This allows usage such as:
            if term in dictionary_reader:

        :param term: Token to check.
        :type term: str
        :return: True if the term exists.
        :rtype: bool
        """

        return term in self._dict
