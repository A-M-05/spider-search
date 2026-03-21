from ..common.search_constants import BETA, PAGERANK_SCORE_WEIGHT
from ..text.tokstem import normalize
from .dictionary_reader import DictionaryReader
from .postings_reader import PostingsReader
from .doc_table_reader import DocTableReader
from .doc_lengths_reader import DocLengthsReader
from .collection_stats_reader import CollectionStatsReader
from .bigram_reader import BigramReader
from .pagerank_reader import PageRankReader
import math
import heapq


class Searcher:
    """
    Execute ranked retrieval over the built search index.

    This class loads all retrieval-time index structures into memory or
    opens them for direct access, then processes user queries using a
    multi-signal ranking pipeline.

    Ranking signals used:
    - tf-idf style term weighting
    - document length normalization
    - soft conjunction bonus
    - bigram proximity boost
    - positional phrase boost
    - PageRank boost

    The final result returned by the searcher is a ranked list of URLs.
    """

    def __init__(self):
        """
        Initialize the search engine and load all retrieval resources.

        The following data sources are loaded or opened:
        - dictionary
        - merged postings file
        - docID to URL table
        - document lengths
        - collection statistics
        - bigram index
        - PageRank scores

        :return: None
        """
        
        print("Loading index:")
        print("\tLoading dictionary...")
        self._dictionary = DictionaryReader()

        print("\tLoading postings...")
        self._postings = PostingsReader()

        print("\tLoading doc table...")
        self._doc_table = DocTableReader()

        print("\tLoading doc lengths...")
        self._doc_lengths = DocLengthsReader()

        print("\tLoading stats...")
        self._stats = CollectionStatsReader()

        print("\tLoading bigrams...")
        self._bigrams = BigramReader()

        print("\tLoading pagerank...")
        self._pagerank = PageRankReader()

        print("Ready.")

    def phrase_match(self, position_one: list[int], position_two: list[int]) -> bool:
        """
        Check whether two terms appear adjacently in a document.

        This helper compares two ordered position lists and returns True
        if any occurrence of the second term immediately follows an
        occurrence of the first term.

        Example:
            position_one = [3, 10]
            position_two = [4, 11]
            -> True

        :param position_one: Positions of the first term.
        :type position_one: list[int]
        :param position_two: Positions of the second term.
        :type position_two: list[int]
        :return: True if the terms appear adjacently, else False.
        :rtype: bool
        """

        index_one = 0
        index_two = 0

        # Walk through both sorted position lists simultaneously.
        while index_one < len(position_one) and index_two < len(position_two):
            diff = position_two[index_two] - position_one[index_one]

            if diff == 1:
                return True
            elif diff > 1:
                index_one += 1
            else:
                index_two += 1

        return False

    def search(self, query: str, top_k: int = 15) -> list[str]:
        """
        Search the index and return the top-ranked document URLs.

        Query processing pipeline:
        1. normalize query terms
        2. compute query term frequencies
        3. gather dictionary metadata (df, offset, idf)
        4. accumulate document scores from postings
        5. normalize by document length
        6. apply ranking boosts
        7. return top-k URLs

        :param query: Raw user query string.
        :type query: str
        :param top_k: Maximum number of results to return.
        :type top_k: int
        :return: Ranked list of result URLs.
        :rtype: list[str]
        """

        # Normalize query into the same token space used by the index.
        terms = normalize(query)

        print(f"Searching for terms: {terms}")
        if not terms:
            return []

        # Count repeated query terms so query-side term weighting can use
        # query term frequency rather than treating all repeated terms equally.
        query_term_counts = {}
        for term in terms:
            query_term_counts[term] = query_term_counts.get(term, 0) + 1

        # Load collection size for IDF computation.
        n = self._stats.total_docs()
        if n <= 0:
            return []

        # For each query term, retrieve dictionary metadata:
        # - df: document frequency
        # - offset: byte offset into the postings file
        # - idf: inverse document frequency
        term_infos = []
        for term, qtf in query_term_counts.items():
            entry = self._dictionary.lookup(term)
            if entry is None:
                continue

            df, offset = entry
            if df <= 0:
                continue

            idf = math.log10(n / df)
            term_infos.append((term, qtf, df, idf, offset))

        # If no query terms exist in the dictionary, there are no results.
        if not term_infos:
            return []

        # Process higher-idf terms first so rarer terms influence scoring
        # earlier and typically more strongly.
        term_infos.sort(key=lambda x: x[3], reverse=True)

        # scores maps doc_id -> accumulated content score
        # matched_terms counts how many distinct query terms contributed
        # doc_positions keeps positional info for phrase/adjacency boosting
        scores = {}
        matched_terms = {}
        doc_positions = {}

        # Accumulate tf-idf style scores from each query term's postings list.
        for term, qtf, _, idf, offset in term_infos:
            postings = self._postings.get_postings(offset)

            # Query-side weight uses log-scaled query tf times idf.
            query_weight = (1 + math.log10(qtf)) * idf

            for doc_id, positions in postings:
                # Save positions so phrase and adjacency checks can be done later.
                doc_positions.setdefault(doc_id, {})[term] = positions

                # Document-side weight is based on log-scaled term frequency.
                term_frequency = len(positions)
                if term_frequency > 0:
                    doc_weight = 1 + math.log10(term_frequency)
                else:
                    doc_weight = 0.0

                # Add this term's contribution to the document's total score.
                scores[doc_id] = scores.get(doc_id, 0.0) + (doc_weight * query_weight)

                # Track how many query terms matched this document.
                matched_terms[doc_id] = matched_terms.get(doc_id, 0) + 1

        if not scores:
            return []

        # Normalize scores by precomputed document length so long documents
        # do not dominate purely because they contain more terms overall.
        for doc_id in scores:
            length = self._doc_lengths.get_length(doc_id)
            if length > 0:
                scores[doc_id] /= length

        # Soft conjunction bonus: documents matching more query terms receive
        # a small extra reward beyond raw tf-idf similarity.
        for doc_id in scores:
            scores[doc_id] += BETA * matched_terms[doc_id]

        # Bigram boost: if adjacent normalized query terms form a bigram that
        # also appears in a document, boost that document slightly.
        if len(terms) >= 2:
            for i in range(len(terms) - 1):
                bigram = terms[i] + "_" + terms[i + 1]
                matching_docs = self._bigrams.get_docs(bigram)

                for doc_id in matching_docs:
                    if doc_id in scores:
                        scores[doc_id] += 0.5

        # Positional phrase boost: if consecutive query terms appear adjacent
        # in the document according to positional postings, reward the document.
        if len(terms) >= 2:
            for doc_id in scores:
                if doc_id not in doc_positions:
                    continue

                term_map = doc_positions[doc_id]

                for i in range(len(terms) - 1):
                    t1 = terms[i]
                    t2 = terms[i + 1]

                    if t1 in term_map and t2 in term_map:
                        if self.phrase_match(term_map[t1], term_map[t2]):
                            scores[doc_id] += 1.0

        # PageRank boost: add a query-independent authority signal so highly
        # authoritative pages can be favored when content relevance is similar.
        for doc_id in scores:
            pagerank_score = self._pagerank.get_score(doc_id)
            scores[doc_id] += PAGERANK_SCORE_WEIGHT * pagerank_score

        # Select the top-k highest scoring documents without sorting the
        # entire score map.
        top_docs = heapq.nlargest(top_k, scores.items(), key=lambda x: x[1])

        # Convert ranked docIDs back into URLs for final output.
        urls = []
        for doc_id, score in top_docs:
            url = self._doc_table.get_url(doc_id)
            if url:
                urls.append(url)

        return urls

    def close(self) -> None:
        """
        Close any open retrieval-time file handles.

        Currently this closes the postings file reader.

        :return: None
        """

        self._postings.close()
