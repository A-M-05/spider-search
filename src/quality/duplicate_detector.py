from ..common.search_constants import SIMHASH_HAMMING_THRESHOLD, SIMHASH_BUCKET_BITS, SIMHASH_BITS
from .simhash_utils import compute_simhash, hamming_distance, bucket_key
import re, hashlib


class DuplicateDetector:
    """
    Detect exact and near-duplicate documents during indexing.

    This class maintains two independent duplicate-detection mechanisms:

    1. Exact duplicate detection
       - Builds a normalized text representation from extracted fields
       - Hashes that representation
       - Treats identical hashes as exact duplicates

    2. Near-duplicate detection
       - Builds a SimHash fingerprint from weighted term counts
       - Buckets documents by high-order fingerprint bits
       - Compares only within the same bucket using Hamming distance

    Stored state:
    - seen_hashes: maps exact-content hash -> (kept_doc_id, kept_url)
    - fingerprint_buckets: maps bucket key -> list of prior fingerprints

    This allows the indexer to:
    - skip exact duplicate pages entirely
    - log near-duplicate relationships for later use

    :return: None
    """

    def __init__(self):
        """
        Initialize duplicate-detection state and thresholds.

        The thresholds and fingerprint settings are loaded from the
        configuration module so behavior can be tuned without changing
        detection logic.

        :return: None
        """
        # Exact duplicate memory:
        # key   -> SHA-1 hash of normalized extracted text
        # value -> (kept_doc_id, kept_url)
        self.seen_hashes = {}

        # Near-duplicate memory:
        # key   -> bucket key derived from SimHash fingerprint prefix
        # value -> list of (doc_id, url, fingerprint) tuples
        self.fingerprint_buckets = {}

        # Configurable settings controlling near-duplicate sensitivity.
        self.threshold = SIMHASH_HAMMING_THRESHOLD
        self.bucket_bits = SIMHASH_BUCKET_BITS
        self.num_bits = SIMHASH_BITS

    def build_exact_text(self, fields: dict[str, str]) -> str:
        """
        Build the normalized text signature used for exact duplicate detection.

        The signature is created by combining the document's extracted
        textual fields (title, headings, bold text, and body), then
        lowercasing and collapsing whitespace so trivial formatting
        differences do not prevent duplicate matches.

        :param fields: Extracted text fields from a document.
        :type fields: dict[str, str]
        :return: Normalized combined text used for hashing.
        :rtype: str
        """

        # Combine all indexable text fields into a single string so exact
        # duplicate detection reflects the content that the search engine
        # actually uses for indexing.
        combined_text = " ".join([
            fields["title"],
            fields["headings"],
            fields["bold"],
            fields["body"]
        ])

        # Normalize case and whitespace so formatting differences alone do
        # not make two otherwise identical pages appear different.
        normalized_combined_text = re.sub(r"\s+", " ", combined_text.lower()).strip()
        return normalized_combined_text

    def check_exact(self, url: str, fields: dict[str, str], current_doc_id: int) -> tuple[bool, int | None, str | None]:
        """
        Check whether the current page is an exact duplicate of prior content.

        A page is considered an exact duplicate here if its normalized
        extracted text produces the same SHA-1 hash as a previously kept
        page. If so, the caller can skip indexing the new page and record
        the relationship to the earlier kept document.

        :param url: URL of the current page being considered.
        :type url: str
        :param fields: Extracted text fields for the current page.
        :type fields: dict[str, str]
        :param current_doc_id: Candidate docID that would be assigned if kept.
        :type current_doc_id: int
        :return: Tuple containing:
                 - whether this page is an exact duplicate
                 - the kept docID if duplicate
                 - the kept URL if duplicate
        :rtype: tuple[bool, int | None, str | None]
        """

        # Build a normalized text signature and hash it for exact matching.
        normalized_text = self.build_exact_text(fields)
        exact_hash = hashlib.sha1(normalized_text.encode("utf-8")).hexdigest()

        # If this exact content hash was seen before, return the already-kept
        # page information so the caller can skip indexing the duplicate.
        if exact_hash in self.seen_hashes:
            kept_doc_id, kept_url = self.seen_hashes[exact_hash]
            return (True, kept_doc_id, kept_url)

        # Otherwise, remember this page as the canonical representative for
        # this exact content.
        self.seen_hashes[exact_hash] = (current_doc_id, url)
        return (False, None, None)

    def check_near(self, url: str, current_doc_id: int, local_counts: dict[str, float]) -> list[tuple[int, str, int]]:
        """
        Check whether the current page is a near duplicate of prior pages.

        Near-duplicate detection uses SimHash over the document's weighted
        term counts. The fingerprint is assigned to a bucket based on its
        high-order bits, and only documents already in the same bucket are
        compared using Hamming distance. This keeps comparisons much cheaper
        than an all-pairs scan.

        :param url: URL of the current page being considered.
        :type url: str
        :param current_doc_id: Candidate docID for the current page.
        :type current_doc_id: int
        :param local_counts: Weighted term counts for the current document.
        :type local_counts: dict[str, float]
        :return: List of near-duplicate matches in the form
                 (prior_doc_id, prior_url, distance).
        :rtype: list[tuple[int, str, int]]
        """

        # Compute the SimHash fingerprint from the weighted term vector.
        fingerprint = compute_simhash(local_counts, self.num_bits)

        # Use the high-order bits of the fingerprint as a bucket key so only
        # likely similar pages are compared to one another.
        key = bucket_key(fingerprint, self.bucket_bits, self.num_bits)

        # Retrieve prior fingerprints already assigned to this bucket.
        bucket = self.fingerprint_buckets.get(key, [])
        near_matches = []

        # Compare only against prior pages in the same bucket. If the
        # Hamming distance is within the configured threshold, treat it as
        # a near-duplicate match and record it.
        for prior_doc_id, prior_url, prior_fingerprint in bucket:
            distance = hamming_distance(fingerprint, prior_fingerprint)
            if distance <= self.threshold:
                near_matches.append((prior_doc_id, prior_url, distance))

        # Store the current page in the bucket so future pages can compare
        # against it.
        bucket.append((current_doc_id, url, fingerprint))
        self.fingerprint_buckets[key] = bucket

        return near_matches
