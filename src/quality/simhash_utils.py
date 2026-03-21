import hashlib

def hash_token(token: str, num_bits: int) -> int:
    """
    Convert a token into a stable fixed-width integer hash.

    This function hashes a token using SHA-1 and truncates the result
    to a fixed number of bits. The truncated hash is used as the token's
    bit contribution in SimHash computation so results remain consistent
    across runs.

    :param token: Token string to hash.
    :type token: str
    :param num_bits: Number of low-order bits to keep.
    :type num_bits: int
    :return: Integer hash truncated to num_bits.
    :rtype: int
    """
    
    # SHA-1 produces a stable byte digest independent of Python runtime.
    digest = hashlib.sha1(token.encode("utf-8")).digest()

    # Convert the digest into a large integer so we can apply bit operations.
    value = int.from_bytes(digest, "big")

    # Keep only the requested number of bits using a mask.
    mask = (1 << num_bits) - 1
    return value & mask


def compute_simhash(local_counts: dict[str, int | float], num_bits: int) -> int:
    """
    Compute a SimHash fingerprint from weighted term counts.

    Each token contributes to the fingerprint by adding or subtracting
    its weight across bit positions depending on the token's hashed bits.
    After processing all tokens, positive dimensions become 1-bits and
    negative dimensions become 0-bits.

    :param local_counts: Mapping of token → weighted term frequency.
    :type local_counts: dict[str, float]
    :param num_bits: Number of bits in the final fingerprint.
    :type num_bits: int
    :return: SimHash fingerprint as an integer.
    :rtype: int
    """

    # Each dimension accumulates signed weight contributions.
    vector = [0.0] * num_bits

    # Process each token independently.
    for token, weight in local_counts.items():
        token_hash = hash_token(token, num_bits)

        # For each bit position:
        # - add weight if bit is 1
        # - subtract weight if bit is 0
        for i in range(num_bits):
            bitmask = 1 << i
            if token_hash & bitmask:
                vector[i] += weight
            else:
                vector[i] -= weight

    # Convert accumulated vector into final fingerprint bits.
    fingerprint = 0
    for i in range(num_bits):
        if vector[i] > 0:
            fingerprint |= (1 << i)

    return fingerprint


def hamming_distance(fp1: int, fp2: int) -> int:
    """
    Compute the Hamming distance between two fingerprints.

    The Hamming distance measures how many bit positions differ
    between two fingerprints and is used for near-duplicate detection.

    :param fp1: First fingerprint.
    :type fp1: int
    :param fp2: Second fingerprint.
    :type fp2: int
    :return: Number of differing bits.
    :rtype: int
    """

    # XOR highlights all differing bits.
    x = fp1 ^ fp2

    # Count set bits using simple bit shifting.
    count = 0
    while x:
        count += x & 1
        x >>= 1

    return count


def bucket_key(fingerprint: int, bucket_bits: int, num_bits: int):
    """
    Extract a bucket key from the high-order bits of a fingerprint.

    Bucketing limits comparisons in near-duplicate detection by grouping
    fingerprints with similar prefixes so only likely matches are compared.

    :param fingerprint: SimHash fingerprint.
    :type fingerprint: int
    :param bucket_bits: Number of leading bits used for bucketing.
    :type bucket_bits: int
    :param num_bits: Total fingerprint bit width.
    :type num_bits: int
    :return: Bucket key derived from high-order bits.
    :rtype: int
    """

    # Shift right so only the top bucket_bits remain.
    shift = num_bits - bucket_bits
    return fingerprint >> shift
