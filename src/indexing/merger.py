import heapq
from .dictionary import DictionaryWrite
from ..common.paths import PARTIAL_DIR, MERGED_POSTINGS_PATH, DICTIONARY_PATH


def parse_line(line: str) -> tuple[str, list[str]]:
    """
    Parse one line from a partial index file.

    Each partial index line is expected to follow the format:
        term<TAB>doc_id:pos1,pos2,... doc_id:pos1,pos2,...

    The postings portion is returned as a list of raw posting strings so
    later merge logic can combine postings from multiple partial files.

    :param line: Raw line read from a partial index file.
    :type line: str
    :return: Tuple containing the term and its postings list.
    :rtype: tuple[str, list[str]]
    """

    # Remove trailing whitespace and handle empty lines safely.
    line = line.strip()
    if not line:
        return ("", [])

    # Split into the term and the serialized postings portion.
    parts = line.split("\t", 1)
    term = parts[0]

    # If the line has no postings payload, return an empty postings list.
    if len(parts) < 2 or not parts[1].strip():
        return (term, [])

    postings_list = parts[1].split()
    return (term, postings_list)


def add_postings_to_accumulator(accumulator_dict: dict, postings_list: list[str]):
    """
    Merge serialized postings into an in-memory postings accumulator.

    Each posting string is expected to follow the format:
        doc_id:pos1,pos2,...

    If a docID already exists in the accumulator, its positions are extended.
    Otherwise, a new postings entry is created for that docID.

    :param accumulator_dict: Accumulator mapping docID to position list.
    :type accumulator_dict: dict[int, list[int]]
    :param postings_list: List of serialized postings strings.
    :type postings_list: list[str]
    :return: None
    """

    # Deserialize each posting and merge its positions into the accumulator.
    for item in postings_list:
        doc_id_str, freq_str = item.split(":")
        doc_id = int(doc_id_str)
        positions = [int(p) for p in freq_str.split(",")]

        if doc_id in accumulator_dict:
            accumulator_dict[doc_id].extend(positions)
        else:
            accumulator_dict[doc_id] = positions

class Merger:
    """
    Merge all partial index files into one final postings file.

    This class performs a multi-way merge over sorted partial index files
    produced during indexing. It combines postings for the same term across
    partials, writes the merged postings file, and simultaneously builds the
    dictionary file containing document frequency and byte offset metadata.

    The merge assumes:
    - each partial file is already sorted lexicographically by term
    - postings within a term are grouped by docID
    """

    def __init__(self):
        """
        Initialize the merger.

        No persistent state is required; merge_partials drives the full process.

        :return: None
        """

        pass

    def merge_partials(self):
        """
        Merge all partial postings files into final postings and dictionary files.

        This function:
        1. collects all partial index files
        2. opens them simultaneously
        3. uses a heap to repeatedly select the smallest next term
        4. combines postings for identical terms
        5. writes the merged postings line
        6. records dictionary metadata (document frequency and byte offset)

        Output files:
        - merged postings file
        - dictionary file

        :return: None
        """
        
        # Collect partial index files in sorted order so the merge process
        # is deterministic and consistent across runs.
        partial_files = sorted(PARTIAL_DIR.glob("partial_*.tsv"))

        if not partial_files:
            print("ERROR: No partial files found.")
            return

        # Ensure output directories exist before writing the final merged files.
        MERGED_POSTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
        DICTIONARY_PATH.parent.mkdir(parents=True, exist_ok=True)

        # Clear any previous dictionary output before starting a fresh merge.
        open(DICTIONARY_PATH, "w").close()

        # Open all partial files so their current frontiers can be merged
        # together with a heap-based multi-way merge.
        file_handles = []
        for file_path in partial_files:
            file_handles.append(open(file_path, "r"))

        # Initialize the heap using the first line from each partial file.
        # Heap entries are tuples:
        #   (term, file_id, postings_list)
        heap = []
        for file_id in range(len(file_handles)):
            line = file_handles[file_id].readline()
            if line:
                term, postings_list = parse_line(line)
                heapq.heappush(heap, (term, file_id, postings_list))

        # Open the final merged postings output and dictionary writer.
        merged_file = open(MERGED_POSTINGS_PATH, "w")
        dictionary_writer = DictionaryWrite()

        # Track byte offsets into the merged postings file so the dictionary
        # can support direct seek-based lookup at query time.
        offset_bytes = 0

        # Repeatedly pull the lexicographically smallest available term from
        # the heap, then merge all heap entries that correspond to that term.
        while heap:
            term, file_id, postings_list = heapq.heappop(heap)

            # Start a new in-memory accumulator for this term's postings.
            postings_accumulator = {}
            add_postings_to_accumulator(postings_accumulator, postings_list)

            # Track which partial files contributed the current term so their
            # next lines can be advanced after the term is fully merged.
            file_contribution = [file_id]

            # While the next heap entry has the same term, merge it into the
            # accumulator instead of writing separate lines.
            while heap and heap[0][0] == term:
                _, file_id2, postings_list2 = heapq.heappop(heap)
                add_postings_to_accumulator(postings_accumulator, postings_list2)
                file_contribution.append(file_id2)

            # Finalize this term's merged postings by sorting docIDs and
            # serializing their positional postings back into string form.
            sorted_doc_ids = sorted(postings_accumulator.keys())

            final_postings_list = []
            for doc_id in sorted_doc_ids:
                positions = postings_accumulator[doc_id]
                pos_str = ",".join(str(p) for p in positions)
                final_postings_list.append(f"{doc_id}:{pos_str}")

            # Document frequency is the number of distinct docIDs for this term.
            doc_freq = len(final_postings_list)

            # Write the merged postings line and compute how many bytes it
            # contributed so future offsets remain correct.
            merged_line_str = term + "\t" + " ".join(final_postings_list) + "\n"
            bytes_written = len(merged_line_str.encode("utf-8"))
            merged_file.write(merged_line_str)

            # Record term metadata in the dictionary before moving the offset
            # forward to the next postings line.
            dictionary_writer.write_entry(term, doc_freq, offset_bytes)
            offset_bytes += bytes_written

            # Advance each partial file that contributed this term and push
            # its next available line back into the heap.
            for file_id in file_contribution:
                curr_line = file_handles[file_id].readline()

                if curr_line:
                    next_term, next_postings_list = parse_line(curr_line)
                    heapq.heappush(heap, (next_term, file_id, next_postings_list))

        # Close all output and input files after merge completion.
        merged_file.close()
        dictionary_writer.close()
        for file in file_handles:
            file.close()
