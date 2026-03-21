from nltk.stem.snowball import SnowballStemmer

def tokenize(text_content: str) -> list[str]:
    """
    Convert raw text into a list of tokens.

    Tokenization rules:
    - lowercase all text
    - keep only ASCII alphanumeric characters
    - split tokens when encountering non-alphanumeric characters

    :param text_content: Raw input text.
    :type text_content: str
    :return: List of tokens.
    :rtype: list[str]
    """

    current = []
    tokens = []

    # Convert entire string to lowercase so token matching is case-insensitive.
    text_lower = text_content.lower()

    for char in text_lower:
        # Only keep ASCII alphanumeric characters as part of tokens.
        if char.isalnum() and char.isascii():
            current.append(char)
        else:
            # When a delimiter is encountered, finalize the current token.
            if current:
                tokens.append("".join(current))
                current = []

    # Capture the final token if the string ended with an alphanumeric character.
    if current:
        tokens.append("".join(current))

    return tokens


# Initialize a Snowball stemmer for English tokens.
stemmer = SnowballStemmer("english")


def stem_tokens(tokens: list[str]) -> list[str]:
    """
    Apply stemming to a list of tokens.

    Stemming reduces tokens to their root forms so variations like
    "running", "runs", and "runner" map closer together.

    :param tokens: Tokenized words.
    :type tokens: list[str]
    :return: Stemmed tokens.
    :rtype: list[str]
    """

    return [stemmer.stem(token) for token in tokens]


def normalize(content: str) -> list[str]:
    """
    Full normalization pipeline used by the search engine.

    This function combines:
    - tokenization
    - stemming

    It ensures that query text and indexed text share the same
    normalization process so terms match correctly.

    :param content: Raw text input.
    :type content: str
    :return: Normalized token list.
    :rtype: list[str]
    """

    tokens = tokenize(content)
    return stem_tokens(tokens)
