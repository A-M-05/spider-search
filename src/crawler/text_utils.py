from bs4 import BeautifulSoup
from .crawl_constants import STOP_WORDS

def to_text(content):
    """
    Convert raw response content into a Python string.
    Handles None, bytes, and already-string-like content.
    """
    # If content is missing, return empty string
    if content is None:
        return ""
    # If content is bytes, decode as UTF-8 (ignore bad byte sequences)
    if isinstance(content, bytes):
        return content.decode("utf-8", errors="ignore")
    # Otherwise, coerce to string
    return str(content)


def extract_visible_text(html):
    """
    Extract visible human-readable text from HTML by removing
    common non-content tags (scripts, nav, etc.) and returning the text.
    """
    # Parse HTML into a BeautifulSoup DOM
    soup = BeautifulSoup(html, "html.parser")
    # Remove tags that usually contain non-visible or repeated boilerplate content
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "aside"]):
        # Delete the entire tag subtree from the DOM
        tag.decompose()
    # Return visible text, separated by spaces, trimmed of extra whitespace
    return soup.get_text(separator=" ", strip=True)


def tokenize(text_content: str) -> list[str]:
    """
    Returns tokens EXCLUDING stopwords and digits.
    Used for 'top words' (content words).
    """
    # Build the current token one character at a time
    current = []
    # Store finalized tokens here
    all_tokens = []

    # Normalize to lowercase for consistent token comparison
    text_lower = text_content.lower()

    # Scan the text character-by-character
    for char in text_lower:
        # Keep ASCII alphanumeric chars as part of the current token
        if char.isalnum() and char.isascii():
            current.append(char)
        else:
            # If we hit a delimiter and we have a token buffered, flush it
            if current:
                token = ''.join(current)
                # Keep token only if non-empty, not a stopword, and not pure digits
                if token and token not in STOP_WORDS and not token.isdigit():
                    all_tokens.append(token)
                # Reset buffer for next token
                current = []

    # Flush final buffered token if text ended mid-token
    if current:
        token = ''.join(current)
        # Apply same filtering rules as above
        if token and token not in STOP_WORDS and not token.isdigit():
            all_tokens.append(token)

    # Return list of filtered content tokens
    return all_tokens


def tokenize_with_stopwords(text_content: str) -> list[str]:
    """
    Returns tokens INCLUDING stopwords (excluding pure digits).
    Used to compute stopword frequencies.
    """
    # Buffer for the token currently being built
    current = []
    # Collected tokens (includes stopwords)
    tokens = []

    # Normalize to lowercase so counts are case-insensitive
    text_lower = text_content.lower()

    # Scan the text character-by-character
    for char in text_lower:
        # Keep ASCII alphanumeric chars in the current token
        if char.isalnum() and char.isascii():
            current.append(char)
        else:
            # On delimiter, flush buffered token if it exists
            if current:
                token = ''.join(current)
                # Keep token only if non-empty and not pure digits
                if token and not token.isdigit():
                    tokens.append(token)
                # Reset buffer for next token
                current = []

    # Flush final buffered token if text ended mid-token
    if current:
        token = ''.join(current)
        # Apply same filtering rules as above
        if token and not token.isdigit():
            tokens.append(token)

    # Return list of tokens including stopwords
    return tokens
