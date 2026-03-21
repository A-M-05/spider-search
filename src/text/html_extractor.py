from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning, MarkupResemblesLocatorWarning
import warnings
import re
from ..common.url_utils import resolve_and_normalize_url

# Suppress noisy parser warnings that are common in messy crawled web data.
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

# Precompiled regex used to collapse runs of whitespace into single spaces.
_WHITESPACE_RE = re.compile(r"\s+")

def _normalize_whitespaces(text: str) -> str:
    """
    Collapse repeated whitespace and trim surrounding spaces.

    This helper ensures extracted text is normalized consistently before
    it is stored, indexed, or used for duplicate detection.

    :param text: Raw text string.
    :type text: str
    :return: Whitespace-normalized text.
    :rtype: str
    """

    return _WHITESPACE_RE.sub(" ", text).strip()


def extract_fields(base_url: str, html: str) -> tuple[dict[str, str], list[tuple[str, str]]]:
    """
    Extract indexable text fields and outbound anchor links from HTML.

    The returned fields dictionary contains:
    - title
    - headings
    - bold
    - body

    The returned links list contains:
    - (normalized_target_url, anchor_text)

    Anchor text is extracted before non-content tags are removed so link
    structure can still be captured even if the page is later filtered or
    skipped during indexing.

    :param html: Raw HTML content for a page.
    :type html: str
    :return: Tuple of (fields dictionary, outbound links list).
    :rtype: tuple[dict[str, str], list[tuple[str, str]]]
    """

    # If there is no HTML content, return empty fields and no links.
    if not html:
        return {"title": "", "headings": "", "bold": "", "body": ""}, []

    # Parse the page using BeautifulSoup so structural HTML elements can be
    # examined and text can be extracted cleanly.
    soup = BeautifulSoup(html, "lxml")

    # Extract anchor targets and visible anchor text before stripping tags.
    # Target URLs are normalized so they can later be matched against indexed
    # URLs when building anchor maps or graph edges.
    links = []
    for a_tag in soup.find_all("a", href=True):
        raw_url = a_tag["href"]

        try:
            target_url = resolve_and_normalize_url(base_url, raw_url)
        except Exception:
            continue

        anchor_text = _normalize_whitespaces(
            a_tag.get_text(separator=" ", strip=True)
        )

        if anchor_text and target_url:
            links.append((target_url, anchor_text))

    # Remove script and style content so these do not pollute the text fields
    # used for indexing and ranking.
    for tag in soup(["script", "style"]):
        tag.decompose()

    # Extract the page title separately, then remove it from the soup so it
    # does not get duplicated again in the body text.
    title_text = ""
    title_tag = soup.title
    if title_tag:
        title_text = _normalize_whitespaces(
            title_tag.get_text(separator=" ", strip=True)
        )
        title_tag.decompose()

    # Extract heading text from h1/h2/h3 tags and remove those tags after
    # recording them so they do not get counted again in the body.
    headings_parts = []
    for tag in soup.find_all(["h1", "h2", "h3"]):
        heading = _normalize_whitespaces(tag.get_text(separator=" ", strip=True))
        if heading:
            headings_parts.append(heading)
        tag.decompose()
    headings_text = _normalize_whitespaces(" ".join(headings_parts)) if headings_parts else ""

    # Extract bold/strong text separately and remove it afterward so these
    # terms can be given their own field weighting without double counting.
    bold_parts = []
    for tag in soup.find_all(["b", "strong"]):
        bold = _normalize_whitespaces(tag.get_text(separator=" ", strip=True))
        if bold:
            bold_parts.append(bold)
        tag.decompose()
    bold_text = _normalize_whitespaces(" ".join(bold_parts)) if bold_parts else ""

    # Whatever readable text remains after removing the above structures is
    # treated as the general body field.
    body_text = _normalize_whitespaces(soup.get_text(separator=" ", strip=True))

    fields = {
        "title": title_text,
        "headings": headings_text,
        "bold": bold_text,
        "body": body_text,
    }

    return fields, links
