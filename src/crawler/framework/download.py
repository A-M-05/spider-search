import requests
from .response import Response

# Timeout for all HTTP requests (connect timeout, read timeout)
REQUEST_TIMEOUT = (5, 15)


def download(url, config, logger=None):
    """
    Fetch a URL directly using requests, returning a Response object.

    This replaces the original Spacetime cache-server download so the
    crawler can run standalone without external infrastructure.

    :param url: URL to fetch.
    :param config: Crawler config object (used for user agent).
    :param logger: Optional logger for error reporting.
    :return: Response object wrapping the HTTP response.
    """
    headers = {"User-Agent": config.user_agent}

    try:
        resp = requests.get(
            url,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True
        )
        return Response({
            "url": resp.url,
            "status": resp.status_code,
            "response": resp,
        })

    except requests.exceptions.TooManyRedirects:
        if logger:
            logger.warning(f"Too many redirects: {url}")
        return Response({"url": url, "status": 600, "error": "Too many redirects"})

    except requests.exceptions.ConnectionError:
        if logger:
            logger.warning(f"Connection error: {url}")
        return Response({"url": url, "status": 601, "error": "Connection error"})

    except requests.exceptions.Timeout:
        if logger:
            logger.warning(f"Timeout: {url}")
        return Response({"url": url, "status": 602, "error": "Timeout"})

    except Exception as e:
        if logger:
            logger.error(f"Unexpected error fetching {url}: {e}")
        return Response({"url": url, "status": 603, "error": str(e)})
