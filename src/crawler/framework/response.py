class Response(object):
    """
    Thin wrapper around an HTTP response, compatible with the scraper interface.

    Previously this wrapped a Spacetime/cbor/pickle response dict.
    Now it wraps a direct requests.Response object, or an error dict
    when the request failed before getting a response.

    Attributes used by scraper.py:
    - url:          final URL after redirects
    - status:       HTTP status code (or synthetic error code 600-603)
    - error:        error string if request failed, else None
    - raw_response: the underlying requests.Response object (has .content, .headers)
    """

    def __init__(self, resp_dict):
        self.url = resp_dict.get("url", "")
        self.status = resp_dict.get("status", 0)
        self.error = resp_dict.get("error", None)

        # For successful responses, raw_response is the requests.Response object.
        # For error responses, it is None.
        self.raw_response = resp_dict.get("response", None)
