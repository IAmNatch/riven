class DebridServiceException(Exception):
    """Base exception for debrid service errors."""

    def __init__(self, message: str, *, provider: str) -> None:
        super().__init__(f"{provider}: {message}")

        self.provider = provider


class DebridServiceHTTPException(DebridServiceException):
    """Raised when there is a network error with the debrid service."""

    pass


class DebridServiceRefusedRangeRequestException(DebridServiceHTTPException):
    """Raised when the debrid service refuses a range request."""

    def __init__(self, provider: str) -> None:
        super().__init__("Refused range request", provider=provider)


class DebridServiceRangeNotSatisfiableException(DebridServiceHTTPException):
    """Raised when the debrid service reports that the requested range is not satisfiable."""

    def __init__(self, provider: str) -> None:
        super().__init__("Requested range not satisfiable", provider=provider)


class DebridServiceForbiddenException(DebridServiceHTTPException):
    """Raised when access to the debrid service is forbidden."""

    def __init__(self, provider: str) -> None:
        super().__init__("Access is forbidden", provider=provider)


class DebridServiceRateLimitedException(DebridServiceHTTPException):
    """Raised when the debrid service rate limits requests."""

    def __init__(self, provider: str) -> None:
        super().__init__("Rate limit exceeded", provider=provider)


class DebridServiceUnableToConnectException(DebridServiceException):
    """Raised when unable to establish a connection to the debrid service."""

    def __init__(self, provider: str) -> None:
        super().__init__("Unable to connect", provider=provider)


class DebridServiceServiceUnavailableException(DebridServiceException):
    """Raised when the debrid service reports that the service is unavailable."""

    def __init__(self, provider: str) -> None:
        super().__init__("Service is unavailable", provider=provider)


class DebridServiceLinkUnavailable(DebridServiceException):
    """Raised when a link is unavailable on the debrid service.

    Optionally carries the underlying torrent identifier and whether this is the
    first time the torrent was observed dead (as opposed to a cached repeat hit).
    A dead torrent is typically a season pack shared by many media items, so the
    first detection is the signal to reset all of them together in one pass.
    """

    def __init__(
        self,
        provider: str,
        link: str,
        torrent_id: str | None = None,
        first_detection: bool = False,
    ) -> None:
        super().__init__(f"Link {link} is unavailable or invalid", provider=provider)

        self.link = link
        self.torrent_id = torrent_id
        self.first_detection = first_detection


class DebridServiceClosedConnectionException(DebridServiceException):
    """Raised when the debrid service closes the connection prematurely."""

    def __init__(self, provider: str) -> None:
        super().__init__("Connection closed prematurely", provider=provider)
