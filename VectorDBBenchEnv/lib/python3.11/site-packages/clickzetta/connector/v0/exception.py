"""exception for interacting with the ClickZetta API."""
from typing import Optional


class ClickzettaClientException(Exception):
    """Base Clickzetta exception class"""

    def __init__(
            self,
            message: str,
            *,
            error_code: Optional[str] = None,
    ) -> None:
        self.message: str = message
        self.error_code: Optional[str] = error_code
        self.telemetry_message: str = message

        self._pretty_msg = (
            f"({self.error_code}): {self.message}" if self.error_code else self.message
        )

    def __repr__(self):
        return f"{self.__class__.__name__}({self.message!r}, {self.error_code!r})"

    def __str__(self):
        return self._pretty_msg


class ClickzettaJobNotExistsException(ClickzettaClientException):
    """Exception for when a job does not exist in the ClickZetta API."""

    pass
