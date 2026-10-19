"""Safe config-validation diagnostics that never echo raw input values."""

from __future__ import annotations

import sys

from pydantic import ValidationError


def format_error_field(loc: tuple[int | str, ...]) -> str:
    return ".".join(str(part) for part in loc)


def sanitized_validation_message(msg: str) -> str:
    """Render a config-validation diagnostic without echoing raw input.

    Pydantic and ``urlsplit`` error text interpolates the supplied value
    (netloc/userinfo/path), which may carry a credential. The diagnostic keeps
    the safe failure class but never the raw input or the raw exception message.
    """
    lowered = msg.lower()
    if is_host_component_failure(lowered):
        return "invalid host component: contains characters rejected by URL normalization"
    return classified_validation_message(lowered)


def classified_validation_message(lowered: str) -> str:
    """Map the failure class to its safe message without echoing the value.

    Hostname/port markers run before the scheme table: the field name inside
    a message like ``REDIS_URL has an invalid port`` would otherwise match
    the ``redis`` scheme marker and mislabel the failure as a scheme error.
    """
    direct = direct_marker_message(lowered)
    if direct is not None:
        return direct
    scheme_message = scheme_failure_message(lowered)
    if scheme_message is not None:
        return scheme_message
    return url_shape_validation_message(lowered)


_NON_SCHEME_VALIDATION_MESSAGES: tuple[tuple[str, str], ...] = (
    ("hostname", "must include a valid hostname"),
    ("port", "has an invalid port"),
)


def direct_marker_message(lowered: str) -> str | None:
    """Hostname/port markers map one-to-one to their safe message."""
    for marker, message in _NON_SCHEME_VALIDATION_MESSAGES:
        if marker in lowered:
            return message
    return None


def url_shape_validation_message(lowered: str) -> str:
    """Database, generic URL, and fallback messages without echoing the value."""
    if is_database_url_failure(lowered):
        return "is not a valid PostgreSQL URL"
    if "url" in lowered and ("valid" in lowered or "scheme" in lowered):
        return "is not a valid URL"
    return "has an invalid value"


def is_host_component_failure(lowered: str) -> bool:
    """URL normalization rejected the host component carrying the raw value."""
    return "nfkc" in lowered or "invalid characters" in lowered or "netloc" in lowered


_SCHEME_FAILURE_MESSAGES: tuple[tuple[str, str], ...] = (
    ("nested", "malformed nested scheme in its host component"),
    ("redis", "must start with redis://"),
    ("websocket", "must start with ws:// or wss://"),
    ("ws://", "must start with ws:// or wss://"),
    ("wss://", "must start with ws:// or wss://"),
)


def scheme_failure_message(lowered: str) -> str | None:
    """The safe scheme expectation, or None when the message is not scheme-shaped."""
    for marker, message in _SCHEME_FAILURE_MESSAGES:
        if marker in lowered:
            return message
    if "http" in lowered and "endpoint" in lowered:
        return "must be an HTTP(S) endpoint"
    return None


def is_database_url_failure(lowered: str) -> bool:
    """The message is about the PostgreSQL URL shape, not its secret value."""
    return "database" in lowered and ("valid" in lowered or "postgresql" in lowered)


def print_validation_errors(exc: ValidationError) -> None:
    print("Configuration validation failed:", file=sys.stderr)
    for error in exc.errors():
        field = format_error_field(error["loc"])
        msg = sanitized_validation_message(str(error["msg"]))
        print(f"  {field}: {msg}", file=sys.stderr)
