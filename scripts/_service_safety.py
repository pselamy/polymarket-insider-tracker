"""Validate every service route before constructing clients or disposable databases."""

from __future__ import annotations

import ipaddress
import os
import socket
from collections.abc import Mapping
from urllib.parse import SplitResult, urlsplit

from sqlalchemy.engine import make_url

from polymarket_insider_tracker.storage.database_url import DatabaseUrlError, normalize_database_url

from ._service_models import ServicePrerequisiteError

# libpq query keys and inherited routing variables can override a loopback URL host.
# Reject both before constructing a client, subprocess or disposable database.
REDIS_SCHEME = "redis"


LIBPQ_ROUTING_QUERY_KEYS = frozenset({"host", "hostaddr", "port", "service"})


LIBPQ_ROUTING_ENVIRONMENT = ("PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE")


def _resolved_addresses(
    host: str, variable: str = "DATABASE_URL"
) -> set[ipaddress.IPv4Address | ipaddress.IPv6Address]:
    try:
        records = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise ServicePrerequisiteError(
            f"{variable} host could not be resolved for loopback verification"
        ) from exc
    addresses: set[ipaddress.IPv4Address | ipaddress.IPv6Address] = set()
    for record in records:
        try:
            addresses.add(ipaddress.ip_address(record[4][0]))
        except ValueError as exc:
            raise ServicePrerequisiteError(
                f"{variable} host returned an invalid address during loopback verification"
            ) from exc
    return addresses


def _require_loopback(host: str, variable: str) -> None:
    addresses = _resolved_addresses(host, variable)
    if not addresses or any(not address.is_loopback for address in addresses):
        raise ServicePrerequisiteError(
            f"{variable} must resolve only to loopback addresses for service verification"
        )


def _require_no_routing_query_keys(canonical: str) -> None:
    for key in make_url(canonical).query:
        if key.lower() in LIBPQ_ROUTING_QUERY_KEYS:
            raise ServicePrerequisiteError(
                f"DATABASE_URL query parameter {key.lower()!r} lets libpq reroute the"
                " connection past the loopback host; remove it for service verification"
            )


def _require_no_routing_environment(environment: Mapping[str, str] | None = None) -> None:
    checked = os.environ if environment is None else environment
    for name in LIBPQ_ROUTING_ENVIRONMENT:
        if checked.get(name):
            raise ServicePrerequisiteError(
                f"{name} must be unset during service verification; libpq would let it"
                " reroute the loopback connection to another server"
            )


def validate_loopback_database_url(database_url: str) -> str:
    """Return a canonical URL only when every route the connection can take is loopback.

    Loopback resolution of the host component alone is not enough: libpq honors
    ``hostaddr``/``host``/``service`` query parameters and the corresponding environment
    variables for the actual route, so those are rejected as prerequisites as well.
    """
    if not database_url:
        raise ServicePrerequisiteError("DATABASE_URL is required")
    try:
        canonical = normalize_database_url(database_url)
    except DatabaseUrlError as exc:
        # The product error names the offending component or key without echoing the value.
        raise ServicePrerequisiteError(str(exc)) from exc
    host = make_url(canonical).host
    if host is None:
        raise ServicePrerequisiteError("DATABASE_URL must include a loopback host")
    _require_loopback(host, "DATABASE_URL")
    _require_no_routing_query_keys(canonical)
    _require_no_routing_environment()
    return canonical


def _validate_redis_port(parsed: SplitResult) -> None:
    try:
        port = parsed.port
    except ValueError as exc:
        raise ServicePrerequisiteError(
            "REDIS_URL port must be an integer between 1 and 65535"
        ) from exc
    if port is not None and not 1 <= port <= 65535:
        raise ServicePrerequisiteError("REDIS_URL port must be an integer between 1 and 65535")


def validate_redis_url(redis_url: str) -> str:
    """Return ``redis_url`` unchanged when it is usable, or fail without echoing any value."""
    if not redis_url:
        raise ServicePrerequisiteError("REDIS_URL is required")
    try:
        parsed = urlsplit(redis_url)
    except ValueError as exc:
        raise ServicePrerequisiteError("REDIS_URL could not be parsed as a URL") from exc
    if parsed.scheme != REDIS_SCHEME:
        raise ServicePrerequisiteError("REDIS_URL must use the redis:// scheme")
    if not parsed.hostname:
        raise ServicePrerequisiteError("REDIS_URL must include a host")
    _validate_redis_port(parsed)
    return redis_url


def validate_loopback_redis_url(redis_url: str) -> str:
    """Return ``redis_url`` only when it is usable and every resolved host address is loopback.

    Shared Redis contract tests write and delete keys, so they may only ever target a disposable
    local service; an arbitrary external ``REDIS_URL`` is rejected before any client exists.
    """
    validated = validate_redis_url(redis_url)
    host = urlsplit(validated).hostname
    assert host is not None
    _require_loopback(host, "REDIS_URL")
    return validated
