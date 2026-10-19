"""Redacting log filter and exception sanitization for the CLI boundary."""

from __future__ import annotations

import contextlib
import logging
import re
from collections.abc import Mapping

from polymarket_insider_tracker.redaction import (
    FAILSAFE_EXCEPTION_MESSAGE,
    MASK,
    redact_argument,
    redact_exception_message,
    redact_text,
)


class RedactingLogFilter(logging.Filter):
    """Scrub rendered log records so tracebacks cannot re-emit URL secrets.

    ``logger.exception`` renders the exception chain and traceback from the raw
    exception object even when the message argument is pre-sanitized. The
    filter rewrites the record's message arguments and, when an exception is
    attached, replaces the exception with a sanitized clone whose messages and
    notes are redacted text (non-string arguments become the placeholder) and
    whose traceback frames are dropped, so formatters render only safe text.

    ``configure_logging`` installs the filter on the console handler because a
    logger-level filter never runs for records propagated from other loggers;
    only the handler position makes the scrub a shared fail-closed boundary
    for every application and third-party logger.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        redact_record_message(record)
        redact_record_args(record)
        redact_record_exc_info(record)
        keep_record_formattable(record)
        return True


def redact_record_message(record: logging.LogRecord) -> None:
    """Scrub the record's message object so no shape can carry a raw secret.

    A string keeps its redacted text. An exception logged directly as the
    message renders through the fail-closed exception renderer, because
    ``str()`` on it would interpolate raw arguments first. Any other object
    follows the shared argument policy: safe scalars stay readable and
    containers become the placeholder.
    """
    msg = record.msg
    if isinstance(msg, str):
        record.msg = redact_text(msg)
    elif isinstance(msg, BaseException):
        record.msg = redact_exception_message(msg)
    else:
        record.msg = redact_argument(msg)


def redact_record_args(record: logging.LogRecord) -> None:
    """Scrub %-style arguments through the shared fail-closed argument policy.

    Numbers, booleans, and ``None`` survive unchanged so numeric format
    specifiers (``%d``, ``%.1f``) keep rendering at the shared handler
    boundary; containers and bytes become the deterministic placeholder.

    ``LogRecord`` unwraps a sole nonempty argument into ``record.args`` for
    every ``collections.abc.Mapping``, not only a concrete ``dict``, so the
    mapping branch tests the same abstract class the standard library tests.
    Testing ``dict`` instead let ordinary standard-library mappings —
    ``UserDict``, ``ChainMap``, ``MappingProxyType`` — pass the boundary
    untouched and render their keys and values verbatim.
    """
    if isinstance(record.args, tuple):
        record.args = tuple(map(redact_argument, record.args))
    elif isinstance(record.args, Mapping):
        record.args = redacted_mapping_args(record.msg, record.args)


# One %-conversion that does not open a ``%(name)`` mapping key; ``%%``
# literals are consumed before the scan so they can never look like one.
POSITIONAL_CONVERSION_PATTERN = re.compile(r"%[^%(]|%$")


class MaskedPositionalMapping(dict[str, object]):
    """A positional mapping argument that renders only as the placeholder.

    ``LogRecord`` unwraps a sole nonempty mapping into ``record.args`` even
    when the format string consumes it positionally, so the container itself
    would render through ``%s``. Every rendering path — ``str``/``repr`` for
    a positional conversion, key lookup for a residual named reference —
    yields the placeholder, keeping the record emitting without exposing
    keys or values.

    The mapping stays empty so no original key or value survives the
    collapse, and is deliberately truthy because ``getMessage`` interpolates
    only ``if self.args``: an empty container that reports itself falsy is
    skipped entirely and the console prints the unsubstituted format string
    instead of this placeholder.
    """

    def __bool__(self) -> bool:
        return True

    def __str__(self) -> str:
        return MASK

    def __repr__(self) -> str:
        return MASK

    def __missing__(self, key: str) -> object:
        return MASK


def redacted_mapping_args(msg: object, args: Mapping[str, object]) -> dict[str, object]:
    """A named-interpolation mapping keeps redacted values; a positional dict collapses.

    ``LogRecord`` gives a genuine ``%(name)s`` mapping and a sole nonempty
    dict passed to a positional ``%s`` the same ``record.args`` shape; only
    the format string tells them apart. Any positional conversion renders
    the container itself, so the dict must collapse to the placeholder like
    every other container argument — its keys can carry a secret too, and
    named formatting never renders keys.
    """
    if has_positional_conversion(msg):
        return MaskedPositionalMapping()
    return {key: redact_argument(value) for key, value in args.items()}


def has_positional_conversion(msg: object) -> bool:
    """True when the format string renders any conversion not keyed by ``%(name)``."""
    if not isinstance(msg, str):
        return False
    return POSITIONAL_CONVERSION_PATTERN.search(msg.replace("%%", "")) is not None


def keep_record_formattable(record: logging.LogRecord) -> None:
    """Scrubbing an argument must never cost the record its emission.

    A placeholder is text, so a format string that types its conversion
    (``%(attempts)d`` over a collapsed container, ``%d`` over a masked bytes
    argument) can no longer interpolate, and the unhandled error inside
    ``getMessage`` would drop the record at the shared handler rather than
    log it. Dropping the unrenderable arguments keeps the record's own
    redacted format string as the diagnostic, which emits for every format;
    a record that still renders is left exactly as scrubbed.
    """
    if record.args is None:
        return
    try:
        record.getMessage()
    except Exception:
        # Any rendering failure, not one anticipated type: the guard exists
        # precisely because the failing conversion cannot be enumerated.
        record.args = None


def redact_record_exc_info(record: logging.LogRecord) -> None:
    """Replace the attached exception with its sanitized clone and no frames.

    ``logging.Formatter`` caches its rendered traceback on ``record.exc_text``
    and every later handler reuses that cache verbatim, so a record already
    formatted by an earlier unguarded handler would re-emit the raw rendering
    past the sanitized clone. The cache is always discarded: with ``exc_info``
    present the console formatter rebuilds it from the sanitized clone, and a
    preformatted exception-only record deliberately keeps its message while
    the unsanitizable cached exception text is withheld.
    """
    if record.exc_text:
        record.exc_text = None
    if record.exc_info and record.exc_info[0] is not None:
        record.__dict__["exc_info"] = sanitized_exc_info(record.exc_info)


def sanitized_exc_info(
    exc_info: tuple[type[BaseException] | None, BaseException | None, object | None],
) -> tuple[type[BaseException] | None, BaseException | None, object | None]:
    """Clone the exception graph with redacted messages, preserving types."""
    exc_type, exc_value, _traceback = exc_info
    if exc_value is None:
        return (exc_type, exc_value, None)
    return (exc_type, failsafe_sanitized_exception(exc_value), None)


def failsafe_sanitized_exception(exc: BaseException) -> BaseException:
    """Sanitization must never turn an error report into a new uncontrolled error.

    A pathological exception object (for example a raising ``args``
    property) could otherwise crash the shared logging handler and suppress
    the record entirely; the fully masked placeholder keeps the record
    emitting without re-rendering any input bytes.
    """
    try:
        return sanitized_exception(exc)
    except Exception:
        return RuntimeError(FAILSAFE_EXCEPTION_MESSAGE)


def sanitized_exception(exc: BaseException) -> BaseException:
    """A sanitized clone of the whole cause/context graph, cycle- and sharing-safe.

    Cause and context links may legally form cycles (a self-cause, a mutual
    context pair from an ordinary catch → wrap → re-raise-original sequence)
    or share one node between ``__cause__`` and ``__context__``. Identity
    memoization clones every reachable node exactly once, and an iterative
    second pass re-links the clones, so traversal always terminates, a
    shared node stays shared instead of expanding twice, and no chain depth
    can exhaust the interpreter stack — Python's own traceback formatter
    accepts these graphs and the sanitizer must too.
    """
    originals = exception_graph(exc)
    clones = {id(node): sanitized_node(node) for node in originals}
    for node in originals:
        link_clone(clones, node)
    return clones[id(exc)]


def exception_graph(exc: BaseException) -> list[BaseException]:
    """Every exception reachable through cause/context links, each exactly once."""
    ordered: list[BaseException] = []
    seen: set[int] = set()
    pending = [exc]
    while pending:
        node = pending.pop()
        if id(node) in seen:
            continue
        seen.add(id(node))
        ordered.append(node)
        pending.extend(linked_nodes(node))
    return ordered


def linked_nodes(node: BaseException) -> list[BaseException]:
    """The node's present cause/context links, ready for traversal."""
    return [link for link in (node.__cause__, node.__context__) if link is not None]


def sanitized_node(exc: BaseException) -> BaseException:
    """One node's clone with redacted message and notes, links not yet attached."""
    clone = clone_with_redacted_message(exc)
    copy_redacted_notes(clone, exc)
    return clone


def link_clone(clones: dict[int, BaseException], node: BaseException) -> None:
    """Attach the memoized cause/context clones, mirroring the original links."""
    clone = clones[id(node)]
    clone.__cause__ = linked_clone(clones, node.__cause__)
    clone.__context__ = linked_clone(clones, node.__context__)
    clone.__suppress_context__ = node.__suppress_context__


def linked_clone(
    clones: dict[int, BaseException], link: BaseException | None
) -> BaseException | None:
    if link is None:
        return None
    return clones[id(link)]


def copy_redacted_notes(clone: BaseException, exc: BaseException) -> None:
    """Copy exception notes as redacted text; non-string notes become the placeholder."""
    notes = getattr(exc, "__notes__", None)
    if not notes:
        return
    for note in list(notes):
        clone.add_note(redacted_note(note))


def redacted_note(note: object) -> str:
    """One exception note as safe text; only strings have redactable structure."""
    if isinstance(note, str):
        return redact_text(note)
    return "***"


def clone_with_redacted_message(exc: BaseException) -> BaseException:
    """Clone ``exc`` with redacted positional message arguments.

    ``TradesSourceError`` subclasses take keyword-only ``status``/``kind``/
    ``reason``, so a blind positional rebuild always raises and would lose the
    clone; such constructor shapes fall back to plain redacted text instead of
    a misleading same-type claim.
    """
    redacted_args = tuple(redact_argument(arg) for arg in exc.args)
    try:
        clone = type(exc)(*redacted_args)
    except Exception:
        return fallback_sanitized_error(exc)
    carry_status(clone, exc)
    return clone


def fallback_sanitized_error(exc: BaseException) -> BaseException:
    """A plain error carrying only redacted text when the type resists cloning.

    The fallback renders through the fail-closed exception renderer, never
    ``str(exc)`` directly: stringifying the original would re-emit bytes or
    container arguments verbatim inside the sanitized clone's message. The
    raw original is never attached to the clone — the graph link pass wires
    sanitized clones only.
    """
    return RuntimeError(redact_exception_message(exc))


def carry_status(clone: BaseException, exc: BaseException) -> None:
    """Copy a ``status`` attribute onto the sanitized clone when present."""
    if exc.__dict__.get("status") is not None:
        set_cloned_status(clone, exc)


def set_cloned_status(clone: BaseException, exc: BaseException) -> None:
    """Copy the status attribute without disturbing the clone's type."""
    with contextlib.suppress(AttributeError, TypeError):
        object.__setattr__(clone, "status", exc.__dict__.get("status"))
