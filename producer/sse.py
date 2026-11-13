"""Minimal, dependency-free Server-Sent Events parser.

Implements the parts of the WHATWG SSE spec that Wikimedia EventStreams uses:
`event:`, `data:` (multi-line), `id:`, comments (`:`), and blank-line dispatch.
Kept separate from the network code so it can be unit tested exhaustively.
"""
from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass


@dataclass(frozen=True)
class SSEEvent:
    event: str
    data: str
    id: str | None


def parse_sse(lines: Iterable[str]) -> Iterator[SSEEvent]:
    """Turn an iterable of decoded lines (without trailing newlines) into events."""
    event_type = "message"
    data_buf: list[str] = []
    last_id: str | None = None

    for raw in lines:
        line = raw.rstrip("\r")
        if line == "":
            if data_buf:
                yield SSEEvent(event=event_type, data="\n".join(data_buf), id=last_id)
            event_type, data_buf = "message", []
            continue
        if line.startswith(":"):
            continue  # comment / keep-alive
        field, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]
        if field == "event":
            event_type = value
        elif field == "data":
            data_buf.append(value)
        elif field == "id":
            # Per spec the id persists across events until changed.
            last_id = value
        # `retry:` and unknown fields are ignored.

    if data_buf:  # stream ended without a trailing blank line
        yield SSEEvent(event=event_type, data="\n".join(data_buf), id=last_id)
