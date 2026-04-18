from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Tuple

from aiohttp import web


def _parse_iso(raw: str, field: str) -> datetime:
    """Parse an ISO 8601 string to a UTC-aware datetime.

    Accepts Z suffix, explicit offset, or naive (treated as UTC).
    Raises HTTPBadRequest on any malformed input.
    """
    try:
        # aiohttp decodes '+' as ' ' in query strings; restore it so that
        # timezone offsets like "+00:00" are preserved.
        s = raw.replace(" ", "+")
        s = s.replace("Z", "+00:00") if s.endswith("Z") else s
        dt = datetime.fromisoformat(s)
    except ValueError as exc:
        raise web.HTTPBadRequest(
            reason=f"invalid ISO datetime for '{field}': {raw!r}",
        ) from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def parse_time_window(
    request: web.Request,
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """Extract optional ?from= and ?to= query params as UTC datetimes.

    Returns (from_dt, to_dt). Either or both can be None when absent.
    Raises HTTPBadRequest on malformed input.
    """
    raw_from = request.query.get("from")
    raw_to = request.query.get("to")
    from_dt = _parse_iso(raw_from, "from") if raw_from else None
    to_dt = _parse_iso(raw_to, "to") if raw_to else None
    return from_dt, to_dt
