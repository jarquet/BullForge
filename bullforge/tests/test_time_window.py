from datetime import datetime, timezone

import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request

from shared.time_window import parse_time_window


def _req(query: str = ""):
    return make_mocked_request("GET", "/test" + (("?" + query) if query else ""))


def test_no_params_returns_none_tuple():
    from_dt, to_dt = parse_time_window(_req())
    assert from_dt is None
    assert to_dt is None


def test_parses_iso_with_z():
    from_dt, to_dt = parse_time_window(
        _req("from=2026-04-01T00:00:00Z&to=2026-04-17T00:00:00Z"),
    )
    assert from_dt == datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
    assert to_dt == datetime(2026, 4, 17, 0, 0, 0, tzinfo=timezone.utc)


def test_parses_iso_with_offset():
    from_dt, _ = parse_time_window(_req("from=2026-04-01T00:00:00+00:00"))
    assert from_dt == datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_naive_iso_is_treated_as_utc():
    from_dt, _ = parse_time_window(_req("from=2026-04-01T00:00:00"))
    assert from_dt == datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_rejects_malformed_from():
    with pytest.raises(web.HTTPBadRequest):
        parse_time_window(_req("from=not-a-date"))


def test_rejects_malformed_to():
    with pytest.raises(web.HTTPBadRequest):
        parse_time_window(_req("to=not-a-date"))


def test_only_from_is_valid():
    from_dt, to_dt = parse_time_window(_req("from=2026-04-01T00:00:00Z"))
    assert from_dt == datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
    assert to_dt is None


def test_only_to_is_valid():
    from_dt, to_dt = parse_time_window(_req("to=2026-04-17T00:00:00Z"))
    assert from_dt is None
    assert to_dt == datetime(2026, 4, 17, 0, 0, 0, tzinfo=timezone.utc)
