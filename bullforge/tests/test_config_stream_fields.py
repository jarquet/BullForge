import os
from unittest.mock import patch

from shared.config import Settings


def test_execution_stream_queue_max_default():
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("EXECUTION_STREAM_QUEUE_MAX", None)
        s = Settings()
    assert s.execution_stream_queue_max == 1000


def test_execution_stream_queue_max_env_override():
    with patch.dict(os.environ, {"EXECUTION_STREAM_QUEUE_MAX": "250"}):
        s = Settings()
    assert s.execution_stream_queue_max == 250


def test_execution_stream_shutdown_drain_s_default():
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("EXECUTION_STREAM_SHUTDOWN_DRAIN_S", None)
        s = Settings()
    assert s.execution_stream_shutdown_drain_s == 5.0


def test_execution_stream_shutdown_drain_s_env_override():
    with patch.dict(os.environ, {"EXECUTION_STREAM_SHUTDOWN_DRAIN_S": "2.5"}):
        s = Settings()
    assert s.execution_stream_shutdown_drain_s == 2.5
