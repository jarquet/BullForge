import os
import pytest

# Override settings for tests to use localhost instead of docker service names
os.environ.setdefault("TIMESCALEDB_HOST", "localhost")
os.environ.setdefault("REDIS_HOST", "localhost")

from shared.config import Settings


@pytest.fixture
def settings():
    return Settings()
