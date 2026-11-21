import os
import pytest

# Override settings for tests to use localhost instead of docker service names
os.environ.setdefault("TIMESCALEDB_HOST", "localhost")
os.environ.setdefault("REDIS_HOST", "localhost")

from shared.config import Settings


@pytest.fixture
def settings():
    return Settings()


def pytest_collection_modifyitems(config, items):
    # Everything under tests/integration/ talks to a real TimescaleDB/Redis.
    # Unit modules that do are marked explicitly with @pytest.mark.services.
    for item in items:
        if "integration" in item.path.parts:
            item.add_marker(pytest.mark.services)
