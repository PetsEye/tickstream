"""API surface tests. Skipped when the api extra is not installed."""

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("asyncpg")

from api.main import app  # noqa: E402

EXPECTED_PATHS = {
    "/health",
    "/api/symbols",
    "/api/summary",
    "/api/candles",
    "/api/anomalies",
    "/api/stream",
}


def test_expected_routes_are_registered():
    paths = set(app.openapi()["paths"])
    assert paths >= EXPECTED_PATHS
