"""
Tests for the Load module.
Uses mocked SQLAlchemy engine — no real DB required.
Run with: pytest tests/test_load.py -v
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, call
from load.sqlserver_loader import SQLServerLoader


# ── Fixtures ──────────────────────────────────────────────────

@pytest.fixture
def mock_loader():
    """Return a SQLServerLoader with a fully mocked SQLAlchemy engine."""
    with patch("load.sqlserver_loader.create_engine") as mock_create:
        mock_engine = MagicMock()
        mock_create.return_value = mock_engine
        loader = SQLServerLoader(connection_string="mssql+pyodbc://fake")
        loader._engine = mock_engine
        yield loader, mock_engine


@pytest.fixture
def clean_df():
    return pd.DataFrame([
        {
            "timestamp"           : "2025-04-15 02:28:40",
            "plant_name"          : "Plant 1",
            "status"              : "Healthy",
            "confidence_score_pct": 83.54,
            "moisture_sensor"     : 50.0,
            "humidity"            : 67.2,
            "nitrogen"            : 0.0,
            "phosphorus"          : 19.0,
            "potassium"           : 0.0,
            "temperature"         : 33.8,
            "reading_time"        : "02:28",
            "outside_temperature" : 24.5,
            "outside_humidity"    : 90.8,
            "inside_temperature"  : 25.2,
            "inside_humidity"     : None,
            "etl_inserted_at"     : "2025-05-01 10:00:00",
        }
    ])


# ── Tests ─────────────────────────────────────────────────────

class TestSQLServerLoader:

    def test_load_returns_row_count(self, mock_loader, clean_df):
        loader, engine = mock_loader
        ctx = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=ctx)
        engine.begin.return_value.__exit__  = MagicMock(return_value=False)

        count = loader.load(clean_df)
        assert count == 1

    def test_load_empty_df_returns_zero(self, mock_loader):
        loader, _ = mock_loader
        count = loader.load(pd.DataFrame())
        assert count == 0

    def test_get_watermark_returns_none_when_empty(self, mock_loader):
        loader, engine = mock_loader
        ctx = MagicMock()
        ctx.execute.return_value.fetchone.return_value = None
        engine.connect.return_value.__enter__ = MagicMock(return_value=ctx)
        engine.connect.return_value.__exit__  = MagicMock(return_value=False)

        result = loader.get_watermark()
        assert result is None

    def test_get_watermark_returns_value(self, mock_loader):
        loader, engine = mock_loader
        ctx = MagicMock()
        ctx.execute.return_value.fetchone.return_value = ("2025-04-15 02:28:40",)
        engine.connect.return_value.__enter__ = MagicMock(return_value=ctx)
        engine.connect.return_value.__exit__  = MagicMock(return_value=False)

        result = loader.get_watermark()
        assert result == "2025-04-15 02:28:40"

    def test_update_watermark_executes_merge(self, mock_loader):
        loader, engine = mock_loader
        ctx = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=ctx)
        engine.begin.return_value.__exit__  = MagicMock(return_value=False)

        loader.update_watermark("2025-05-05 16:04:32")
        ctx.execute.assert_called_once()

    def test_connection_test_ok(self, mock_loader):
        loader, engine = mock_loader
        ctx = MagicMock()
        engine.connect.return_value.__enter__ = MagicMock(return_value=ctx)
        engine.connect.return_value.__exit__  = MagicMock(return_value=False)

        assert loader.test_connection() is True

    def test_connection_test_fails(self, mock_loader):
        from sqlalchemy.exc import SQLAlchemyError
        loader, engine = mock_loader
        engine.connect.side_effect = SQLAlchemyError("Connection refused")

        assert loader.test_connection() is False
