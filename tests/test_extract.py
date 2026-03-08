"""
Tests for the Extract module.

Run locally (without a real Google Sheet) using the mock below.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from extract.google_sheets_extractor import GoogleSheetsExtractor


# ── Fixtures ──────────────────────────────────────────────────

MOCK_SHEET_ROWS = [
    ["Timestamp", "Plant Name", "Status", "Confidence Score (%)",
     "Moisture Sensor", "Humidity", "Nitrogen", "Phosphorus", "Potassium",
     "Temperature", "Time", "Outside Temperature", "Outside Humidity",
     "Inside Temperature", "Inside Humidity"],
    ["2025-04-15 02:28:40", "Plant 1", "Healthy", "83.54", "50",
     "67.2", "0", "19", "0", "33.8", "02:28", "24.5", "90.8", "25.2", ""],
    ["2025-04-15 02:28:40", "Plant 2", "Healthy", "87.45", "35",
     "67.2", "0", "19", "0", "33.8", "02:28", "24.7", "90.8", "25.5", ""],
    ["2025-05-05 16:04:32", "Plant 1", "Wilted",  "91.99", "50.1",
     "67.2", "21", "19", "18", "33.8", "16:04", "24.8", "91.2", "25.8", ""],
]


@pytest.fixture
def mock_extractor():
    """Return a GoogleSheetsExtractor with a mocked API service."""
    with patch.object(GoogleSheetsExtractor, "_build_service") as mock_build:
        mock_service = MagicMock()
        mock_values  = mock_service.spreadsheets.return_value.values.return_value
        mock_values.get.return_value.execute.return_value = {"values": MOCK_SHEET_ROWS}
        mock_build.return_value = mock_service
        extractor = GoogleSheetsExtractor(
            credentials_path="fake_path.json",
            spreadsheet_id="FAKE_ID",
            sheet_name="Sheet1",
        )
        yield extractor


# ── Tests ─────────────────────────────────────────────────────

class TestGoogleSheetsExtractor:

    def test_full_load_returns_all_data_rows(self, mock_extractor):
        """Without a watermark, all data rows should be returned."""
        df = mock_extractor.extract(last_watermark=None)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3   # 3 data rows (header excluded)

    def test_incremental_load_filters_old_rows(self, mock_extractor):
        """With a watermark, only rows newer than it are returned."""
        df = mock_extractor.extract(last_watermark="2025-04-15 02:28:40")
        assert len(df) == 1
        assert "Plant 1" in df["Plant Name"].values

    def test_columns_match_sheet_header(self, mock_extractor):
        """Column names must match the sheet header row."""
        df = mock_extractor.extract()
        expected_cols = [
            "Timestamp", "Plant Name", "Status", "Confidence Score (%)",
            "Moisture Sensor", "Humidity", "Nitrogen", "Phosphorus", "Potassium",
            "Temperature", "Time", "Outside Temperature", "Outside Humidity",
            "Inside Temperature", "Inside Humidity",
        ]
        assert list(df.columns) == expected_cols

    def test_empty_sheet_returns_empty_dataframe(self, mock_extractor):
        """An empty sheet should produce an empty DataFrame without error."""
        mock_extractor._service.spreadsheets().values().get().execute.return_value = {
            "values": []
        }
        df = mock_extractor.extract()
        assert df.empty
