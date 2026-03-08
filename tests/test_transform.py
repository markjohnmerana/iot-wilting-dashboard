"""
Tests for the Transform module.
Run with: pytest tests/test_transform.py -v
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
import pandas as pd
import numpy as np
from transform.plant_transformer import PlantTransformer

# ── Sample raw data (mimics extractor output) ─────────────────
RAW_COLUMNS = [
    "Timestamp", "Plant Name", "Status", "Confidence Score (%)",
    "Moisture Sensor", "Humidity", "Nitrogen", "Phosphorus", "Potassium",
    "Temperature", "Time", "Outside Temperature", "Outside Humidity",
    "Inside Temperature", "Inside Humidity",
]


def make_raw_df(rows):
    return pd.DataFrame(rows, columns=RAW_COLUMNS)


@pytest.fixture
def transformer():
    return PlantTransformer()


@pytest.fixture
def good_df():
    return make_raw_df([
        ["2025-04-15 02:28:40", "Plant 1", "Healthy", "83.54", "50",   "67.2", "0",  "19", "0",  "33.8", "02:28", "24.5", "90.8", "25.2", ""],
        ["2025-04-15 02:28:40", "Plant 2", "healthy", "87.45", "35",   "67.2", "0",  "19", "0",  "33.8", "02:28", "24.7", "90.8", "25.5", ""],
        ["2025-05-05 16:04:32", "Plant 1", "Wilted",  "91.99", "50.1", "67.2", "21", "19", "18", "33.8", "16:04", "24.8", "91.2", "25.8", ""],
    ])


# ── Tests ─────────────────────────────────────────────────────

class TestPlantTransformer:

    def test_column_rename(self, transformer, good_df):
        result = transformer.transform(good_df)
        assert "timestamp"           in result.columns
        assert "plant_name"          in result.columns
        assert "confidence_score_pct" in result.columns
        assert "Timestamp"           not in result.columns

    def test_timestamp_is_datetime(self, transformer, good_df):
        result = transformer.transform(good_df)
        assert pd.api.types.is_datetime64_any_dtype(result["timestamp"])

    def test_numeric_columns_are_float(self, transformer, good_df):
        result = transformer.transform(good_df)
        for col in ["confidence_score_pct", "moisture_sensor", "humidity"]:
            assert pd.api.types.is_float_dtype(result[col]), f"{col} should be float"

    def test_status_title_cased(self, transformer, good_df):
        """'healthy' should become 'Healthy' after casing normalisation."""
        result = transformer.transform(good_df)
        assert all(result["status"] == result["status"].str.title())

    def test_duplicate_rows_removed(self, transformer):
        """Duplicate (timestamp, plant_name) rows should be deduplicated."""
        df = make_raw_df([
            ["2025-04-15 02:28:40", "Plant 1", "Healthy", "83.54", "50", "67.2", "0", "19", "0", "33.8", "02:28", "24.5", "90.8", "25.2", ""],
            ["2025-04-15 02:28:40", "Plant 1", "Healthy", "83.54", "50", "67.2", "0", "19", "0", "33.8", "02:28", "24.5", "90.8", "25.2", ""],
        ])
        result = transformer.transform(df)
        assert len(result) == 1

    def test_invalid_timestamp_dropped(self, transformer):
        """Rows with unparseable timestamps should be dropped."""
        df = make_raw_df([
            ["NOT A DATE",          "Plant 1", "Healthy", "83.54", "50", "67.2", "0", "19", "0", "33.8", "02:28", "24.5", "90.8", "25.2", ""],
            ["2025-04-15 02:28:40", "Plant 2", "Healthy", "87.45", "35", "67.2", "0", "19", "0", "33.8", "02:28", "24.7", "90.8", "25.5", ""],
        ])
        result = transformer.transform(df)
        assert len(result) == 1
        assert "Plant 2" in result["plant_name"].values

    def test_etl_metadata_added(self, transformer, good_df):
        """etl_inserted_at column must be added."""
        result = transformer.transform(good_df)
        assert "etl_inserted_at" in result.columns

    def test_empty_df_returns_empty(self, transformer):
        """Empty input should produce empty output without error."""
        result = transformer.transform(pd.DataFrame())
        assert result.empty
