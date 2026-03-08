"""
Transform module — Plant Monitoring Data
────────────────────────────────────────
Cleans and standardises the raw DataFrame coming out of the extractor.

Column map (sheet → database)
──────────────────────────────
Timestamp              → timestamp
Plant Name             → plant_name
Status                 → status
Confidence Score (%)   → confidence_score_pct
Moisture Sensor        → moisture_sensor
Humidity               → humidity
Nitrogen               → nitrogen
Phosphorus             → phosphorus
Potassium              → potassium
Temperature            → temperature
Time                   → reading_time
Outside Temperature    → outside_temperature
Outside Humidity       → outside_humidity
Inside Temperature     → inside_temperature
Inside Humidity        → inside_humidity
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import numpy as np
from logger import get_logger

log = get_logger(__name__)

# Exact column names as they appear in the Google Sheet header row
COLUMN_RENAME_MAP = {
    "Timestamp"            : "timestamp",
    "Plant Name"           : "plant_name",
    "Status"               : "status",
    "Confidence Score (%)" : "confidence_score_pct",
    "Moisture Sensor"      : "moisture_sensor",
    "Humidity"             : "humidity",
    "Nitrogen"             : "nitrogen",
    "Phosphorus"           : "phosphorus",
    "Potassium"            : "potassium",
    "Temperature"          : "temperature",
    "Time"                 : "reading_time",
    "Outside Temperature"  : "outside_temperature",
    "Outside Humidity"     : "outside_humidity",
    "Inside Temperature"   : "inside_temperature",
    "Inside Humidity"      : "inside_humidity",
}

NUMERIC_COLS = [
    "confidence_score_pct",
    "moisture_sensor",
    "humidity",
    "nitrogen",
    "phosphorus",
    "potassium",
    "temperature",
    "outside_temperature",
    "outside_humidity",
    "inside_temperature",
    "inside_humidity",
]


class PlantTransformer:
    """
    Cleans and type-casts the raw plant monitoring DataFrame.

    Usage
    -----
    transformer = PlantTransformer()
    clean_df = transformer.transform(raw_df)
    """

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Orchestrates all transformation steps.

        Parameters
        ----------
        df : pd.DataFrame  — raw DataFrame from GoogleSheetsExtractor

        Returns
        -------
        pd.DataFrame  — cleaned, typed DataFrame ready for loading
        """
        if df.empty:
            log.warning("Transform received an empty DataFrame. Skipping.")
            return df

        log.info("Starting transform on %d rows.", len(df))

        df = self._rename_columns(df)
        df = self._cast_timestamp(df)
        df = self._cast_numerics(df)
        df = self._clean_strings(df)
        df = self._drop_duplicates(df)
        df = self._drop_empty_rows(df)
        df = self._add_metadata(df)

        log.info("Transform complete. Output rows: %d", len(df))
        return df

    # ──────────────────────────────────────────
    # Steps
    # ──────────────────────────────────────────

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rename sheet headers to snake_case DB column names."""
        df = df.rename(columns=COLUMN_RENAME_MAP)
        missing = [c for c in COLUMN_RENAME_MAP.values() if c not in df.columns]
        if missing:
            log.warning("Expected columns not found after rename: %s", missing)
        log.debug("Columns after rename: %s", list(df.columns))
        return df

    def _cast_timestamp(self, df: pd.DataFrame) -> pd.DataFrame:
        """Parse timestamp column to datetime, drop unparseable rows."""
        if "timestamp" not in df.columns:
            log.error("'timestamp' column is missing — cannot continue.")
            raise KeyError("'timestamp' column missing from source data.")

        before = len(df)
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["timestamp"])
        dropped = before - len(df)
        if dropped:
            log.warning("Dropped %d rows with unparseable timestamps.", dropped)
        return df

    def _cast_numerics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Coerce numeric columns to float, leaving bad values as NaN."""
        for col in NUMERIC_COLS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                nan_count = df[col].isna().sum()
                if nan_count:
                    log.debug("Column '%s' has %d NaN values after cast.", col, nan_count)
            else:
                log.warning("Numeric column '%s' not found in DataFrame.", col)
        return df

    def _clean_strings(self, df: pd.DataFrame) -> pd.DataFrame:
        """Strip whitespace, normalise status and plant_name casing."""
        for col in ["plant_name", "status"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.title()
        return df

    def _drop_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove exact duplicate rows (same timestamp + plant_name)."""
        before = len(df)
        df = df.drop_duplicates(subset=["timestamp", "plant_name"], keep="last")
        dropped = before - len(df)
        if dropped:
            log.info("Dropped %d duplicate rows.", dropped)
        return df

    def _drop_empty_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop rows where all key sensor values are NaN."""
        key_cols = [c for c in NUMERIC_COLS if c in df.columns]
        before = len(df)
        df = df.dropna(subset=key_cols, how="all")
        dropped = before - len(df)
        if dropped:
            log.info("Dropped %d rows with all-NaN sensor values.", dropped)
        return df

    def _add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Attach ETL metadata columns for auditability."""
        df["etl_inserted_at"] = pd.Timestamp.utcnow()
        return df
