"""
Load module — SQL Server
────────────────────────
Handles:
  • Upsert (MERGE) of plant readings into SQL Server
  • Watermark table read/write for incremental tracking
  • Connection management via SQLAlchemy
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from typing import Optional
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from config import (
    SQLALCHEMY_CONNECTION_STRING,
    TARGET_TABLE,
    WATERMARK_TABLE,
    BATCH_SIZE,
    TIMESTAMP_COLUMN,
)
from logger import get_logger

log = get_logger(__name__)


class SQLServerLoader:
    """
    Loads cleaned plant monitoring data into SQL Server.

    Usage
    -----
    loader = SQLServerLoader()
    loader.load(clean_df)
    new_mark = loader.get_watermark()
    loader.update_watermark(new_mark)
    """

    def __init__(self, connection_string: str = SQLALCHEMY_CONNECTION_STRING):
        self._engine = self._create_engine(connection_string)

    # ──────────────────────────────────────────
    # Public
    # ──────────────────────────────────────────

    def load(self, df: pd.DataFrame) -> int:
        """
        MERGE (upsert) rows into the target table.
        Uses (timestamp, plant_name) as the natural key to avoid duplicates
        even if the DAG is re-run.

        Returns
        -------
        int : number of rows processed
        """
        if df.empty:
            log.info("No data to load.")
            return 0

        log.info("Loading %d rows into [%s].", len(df), TARGET_TABLE)
        rows_loaded = 0

        for batch_start in range(0, len(df), BATCH_SIZE):
            batch = df.iloc[batch_start : batch_start + BATCH_SIZE]
            self._upsert_batch(batch)
            rows_loaded += len(batch)
            log.info("  Loaded batch rows %d–%d.", batch_start + 1, rows_loaded)

        log.info("Load complete. Total rows loaded: %d", rows_loaded)
        return rows_loaded

    def get_watermark(self) -> Optional[str]:
        """
        Read the latest watermark from the watermark table.

        Returns
        -------
        str or None — ISO timestamp string, or None if table is empty
        """
        sql = f"""
            SELECT TOP 1 last_watermark
            FROM [{WATERMARK_TABLE}]
            WHERE pipeline_name = 'plant_monitoring'
            ORDER BY updated_at DESC
        """
        try:
            with self._engine.connect() as conn:
                result = conn.execute(text(sql)).fetchone()
                if result:
                    watermark = str(result[0])
                    log.info("Current watermark: %s", watermark)
                    return watermark
                log.info("No watermark found — this is likely the first run.")
                return None
        except SQLAlchemyError as exc:
            log.error("Failed to read watermark: %s", exc)
            raise

    def update_watermark(self, new_watermark: str) -> None:
        """
        Upsert the watermark record with the latest processed timestamp.

        Parameters
        ----------
        new_watermark : str — ISO timestamp string
        """
        sql = f"""
            MERGE [{WATERMARK_TABLE}] AS target
            USING (SELECT 'plant_monitoring' AS pipeline_name) AS source
                ON target.pipeline_name = source.pipeline_name
            WHEN MATCHED THEN
                UPDATE SET last_watermark = :wm, updated_at = GETUTCDATE()
            WHEN NOT MATCHED THEN
                INSERT (pipeline_name, last_watermark, updated_at)
                VALUES ('plant_monitoring', :wm, GETUTCDATE());
        """
        try:
            with self._engine.begin() as conn:
                conn.execute(text(sql), {"wm": new_watermark})
            log.info("Watermark updated to: %s", new_watermark)
        except SQLAlchemyError as exc:
            log.error("Failed to update watermark: %s", exc)
            raise

    def test_connection(self) -> bool:
        """Quick connectivity check — returns True if DB is reachable."""
        try:
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            log.info("Database connection test: OK")
            return True
        except SQLAlchemyError as exc:
            log.error("Database connection test FAILED: %s", exc)
            return False

    # ──────────────────────────────────────────
    # Private helpers
    # ──────────────────────────────────────────

    def _create_engine(self, connection_string: str):
        try:
            engine = create_engine(connection_string)
            log.info("SQLAlchemy engine created.")
            return engine
        except Exception as exc:
            log.error("Failed to create SQLAlchemy engine: %s", exc)
            raise

    def _upsert_batch(self, df: pd.DataFrame) -> None:
        import math

        df = df.copy()

        df["timestamp"] = df["timestamp"].astype(str)
        if "etl_inserted_at" in df.columns:
            df["etl_inserted_at"] = pd.to_datetime(df["etl_inserted_at"]).dt.strftime("%Y-%m-%d %H:%M:%S")

        rows = df.to_dict(orient="records")

        # Replace nan with None AFTER to_dict — pandas re-introduces nan for float cols
        cleaned_rows = [
            {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in row.items()}
            for row in rows
        ]

        merge_sql = f"""
            MERGE [plant_readings] AS target
            USING (
                SELECT
                    CAST(:timestamp AS DATETIME2)        AS timestamp,
                    :plant_name                          AS plant_name,
                    :status                              AS status,
                    :confidence_score_pct                AS confidence_score_pct,
                    :moisture_sensor                     AS moisture_sensor,
                    :humidity                            AS humidity,
                    :nitrogen                            AS nitrogen,
                    :phosphorus                          AS phosphorus,
                    :potassium                           AS potassium,
                    :temperature                         AS temperature,
                    :reading_time                        AS reading_time,
                    :outside_temperature                 AS outside_temperature,
                    :outside_humidity                    AS outside_humidity,
                    :inside_temperature                  AS inside_temperature,
                    :inside_humidity                     AS inside_humidity,
                    CAST(:etl_inserted_at AS DATETIME2)  AS etl_inserted_at
            ) AS source
                ON  target.timestamp  = source.timestamp
                AND target.plant_name = source.plant_name
            WHEN MATCHED THEN
                UPDATE SET
                    status                = source.status,
                    confidence_score_pct  = source.confidence_score_pct,
                    moisture_sensor       = source.moisture_sensor,
                    humidity              = source.humidity,
                    nitrogen              = source.nitrogen,
                    phosphorus            = source.phosphorus,
                    potassium             = source.potassium,
                    temperature           = source.temperature,
                    reading_time          = source.reading_time,
                    outside_temperature   = source.outside_temperature,
                    outside_humidity      = source.outside_humidity,
                    inside_temperature    = source.inside_temperature,
                    inside_humidity       = source.inside_humidity,
                    etl_inserted_at       = source.etl_inserted_at
            WHEN NOT MATCHED THEN
                INSERT (
                    timestamp, plant_name, status, confidence_score_pct,
                    moisture_sensor, humidity, nitrogen, phosphorus, potassium,
                    temperature, reading_time, outside_temperature,
                    outside_humidity, inside_temperature, inside_humidity,
                    etl_inserted_at
                )
                VALUES (
                    source.timestamp, source.plant_name, source.status,
                    source.confidence_score_pct, source.moisture_sensor,
                    source.humidity, source.nitrogen, source.phosphorus,
                    source.potassium, source.temperature, source.reading_time,
                    source.outside_temperature, source.outside_humidity,
                    source.inside_temperature, source.inside_humidity,
                    source.etl_inserted_at
                );
        """

        try:
            with self._engine.begin() as conn:
                conn.execute(text(merge_sql), cleaned_rows)
        except SQLAlchemyError as exc:
            log.error("MERGE failed: %s", exc)
            raise