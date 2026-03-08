"""
run_local.py — Local pipeline test runner
──────────────────────────────────────────
Use this to run and debug the full ETL pipeline on your local machine
WITHOUT Airflow or Docker.

Prerequisites:
  1. pip install -r requirements.txt
  2. cp .env.example .env  (and fill in your values)
  3. Place your google_credentials.json in credentials/
  4. Run the SQL schema:  sqlcmd -S localhost -U sa -i sql/schema.sql

Usage:
  python run_local.py              # incremental (uses watermark)
  python run_local.py --full       # force full load (ignores watermark)
  python run_local.py --test-only  # only test connections, no data load
"""

import sys
import argparse
import pandas as pd
from datetime import datetime

from config import TARGET_TABLE, WATERMARK_TABLE
from logger import get_logger
from extract.google_sheets_extractor import GoogleSheetsExtractor
from transform.plant_transformer import PlantTransformer
from load.sqlserver_loader import SQLServerLoader

log = get_logger("run_local")


def run_pipeline(force_full_load: bool = False, test_only: bool = False):
    start_time = datetime.utcnow()
    log.info("=" * 60)
    log.info("Plant Monitoring ETL — Local Run")
    log.info("Started at: %s UTC", start_time.isoformat())
    log.info("=" * 60)

    # ── 1. Initialise components ──────────────────────────────
    loader      = SQLServerLoader()
    extractor   = GoogleSheetsExtractor()
    transformer = PlantTransformer()

    # ── 2. Test connections ───────────────────────────────────
    log.info("Step 1/5: Testing SQL Server connection...")
    if not loader.test_connection():
        log.error("[FAIL] Cannot connect to SQL Server. Check your .env settings.")
        sys.exit(1)
    log.info("[OK] SQL Server connection OK.")

    if test_only:
        log.info("--test-only flag set. Exiting after connection test.")
        return

    # ── 3. Get watermark ──────────────────────────────────────
    log.info("Step 2/5: Reading watermark...")
    watermark = None if force_full_load else loader.get_watermark()

    if force_full_load:
        log.info("Full load requested — watermark ignored.")
    else:
        log.info("Watermark: %s", watermark or "None (first run)")

    # ── 4. Extract ────────────────────────────────────────────
    log.info("Step 3/5: Extracting from Google Sheets...")
    raw_df = extractor.extract(last_watermark=watermark)

    if raw_df.empty:
        log.info("[OK] No new data since last run. Pipeline complete.")
        return

    log.info("Extracted %d rows.", len(raw_df))

    # ── 5. Transform ──────────────────────────────────────────
    log.info("Step 4/5: Transforming data...")
    clean_df = transformer.transform(raw_df)
    log.info("Clean rows: %d", len(clean_df))

    if clean_df.empty:
        log.warning("Transform produced 0 rows. Check your source data.")
        return

    # Preview
    log.info("Sample output:\n%s", clean_df.head(3).to_string())

    # ── 6. Load ───────────────────────────────────────────────
    log.info("Step 5/5: Loading into SQL Server [%s]...", TARGET_TABLE)
    rows_loaded = loader.load(clean_df)
    log.info("[OK] Loaded %d rows.", rows_loaded)

    # ── 7. Update watermark ───────────────────────────────────
    clean_df["timestamp"] = pd.to_datetime(clean_df["timestamp"])
    new_watermark = clean_df["timestamp"].max().isoformat()
    loader.update_watermark(new_watermark)

    # ── Summary ───────────────────────────────────────────────
    elapsed = (datetime.utcnow() - start_time).total_seconds()
    log.info("=" * 60)
    log.info("[OK] Pipeline complete in %.1f seconds.", elapsed)
    log.info("   Rows extracted : %d", len(raw_df))
    log.info("   Rows loaded    : %d", rows_loaded)
    log.info("   New watermark  : %s", new_watermark)
    log.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ETL pipeline locally.")
    parser.add_argument("--full",      action="store_true", help="Force full load")
    parser.add_argument("--test-only", action="store_true", help="Only test connections")
    args = parser.parse_args()

    run_pipeline(force_full_load=args.full, test_only=args.test_only)
