"""
Extract module — Google Sheets
─────────────────────────────
Pulls raw data from a Google Sheet and returns a pandas DataFrame.
Supports incremental loads via a watermark (last ingested timestamp).
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from typing import Optional
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from config import (
    GOOGLE_CREDENTIALS_PATH,
    SPREADSHEET_ID,
    SHEET_NAME,
    TIMESTAMP_COLUMN,
)
from logger import get_logger

log = get_logger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


class GoogleSheetsExtractor:
    """
    Extracts plant monitoring data from a Google Sheet.

    Usage
    -----
    extractor = GoogleSheetsExtractor()
    df = extractor.extract(last_watermark="2025-04-15 02:28:40")
    """

    def __init__(
        self,
        credentials_path: str = GOOGLE_CREDENTIALS_PATH,
        spreadsheet_id: str   = SPREADSHEET_ID,
        sheet_name: str       = SHEET_NAME,
    ):
        self.spreadsheet_id  = spreadsheet_id
        self.sheet_name      = sheet_name
        self._service        = self._build_service(credentials_path)

    # ──────────────────────────────────────────
    # Public
    # ──────────────────────────────────────────

    def extract(self, last_watermark: Optional[str] = None) -> pd.DataFrame:
        """
        Fetch all rows from the sheet, then filter to rows newer than
        last_watermark so only new records are returned (incremental load).

        Parameters
        ----------
        last_watermark : str or None
            ISO-format timestamp string. If None, all rows are returned
            (full load on first run).

        Returns
        -------
        pd.DataFrame
        """
        log.info("Starting extraction from sheet '%s'", self.sheet_name)

        raw_df = self._fetch_sheet()

        if raw_df.empty:
            log.warning("Sheet returned no data.")
            return raw_df

        log.info("Total rows fetched from sheet: %d", len(raw_df))

        if last_watermark:
            watermark_ts = pd.to_datetime(last_watermark)
            raw_df[TIMESTAMP_COLUMN] = pd.to_datetime(raw_df[TIMESTAMP_COLUMN])
            filtered = raw_df[raw_df[TIMESTAMP_COLUMN] > watermark_ts].copy()
            log.info(
                "Incremental filter applied (watermark=%s). New rows: %d",
                last_watermark,
                len(filtered),
            )
            return filtered
        else:
            log.info("No watermark found — performing full load.")
            return raw_df

    # ──────────────────────────────────────────
    # Private helpers
    # ──────────────────────────────────────────

    def _build_service(self, credentials_path: str):
        """Authenticate and return the Sheets API service object."""
        try:
            creds = Credentials.from_service_account_file(
                credentials_path, scopes=SCOPES
            )
            service = build("sheets", "v4", credentials=creds)
            log.info("Google Sheets service authenticated successfully.")
            return service
        except FileNotFoundError:
            log.error(
                "Credentials file not found at '%s'. "
                "Place your google_credentials.json in the credentials/ folder.",
                credentials_path,
            )
            raise
        except Exception as exc:
            log.error("Failed to authenticate with Google Sheets: %s", exc)
            raise

    def _fetch_sheet(self) -> pd.DataFrame:
        """Call the Sheets API and return a DataFrame with correct headers."""
        try:
            result = (
                self._service.spreadsheets()
                .values()
                .get(
                    spreadsheetId=self.spreadsheet_id,
                    range=self.sheet_name,
                )
                .execute()
            )
        except HttpError as exc:
            log.error("Google Sheets API error: %s", exc)
            raise

        rows = result.get("values", [])
        if not rows:
            return pd.DataFrame()

        headers = [h.strip() for h in rows[0]]
        data    = rows[1:]

        # Pad short rows so every row matches header length
        data = [row + [""] * (len(headers) - len(row)) for row in data]

        df = pd.DataFrame(data, columns=headers)
        log.debug("Raw columns from sheet: %s", list(df.columns))
        return df
