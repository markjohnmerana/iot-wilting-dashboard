"""
Central configuration for the Plant Monitoring ETL pipeline.
All environment variables are loaded here. Never hardcode credentials.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ──────────────────────────────────────────────
# Google Sheets
# ──────────────────────────────────────────────
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials/google_credentials.json")
SPREADSHEET_ID          = os.getenv("Sheetname", "your_google_sheet_id_here")
SHEET_NAME              = os.getenv("SHEET_NAME", "Sheet1")

# ──────────────────────────────────────────────
# SQL Server
# ──────────────────────────────────────────────
SQL_SERVER_HOST     = os.getenv("SQL_SERVER_HOST", "localhost")
SQL_SERVER_PORT     = os.getenv("SQL_SERVER_PORT", "localport")
SQL_SERVER_DATABASE = os.getenv("SQL_SERVER_DATABASE", "PlantMonitoring")
SQL_SERVER_USER     = os.getenv("SQL_SERVER_USER", "sa")
SQL_SERVER_PASSWORD = os.getenv("SQL_SERVER_PASSWORD", "strong_password")
SQL_SERVER_DRIVER   = os.getenv("SQL_SERVER_DRIVER", "ODBC Driver 17 for SQL Server")

# Connection string for SQLAlchemy
SQLALCHEMY_CONNECTION_STRING = (
    f"mssql+pyodbc://{SQL_SERVER_USER}:{SQL_SERVER_PASSWORD}"
    f"@{SQL_SERVER_HOST}:{SQL_SERVER_PORT}/{SQL_SERVER_DATABASE}"
    f"?driver={SQL_SERVER_DRIVER.replace(' ', '+')}"
    f"&TrustServerCertificate=yes"
)

# ──────────────────────────────────────────────
# ETL Settings
# ──────────────────────────────────────────────
WATERMARK_TABLE  = "etl_watermark"       # Tracks last ingested timestamp
TARGET_TABLE     = "plant_readings"      # Main target table
TIMESTAMP_COLUMN = "Timestamp"           # Column used for incremental loads
BATCH_SIZE       = 500                   # Rows per insert batch

# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_DIR   = os.getenv("LOG_DIR", "logs")
