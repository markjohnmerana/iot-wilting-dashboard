-- ============================================================
-- Plant Monitoring ETL — SQL Server Schema
-- Run this ONCE before the first DAG execution.
-- Compatible with SQL Server 2017+
-- ============================================================

USE PlantMonitoring;      -- Change to your database name if different
GO

-- ────────────────────────────────────────────────────────────
-- 1. Main plant readings table
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('dbo.plant_readings', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.plant_readings (
        id                   BIGINT IDENTITY(1,1)  NOT NULL,

        -- Natural key
        timestamp            DATETIME2(0)           NOT NULL,
        plant_name           NVARCHAR(100)          NOT NULL,

        -- Status
        status               NVARCHAR(50)           NULL,
        confidence_score_pct DECIMAL(6, 2)          NULL,   -- e.g. 91.99

        -- Sensor readings
        moisture_sensor      DECIMAL(8, 2)          NULL,
        humidity             DECIMAL(8, 2)          NULL,
        nitrogen             DECIMAL(8, 2)          NULL,
        phosphorus           DECIMAL(8, 2)          NULL,
        potassium            DECIMAL(8, 2)          NULL,
        temperature          DECIMAL(6, 2)          NULL,
        reading_time         NVARCHAR(20)           NULL,   -- raw "HH:MM" from sheet

        -- Environmental readings
        outside_temperature  DECIMAL(6, 2)          NULL,
        outside_humidity     DECIMAL(6, 2)          NULL,
        inside_temperature   DECIMAL(6, 2)          NULL,
        inside_humidity      DECIMAL(6, 2)          NULL,

        -- ETL metadata
        etl_inserted_at      DATETIME2(0)           NOT NULL DEFAULT GETUTCDATE(),

        CONSTRAINT PK_plant_readings        PRIMARY KEY CLUSTERED (id),
        CONSTRAINT UQ_plant_readings_ts_pn  UNIQUE (timestamp, plant_name)
    );
    PRINT 'Table dbo.plant_readings created.';
END
ELSE
    PRINT 'Table dbo.plant_readings already exists — skipping.';
GO

-- ────────────────────────────────────────────────────────────
-- 2. Indexes for common query patterns
-- ────────────────────────────────────────────────────────────

-- Fast lookups by plant over time
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_plant_readings_plant_ts'
)
    CREATE NONCLUSTERED INDEX IX_plant_readings_plant_ts
        ON dbo.plant_readings (plant_name, timestamp DESC);
GO

-- Fast range scans by timestamp (used by the watermark filter)
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_plant_readings_ts'
)
    CREATE NONCLUSTERED INDEX IX_plant_readings_ts
        ON dbo.plant_readings (timestamp DESC);
GO

-- Status filtering
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_plant_readings_status'
)
    CREATE NONCLUSTERED INDEX IX_plant_readings_status
        ON dbo.plant_readings (status)
        INCLUDE (plant_name, timestamp, confidence_score_pct);
GO

-- ────────────────────────────────────────────────────────────
-- 3. Watermark table (tracks incremental load state)
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('dbo.etl_watermark', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.etl_watermark (
        id              INT IDENTITY(1,1)  NOT NULL,
        pipeline_name   NVARCHAR(100)      NOT NULL,
        last_watermark  DATETIME2(0)       NOT NULL,
        updated_at      DATETIME2(0)       NOT NULL DEFAULT GETUTCDATE(),

        CONSTRAINT PK_etl_watermark            PRIMARY KEY CLUSTERED (id),
        CONSTRAINT UQ_etl_watermark_pipeline    UNIQUE (pipeline_name)
    );
    PRINT 'Table dbo.etl_watermark created.';
END
ELSE
    PRINT 'Table dbo.etl_watermark already exists — skipping.';
GO

-- ────────────────────────────────────────────────────────────
-- 4. ETL run log (optional — helpful for debugging)
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('dbo.etl_run_log', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.etl_run_log (
        id              BIGINT IDENTITY(1,1)  NOT NULL,
        pipeline_name   NVARCHAR(100)         NOT NULL,
        dag_run_id      NVARCHAR(200)         NULL,
        started_at      DATETIME2(0)          NOT NULL,
        finished_at     DATETIME2(0)          NULL,
        rows_extracted  INT                   NULL,
        rows_loaded     INT                   NULL,
        status          NVARCHAR(20)          NOT NULL DEFAULT 'RUNNING',   -- RUNNING | SUCCESS | FAILED
        error_message   NVARCHAR(MAX)         NULL,

        CONSTRAINT PK_etl_run_log PRIMARY KEY CLUSTERED (id)
    );
    PRINT 'Table dbo.etl_run_log created.';
END
ELSE
    PRINT 'Table dbo.etl_run_log already exists — skipping.';
GO

-- ────────────────────────────────────────────────────────────
-- 5. Handy views for quick inspection
-- ────────────────────────────────────────────────────────────

-- Latest reading per plant
CREATE OR ALTER VIEW dbo.vw_latest_plant_readings AS
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY plant_name ORDER BY timestamp DESC) AS rn
        FROM dbo.plant_readings
    ) ranked
    WHERE rn = 1;
GO

-- Daily averages per plant
CREATE OR ALTER VIEW dbo.vw_daily_plant_averages AS
    SELECT
        CAST(timestamp AS DATE)   AS reading_date,
        plant_name,
        COUNT(*)                  AS reading_count,
        AVG(confidence_score_pct) AS avg_confidence,
        AVG(moisture_sensor)      AS avg_moisture,
        AVG(humidity)             AS avg_humidity,
        AVG(temperature)          AS avg_temperature,
        AVG(outside_temperature)  AS avg_outside_temp,
        AVG(inside_temperature)   AS avg_inside_temp
    FROM dbo.plant_readings
    GROUP BY CAST(timestamp AS DATE), plant_name;
GO

PRINT ' Schema setup complete. All tables, indexes, and views are ready.';
