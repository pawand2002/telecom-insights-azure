-- ============================================================
-- TelecomInsights — Azure SQL Database Setup
-- Run this in Azure SQL Query Editor (portal) or SSMS
-- ============================================================

-- ── 1. Customer dimension table (source system) ───────────────
CREATE TABLE dbo.customers (
    customer_id         VARCHAR(10)     NOT NULL PRIMARY KEY,
    msisdn              VARCHAR(20)     NOT NULL UNIQUE,
    first_name          VARCHAR(50),
    last_name           VARCHAR(50),
    age                 INT,
    nationality         VARCHAR(50),
    language            VARCHAR(30),
    region              VARCHAR(50),
    device_type         VARCHAR(30),
    plan_type           VARCHAR(20),
    plan_name           VARCHAR(50),
    monthly_fee_qar     DECIMAL(10,2),
    data_allowance_gb   INT,
    activation_date     DATE,
    tenure_months       INT,
    segment             VARCHAR(20),
    churn_risk          VARCHAR(10),
    is_active           BIT             DEFAULT 1,
    created_at          DATETIME        DEFAULT GETDATE(),
    updated_at          DATETIME        DEFAULT GETDATE()
);

-- ── 2. Pipeline watermark table (tracks incremental loads) ─────
CREATE TABLE dbo.pipeline_watermark (
    pipeline_name       VARCHAR(100)    NOT NULL PRIMARY KEY,
    last_watermark      DATETIME        NOT NULL,
    last_run_status     VARCHAR(20)     DEFAULT 'PENDING',
    last_run_rows       INT             DEFAULT 0,
    last_run_dttm       DATETIME,
    updated_at          DATETIME        DEFAULT GETDATE()
);

-- ── 3. Pipeline audit log (every run recorded here) ───────────
CREATE TABLE dbo.pipeline_audit (
    audit_id            INT             IDENTITY(1,1) PRIMARY KEY,
    pipeline_name       VARCHAR(100),
    run_id              VARCHAR(100),
    status              VARCHAR(20),
    rows_read           INT             DEFAULT 0,
    rows_written        INT             DEFAULT 0,
    start_dttm          DATETIME,
    end_dttm            DATETIME,
    error_message       VARCHAR(MAX),
    created_at          DATETIME        DEFAULT GETDATE()
);

-- ── 4. Seed watermark table with initial values ───────────────
INSERT INTO dbo.pipeline_watermark (pipeline_name, last_watermark, last_run_status)
VALUES
    ('customers_full_load',     '2000-01-01 00:00:00', 'PENDING'),
    ('cdrs_incremental_load',   '2000-01-01 00:00:00', 'PENDING'),
    ('recharges_incremental',   '2000-01-01 00:00:00', 'PENDING');

-- ── 5. Update watermark stored procedure (called by ADF) ──────
CREATE PROCEDURE dbo.usp_update_watermark
    @pipeline_name  VARCHAR(100),
    @new_watermark  DATETIME,
    @status         VARCHAR(20),
    @rows_written   INT
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE dbo.pipeline_watermark
    SET
        last_watermark  = @new_watermark,
        last_run_status = @status,
        last_run_rows   = @rows_written,
        last_run_dttm   = GETDATE(),
        updated_at      = GETDATE()
    WHERE pipeline_name = @pipeline_name;
END;
GO

-- ── 6. Verify setup ───────────────────────────────────────────
SELECT 'customers'         AS tbl, COUNT(*) AS rows FROM dbo.customers         UNION ALL
SELECT 'pipeline_watermark',        COUNT(*)         FROM dbo.pipeline_watermark UNION ALL
SELECT 'pipeline_audit',            COUNT(*)         FROM dbo.pipeline_audit;
