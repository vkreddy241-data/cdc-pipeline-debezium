-- ============================================================
-- Snowflake setup for CDC pipeline
-- Run once as ACCOUNTADMIN
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- Warehouse
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE   = 'X-SMALL'
    AUTO_SUSPEND     = 60
    AUTO_RESUME      = TRUE
    COMMENT          = 'CDC pipeline warehouse';

-- Database + schemas
CREATE DATABASE IF NOT EXISTS CLAIMS_DB;
USE DATABASE CLAIMS_DB;

CREATE SCHEMA IF NOT EXISTS CDC_STAGING   COMMENT = 'Raw CDC events from Debezium';
CREATE SCHEMA IF NOT EXISTS CONFORMED     COMMENT = 'Merged/deduplicated tables';
CREATE SCHEMA IF NOT EXISTS ANALYTICS     COMMENT = 'Reporting views and marts';

-- Role and user
CREATE ROLE IF NOT EXISTS DATA_ENGINEER;
GRANT USAGE  ON WAREHOUSE COMPUTE_WH TO ROLE DATA_ENGINEER;
GRANT ALL    ON DATABASE  CLAIMS_DB   TO ROLE DATA_ENGINEER;
GRANT ALL    ON ALL SCHEMAS IN DATABASE CLAIMS_DB TO ROLE DATA_ENGINEER;

-- ============================================================
-- CDC Staging tables (semi-structured — store raw JSON)
-- ============================================================
USE SCHEMA CDC_STAGING;

CREATE TABLE IF NOT EXISTS CLAIMS (
    id           STRING        NOT NULL,
    data         VARIANT,
    cdc_op       STRING,       -- INSERT / UPDATE / DELETE / SNAPSHOT
    captured_at  TIMESTAMP_NTZ,
    updated_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS MEMBERS (
    id           STRING        NOT NULL,
    data         VARIANT,
    cdc_op       STRING,
    captured_at  TIMESTAMP_NTZ,
    updated_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS PROVIDERS (
    id           STRING        NOT NULL,
    data         VARIANT,
    cdc_op       STRING,
    captured_at  TIMESTAMP_NTZ,
    updated_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- Conformed tables (flattened, typed, deduplicated)
-- ============================================================
USE SCHEMA CONFORMED;

CREATE TABLE IF NOT EXISTS CLAIMS (
    claim_id         STRING      NOT NULL PRIMARY KEY,
    member_id        STRING,
    provider_npi     STRING,
    service_date     DATE,
    discharge_date   DATE,
    diagnosis_code   STRING,
    procedure_code   STRING,
    billed_amount    NUMBER(18,2),
    allowed_amount   NUMBER(18,2),
    paid_amount      NUMBER(18,2),
    claim_status     STRING,
    claim_type       STRING,
    plan_id          STRING,
    cdc_op           STRING,
    source_ts        TIMESTAMP_NTZ,
    ingested_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _deleted         BOOLEAN DEFAULT FALSE
)
CLUSTER BY (service_date, claim_type);

-- ============================================================
-- Dynamic Table: auto-refresh conformed from staging
-- ============================================================
CREATE OR REPLACE DYNAMIC TABLE CONFORMED.CLAIMS_DYNAMIC
    TARGET_LAG   = '1 minute'
    WAREHOUSE    = COMPUTE_WH
AS
SELECT
    data:claim_id::STRING        AS claim_id,
    data:member_id::STRING       AS member_id,
    data:provider_npi::STRING    AS provider_npi,
    data:service_date::DATE      AS service_date,
    data:billed_amount::NUMBER   AS billed_amount,
    data:paid_amount::NUMBER     AS paid_amount,
    data:claim_status::STRING    AS claim_status,
    data:claim_type::STRING      AS claim_type,
    cdc_op,
    captured_at,
    (cdc_op = 'DELETE')          AS _deleted
FROM CDC_STAGING.CLAIMS
QUALIFY ROW_NUMBER() OVER (PARTITION BY data:claim_id ORDER BY captured_at DESC) = 1;

-- ============================================================
-- Analytics views
-- ============================================================
USE SCHEMA ANALYTICS;

CREATE OR REPLACE VIEW DAILY_CLAIM_SUMMARY AS
SELECT
    service_date,
    claim_type,
    claim_status,
    COUNT(*)                AS total_claims,
    SUM(billed_amount)      AS total_billed,
    SUM(paid_amount)        AS total_paid,
    AVG(paid_amount)        AS avg_paid,
    SUM(CASE WHEN claim_status = 'DENIED' THEN 1 ELSE 0 END) AS denied_count,
    ROUND(
        SUM(CASE WHEN claim_status = 'DENIED' THEN 1 ELSE 0 END)::FLOAT
        / NULLIF(COUNT(*), 0) * 100, 2
    )                       AS denial_rate_pct
FROM CONFORMED.CLAIMS_DYNAMIC
WHERE _deleted = FALSE
GROUP BY 1, 2, 3
ORDER BY 1 DESC;
