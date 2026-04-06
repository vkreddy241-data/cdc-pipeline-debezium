-- ============================================================
-- Merge CDC staging → conformed CLAIMS table
-- Run every 5 minutes via Snowflake Task or Airflow
-- ============================================================

MERGE INTO CONFORMED.CLAIMS AS tgt
USING (
    SELECT
        data:claim_id::STRING      AS claim_id,
        data:member_id::STRING     AS member_id,
        data:provider_npi::STRING  AS provider_npi,
        data:service_date::DATE    AS service_date,
        data:discharge_date::DATE  AS discharge_date,
        data:diagnosis_code::STRING  AS diagnosis_code,
        data:procedure_code::STRING  AS procedure_code,
        data:billed_amount::NUMBER(18,2) AS billed_amount,
        data:allowed_amount::NUMBER(18,2) AS allowed_amount,
        data:paid_amount::NUMBER(18,2)    AS paid_amount,
        data:claim_status::STRING  AS claim_status,
        data:claim_type::STRING    AS claim_type,
        data:plan_id::STRING       AS plan_id,
        cdc_op,
        captured_at                AS source_ts,
        (cdc_op = 'DELETE')        AS _deleted
    FROM CDC_STAGING.CLAIMS
    QUALIFY ROW_NUMBER() OVER (PARTITION BY data:claim_id ORDER BY captured_at DESC) = 1
) AS src ON tgt.claim_id = src.claim_id

WHEN MATCHED AND src._deleted = TRUE
    THEN UPDATE SET tgt._deleted = TRUE, tgt.ingested_at = CURRENT_TIMESTAMP()

WHEN MATCHED
    THEN UPDATE SET
        tgt.member_id      = src.member_id,
        tgt.provider_npi   = src.provider_npi,
        tgt.service_date   = src.service_date,
        tgt.discharge_date = src.discharge_date,
        tgt.diagnosis_code = src.diagnosis_code,
        tgt.procedure_code = src.procedure_code,
        tgt.billed_amount  = src.billed_amount,
        tgt.allowed_amount = src.allowed_amount,
        tgt.paid_amount    = src.paid_amount,
        tgt.claim_status   = src.claim_status,
        tgt.claim_type     = src.claim_type,
        tgt.plan_id        = src.plan_id,
        tgt.cdc_op         = src.cdc_op,
        tgt.source_ts      = src.source_ts,
        tgt.ingested_at    = CURRENT_TIMESTAMP()

WHEN NOT MATCHED AND src._deleted = FALSE
    THEN INSERT (
        claim_id, member_id, provider_npi, service_date, discharge_date,
        diagnosis_code, procedure_code, billed_amount, allowed_amount, paid_amount,
        claim_status, claim_type, plan_id, cdc_op, source_ts, _deleted
    ) VALUES (
        src.claim_id, src.member_id, src.provider_npi, src.service_date, src.discharge_date,
        src.diagnosis_code, src.procedure_code, src.billed_amount, src.allowed_amount, src.paid_amount,
        src.claim_status, src.claim_type, src.plan_id, src.cdc_op, src.source_ts, FALSE
    );
