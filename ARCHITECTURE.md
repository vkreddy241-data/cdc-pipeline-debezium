# Architecture — CDC Pipeline (Debezium → Snowflake)

## Overview

A Change Data Capture pipeline that streams every INSERT, UPDATE, and DELETE from a PostgreSQL `claims_db` into Snowflake with sub-minute latency, using Debezium for log-based CDC and a custom Kafka consumer for Snowflake ingestion.

```
PostgreSQL (claims_db)
  tables: public.claims, public.members, public.providers
  WAL replication slot: debezium_slot
  publication: debezium_pub
        │  pgoutput (logical replication)
        ▼
Debezium PostgreSQL Connector (Kafka Connect)
  - Captures INSERT (op=c), UPDATE (op=u), DELETE (op=d), SNAPSHOT (op=r)
  - Applies SMTs: ExtractNewRecordState (flatten envelope) + InsertField (add pipeline metadata)
  - Heartbeat every 30s to keep replication slot alive
        │
        ▼
Kafka Topics (JSON, no schema registry)
  cdc.public.claims
  cdc.public.members
  cdc.public.providers
        │
        ▼
CDC Kafka Consumer (cdc_consumer.py)
  - Consumes all three topics in one consumer group (cdc-snowflake-sink)
  - Batches 500 messages before flushing (reduces Snowflake round-trips)
  - Routes each message to its CDC_STAGING table by topic name
  - MERGE INTO Snowflake staging: INSERT new, UPDATE existing, soft-DELETE on op=d
        │
        ▼
Snowflake — CDC_STAGING schema
  CDC_STAGING.CLAIMS   (raw VARIANT JSON + cdc_op + captured_at)
  CDC_STAGING.MEMBERS
  CDC_STAGING.PROVIDERS
        │  Snowflake Task (every 5 min) or Airflow DAG
        ▼
Snowflake — CONFORMED schema
  merge_conformed.sql: MERGE staging → typed conformed tables
  - Deduplicates: QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY captured_at DESC) = 1
  - Handles soft deletes: sets _deleted=TRUE instead of physical delete
  - Extracts typed columns from VARIANT (claim_id, amounts, dates, codes)
```

## Key Design Decisions

**Why log-based CDC (Debezium) over query-based polling?**  
Query polling (SELECT WHERE updated_at > last_run) misses hard deletes and requires an updated_at column on every table. Debezium reads from the PostgreSQL WAL, capturing every operation at < 1s latency without adding load to the source database.

**Why store raw JSON (VARIANT) in CDC_STAGING before promoting to typed columns?**  
Staging raw VARIANT decouples schema changes in the source from breaking the Snowflake pipeline. When PostgreSQL adds a column, it lands automatically in the VARIANT blob and can be promoted to a typed column in the next conformed model update — without reprocessing history.

**Why soft-delete in the conformed layer instead of physical DELETE?**  
Downstream BI reports and actuarial models need to audit what was deleted and when. Setting `_deleted = TRUE` preserves the record while hiding it from normal queries via a simple `WHERE _deleted = FALSE` filter.

**Why batch size of 500 in the Kafka consumer?**  
Each Snowflake MERGE is a round-trip. Batching 500 records cuts the number of API calls ~500x vs. row-by-row, staying well within Snowflake's recommended micro-batch ingestion patterns while keeping per-batch latency under 10 seconds.

**Why `QUALIFY ROW_NUMBER()` dedup in the merge SQL instead of deduping in the consumer?**  
The consumer could receive out-of-order messages if partitions lag differently. Deduplicating in SQL at merge time, using `ORDER BY captured_at DESC`, guarantees the conformed layer always reflects the latest state regardless of consumer ordering.

## Latency Profile

| Stage | Typical latency |
|---|---|
| PostgreSQL commit → Kafka | < 1 second |
| Kafka → CDC_STAGING (Snowflake) | < 10 seconds (batch of 500) |
| CDC_STAGING → CONFORMED | < 5 minutes (Snowflake Task interval) |
| **End-to-end** | **< 6 minutes** |

## Tech Stack

| Component | Technology |
|---|---|
| Source | PostgreSQL 15 (logical replication / pgoutput) |
| CDC connector | Debezium 2.x via Kafka Connect |
| Message bus | Apache Kafka |
| Consumer | Python kafka-python + snowflake-connector-python |
| Sink | Snowflake (VARIANT staging + typed conformed) |
| Orchestration | Snowflake Tasks / Apache Airflow |
| Infrastructure | Docker Compose (local), Terraform (cloud) |

## Local Setup

```bash
docker-compose -f docker/docker-compose.yml up -d   # Postgres, Kafka, Zookeeper, Connect
python debezium/register_connector.py               # register CDC connector (run once)
python kafka/consumers/cdc_consumer.py              # start consuming
```
