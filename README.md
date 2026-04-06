# CDC Pipeline with Debezium, Kafka & Snowflake

![CI](https://github.com/vkreddy241-data/cdc-pipeline-debezium/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Debezium](https://img.shields.io/badge/Debezium-2.5-FF0000)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.5-231F20?logo=apachekafka)
![Snowflake](https://img.shields.io/badge/Snowflake-Dynamic%20Tables-29B5E8?logo=snowflake)
![Terraform](https://img.shields.io/badge/Terraform-1.5-7B42BC?logo=terraform)
![Docker](https://img.shields.io/badge/Docker%20Compose-ready-2496ED?logo=docker)

Real-time **Change Data Capture** pipeline that streams row-level changes from PostgreSQL into Snowflake with sub-minute latency — using Debezium, Kafka, and Snowflake Dynamic Tables.

---

## Architecture

```
PostgreSQL (WAL / logical replication)
        │
        ▼
Debezium PostgreSQL Connector
(Kafka Connect — ECS Fargate on AWS)
        │
        ▼  Topics: cdc.public.claims
        │          cdc.public.members
        │          cdc.public.providers
        ▼
  Apache Kafka (AWS MSK)
        │
        ├──► Python CDC Consumer → Snowflake CDC_STAGING (VARIANT JSON)
        │
        └──► Snowflake Dynamic Table (auto-refresh, 1-min lag)
                    │
                    ▼
             CONFORMED.CLAIMS  (MERGE — INSERT / UPDATE / DELETE)
                    │
                    ▼
             ANALYTICS views + dashboards
```

## Key Features

| Feature | Detail |
|---|---|
| **Latency** | Sub-1-minute end-to-end (Snowflake Dynamic Table lag = 1 min) |
| **Operations** | INSERT, UPDATE, DELETE, SNAPSHOT all handled |
| **Deduplication** | `ROW_NUMBER()` over `captured_at` per `claim_id` |
| **Soft deletes** | `_deleted` flag — no hard deletes in conformed layer |
| **Local dev** | Full Docker Compose stack (Postgres + Kafka + Debezium + UI) |
| **Cloud** | RDS PostgreSQL + MSK + ECS Fargate + Snowflake |
| **IaC** | Terraform for RDS, MSK, ECS task definition |

## Project Structure

```
cdc-pipeline-debezium/
├── debezium/
│   └── register_connector.py    # Registers Debezium connector via REST API
├── kafka/consumers/
│   └── cdc_consumer.py          # Kafka → Snowflake batch upsert consumer
├── snowflake/
│   ├── setup.sql                # Warehouse, schemas, tables, Dynamic Table
│   └── merge_conformed.sql      # MERGE staging → conformed
├── docker/
│   ├── docker-compose.yml       # Full local CDC stack
│   └── init.sql                 # PostgreSQL seed data
├── infra/terraform/             # RDS + MSK + ECS Fargate
├── tests/
│   └── test_cdc_consumer.py     # pytest unit tests
└── .github/workflows/ci.yml
```

## Quick Start (Local)

### 1. Start the full CDC stack
```bash
cd docker
docker-compose up -d
# Kafka UI:         http://localhost:8080
# Kafka Connect:    http://localhost:8083
# PostgreSQL:       localhost:5432
```

### 2. Register the Debezium connector
```bash
pip install -r requirements.txt
python debezium/register_connector.py
# → Topics created: cdc.public.claims, cdc.public.members, cdc.public.providers
```

### 3. Trigger a CDC event
```bash
docker exec -it postgres psql -U debezium -d claims_db -c \
  "UPDATE claims SET claim_status='APPROVED' WHERE claim_id='C002';"
```

### 4. Watch it in Kafka UI
Open http://localhost:8080 → Topics → `cdc.public.claims`

### 5. Run tests
```bash
pytest tests/ -v
```

## Deploy to AWS

```bash
cd infra/terraform
terraform init
terraform apply \
  -var="vpc_id=vpc-xxx" \
  -var='private_subnet_ids=["subnet-a","subnet-b","subnet-c"]' \
  -var="db_password=<secret>"
```

Then run Snowflake setup:
```sql
-- Connect to Snowflake and run:
\i snowflake/setup.sql
```

## Tech Stack

**CDC:** Debezium 2.5 (PostgreSQL connector, pgoutput plugin)
**Messaging:** Apache Kafka 3.5 (AWS MSK)
**Sink:** Snowflake (Dynamic Tables, MERGE, VARIANT)
**Runtime:** Docker Compose (local) / ECS Fargate (AWS)
**IaC:** Terraform 1.5
**CI/CD:** GitHub Actions

---
Built by [Vikas Reddy Amaravathi](https://linkedin.com/in/vikas-reddy-a-avr03) — Azure Data Engineer @ Cigna
