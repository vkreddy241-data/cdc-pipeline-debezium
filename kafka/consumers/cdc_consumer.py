"""
Kafka consumer: reads CDC events from Debezium topics and routes them
to a Snowflake staging table via the Snowflake Kafka Connector (or direct insert).

Topics consumed:
  cdc.public.claims
  cdc.public.members
  cdc.public.providers
"""

import json
import os
import logging
from datetime import datetime, timezone
from kafka import KafkaConsumer
import snowflake.connector

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPICS          = ["cdc.public.claims", "cdc.public.members", "cdc.public.providers"]
GROUP_ID        = "cdc-snowflake-sink"

SF_CONFIG = {
    "account":   os.getenv("SF_ACCOUNT"),
    "user":      os.getenv("SF_USER"),
    "password":  os.getenv("SF_PASSWORD"),
    "warehouse": os.getenv("SF_WAREHOUSE", "COMPUTE_WH"),
    "database":  os.getenv("SF_DATABASE",  "CLAIMS_DB"),
    "schema":    os.getenv("SF_SCHEMA",    "CDC_STAGING"),
    "role":      os.getenv("SF_ROLE",      "DATA_ENGINEER"),
}

# Map Debezium operation codes to human-readable ops
OP_MAP = {"c": "INSERT", "u": "UPDATE", "d": "DELETE", "r": "SNAPSHOT"}

BATCH_SIZE = 500


def get_snowflake_conn():
    return snowflake.connector.connect(**SF_CONFIG)


def build_upsert_sql(table: str) -> str:
    return f"""
        MERGE INTO {table} AS tgt
        USING (SELECT PARSE_JSON(%s) AS src_data) AS src
        ON tgt.id = src.src_data:id::STRING
        WHEN MATCHED AND src.src_data:__op::STRING = 'DELETE' THEN DELETE
        WHEN MATCHED THEN UPDATE SET
            tgt.data        = src.src_data,
            tgt.updated_at  = CURRENT_TIMESTAMP(),
            tgt.cdc_op      = src.src_data:__op::STRING
        WHEN NOT MATCHED AND src.src_data:__op::STRING != 'DELETE' THEN INSERT
            (id, data, cdc_op, captured_at)
        VALUES (
            src.src_data:id::STRING,
            src.src_data,
            src.src_data:__op::STRING,
            src.src_data:__source_ts_ms::TIMESTAMP_NTZ
        )
    """


def process_batch(batch: list, sf_conn):
    tables = {}
    for msg in batch:
        topic = msg.topic
        table = "CDC_STAGING." + topic.split(".")[-1].upper()
        tables.setdefault(table, []).append(msg)

    cursor = sf_conn.cursor()
    for table, messages in tables.items():
        sql = build_upsert_sql(table)
        for msg in messages:
            try:
                payload = msg.value
                if payload is None:
                    continue
                payload["__op"] = OP_MAP.get(payload.get("__op", "c"), "INSERT")
                payload["__consumed_at"] = datetime.now(timezone.utc).isoformat()
                cursor.execute(sql, (json.dumps(payload),))
            except Exception as e:
                logger.error(f"Failed to upsert to {table}: {e} | payload: {msg.value}")
    cursor.close()
    logger.info(f"Batch of {len(batch)} records committed to Snowflake.")


def main():
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
    )

    sf_conn = get_snowflake_conn()
    logger.info(f"CDC consumer started. Listening to: {TOPICS}")

    batch = []
    try:
        for msg in consumer:
            batch.append(msg)
            if len(batch) >= BATCH_SIZE:
                process_batch(batch, sf_conn)
                consumer.commit()
                batch = []
    except KeyboardInterrupt:
        if batch:
            process_batch(batch, sf_conn)
            consumer.commit()
    finally:
        consumer.close()
        sf_conn.close()


if __name__ == "__main__":
    main()
