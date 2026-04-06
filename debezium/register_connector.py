"""
Registers the Debezium PostgreSQL source connector via Kafka Connect REST API.
Run once after docker-compose up to start CDC capture.
"""

import json
import time
import logging
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CONNECT_URL   = "http://localhost:8083"
CONNECTOR_NAME = "postgres-cdc-source"

CONNECTOR_CONFIG = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class":                     "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname":                   "postgres",
        "database.port":                       "5432",
        "database.user":                       "debezium",
        "database.password":                   "debezium_password",
        "database.dbname":                     "claims_db",
        "database.server.name":                "pgserver",
        "plugin.name":                         "pgoutput",
        "table.include.list":                  "public.claims,public.members,public.providers",
        "slot.name":                           "debezium_slot",
        "publication.name":                    "debezium_pub",
        "topic.prefix":                        "cdc",
        "topic.creation.default.replication.factor": "1",
        "topic.creation.default.partitions":   "4",
        # Transforms: flatten envelope, add metadata
        "transforms":                          "unwrap,addMeta",
        "transforms.unwrap.type":              "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones":   "false",
        "transforms.unwrap.delete.handling.mode": "rewrite",
        "transforms.addMeta.type":             "org.apache.kafka.connect.transforms.InsertField$Value",
        "transforms.addMeta.static.field":     "pipeline",
        "transforms.addMeta.static.value":     "cdc-debezium",
        # Schema
        "key.converter":                       "org.apache.kafka.connect.json.JsonConverter",
        "value.converter":                     "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable":        "false",
        "value.converter.schemas.enable":      "false",
        # Heartbeat to keep replication slot alive
        "heartbeat.interval.ms":               "30000",
        "heartbeat.topics.prefix":             "__debezium-heartbeat",
    },
}


def wait_for_connect(max_retries: int = 20) -> bool:
    for i in range(max_retries):
        try:
            resp = requests.get(f"{CONNECT_URL}/connectors", timeout=5)
            if resp.status_code == 200:
                logger.info("Kafka Connect is up.")
                return True
        except requests.exceptions.ConnectionError:
            pass
        logger.info(f"Waiting for Kafka Connect... ({i+1}/{max_retries})")
        time.sleep(5)
    return False


def register_connector(config: dict) -> dict:
    name = config["name"]

    # Delete existing connector if present
    existing = requests.get(f"{CONNECT_URL}/connectors/{name}")
    if existing.status_code == 200:
        logger.info(f"Deleting existing connector: {name}")
        requests.delete(f"{CONNECT_URL}/connectors/{name}")
        time.sleep(2)

    resp = requests.post(
        f"{CONNECT_URL}/connectors",
        headers={"Content-Type": "application/json"},
        data=json.dumps(config),
    )
    resp.raise_for_status()
    return resp.json()


def check_connector_status(name: str) -> dict:
    resp = requests.get(f"{CONNECT_URL}/connectors/{name}/status")
    resp.raise_for_status()
    return resp.json()


def main():
    if not wait_for_connect():
        raise RuntimeError("Kafka Connect did not start in time.")

    result = register_connector(CONNECTOR_CONFIG)
    logger.info(f"Registered: {json.dumps(result, indent=2)}")

    time.sleep(5)
    status = check_connector_status(CONNECTOR_NAME)
    logger.info(f"Status: {json.dumps(status, indent=2)}")

    connector_state = status["connector"]["state"]
    if connector_state != "RUNNING":
        raise RuntimeError(f"Connector not running: {connector_state}")

    logger.info("CDC connector is RUNNING. Topics: cdc.public.claims, cdc.public.members, cdc.public.providers")


if __name__ == "__main__":
    main()
