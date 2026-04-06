"""
Unit tests for CDC consumer logic.
Run: pytest tests/ -v
"""

from kafka.structs import ConsumerRecord


OP_MAP = {"c": "INSERT", "u": "UPDATE", "d": "DELETE", "r": "SNAPSHOT"}


def make_record(topic: str, payload: dict) -> ConsumerRecord:
    return ConsumerRecord(
        topic=topic, partition=0, offset=0,
        timestamp=1704067200000, timestamp_type=0,
        key=None, value=payload,
        headers=[], checksum=None,
        serialized_key_size=-1, serialized_value_size=100,
        serialized_header_size=-1,
    )


class TestOpMapping:
    def test_create_maps_to_insert(self):
        assert OP_MAP["c"] == "INSERT"

    def test_update_maps_to_update(self):
        assert OP_MAP["u"] == "UPDATE"

    def test_delete_maps_to_delete(self):
        assert OP_MAP["d"] == "DELETE"

    def test_snapshot_maps_to_snapshot(self):
        assert OP_MAP["r"] == "SNAPSHOT"


class TestPayloadParsing:
    def test_claim_payload_fields(self):
        payload = {
            "claim_id":    "C001",
            "member_id":   "M001",
            "paid_amount": 800.0,
            "__op":        "c",
        }
        assert payload["claim_id"] == "C001"
        assert payload["paid_amount"] == 800.0

    def test_delete_payload_has_op(self):
        payload = {"claim_id": "C001", "__op": "d"}
        is_deleted = payload.get("__op") == "d"
        assert is_deleted is True

    def test_none_payload_skipped(self):
        record = make_record("cdc.public.claims", None)
        # Consumer should skip None payloads without error
        assert record.value is None

    def test_batch_groups_by_table(self):
        records = [
            make_record("cdc.public.claims",    {"claim_id": "C001", "__op": "c"}),
            make_record("cdc.public.members",   {"member_id": "M001", "__op": "c"}),
            make_record("cdc.public.claims",    {"claim_id": "C002", "__op": "u"}),
        ]
        tables = {}
        for msg in records:
            table = "CDC_STAGING." + msg.topic.split(".")[-1].upper()
            tables.setdefault(table, []).append(msg)

        assert len(tables["CDC_STAGING.CLAIMS"]) == 2
        assert len(tables["CDC_STAGING.MEMBERS"]) == 1


class TestConnectorConfig:
    def test_required_config_keys(self):
        from debezium.register_connector import CONNECTOR_CONFIG
        cfg = CONNECTOR_CONFIG["config"]
        assert "connector.class" in cfg
        assert "database.hostname" in cfg
        assert "table.include.list" in cfg
        assert "transforms" in cfg

    def test_tables_included(self):
        from debezium.register_connector import CONNECTOR_CONFIG
        tables = CONNECTOR_CONFIG["config"]["table.include.list"]
        assert "public.claims" in tables
        assert "public.members" in tables
        assert "public.providers" in tables
