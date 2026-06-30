"""
consumer.py — consume transactions from Kafka and land them in Bronze Incremental.

Flow:
    Kafka topic `transactions_raw`  ->  [ this consumer ]  ->  data/incremental/bronze/transactions_raw.jsonl

Responsibilities:
  * Subscribe to `transactions_raw`.
  * Deserialize each message from JSON.
  * Log every consumed transaction.
  * Append every event (enriched with Kafka lineage) to the Bronze JSONL file.
  * Route un-parseable / poison messages to a Dead Letter Queue (DLQ) instead of
    crashing the pipeline.
  * Commit the offset ONLY AFTER the event is durably written (manual commit,
    at-least-once). A crash mid-write replays the event rather than losing it.
  * Shut down cleanly on CTRL+C, closing the consumer so the group rebalances.

Lineage columns added to every stored event (mirrors the batch Bronze layer's
`_ingested_at_utc` / `_source_file` convention):
    _kafka_topic, _kafka_partition, _kafka_offset, _consumed_at_utc

Run:
    python consumer.py
"""
import json
import logging
import os
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "transactions_raw")
GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "bronze-incremental-loader")

# Output paths resolve to the PROJECT ROOT regardless of where we launch from
# (consumer.py lives in incremental/, root is one level up).
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_THIS_DIR, ".."))
BRONZE_PATH = os.environ.get(
    "BRONZE_PATH",
    os.path.join(_PROJECT_ROOT, "data", "incremental", "bronze", "transactions_raw.jsonl"),
)
DLQ_PATH = os.environ.get(
    "DLQ_PATH",
    os.path.join(_PROJECT_ROOT, "data", "incremental", "dlq", "transactions_bad.jsonl"),
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | consumer | %(message)s",
)
log = logging.getLogger("consumer")


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _append_jsonl(path: str, record: dict) -> None:
    """Append one JSON object as a line, flushing to disk before we return.

    fsync makes the write durable, so committing the Kafka offset afterwards is
    safe: if we crash before fsync, the offset is uncommitted and the event
    replays. This is the core of the at-least-once guarantee.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def land_to_bronze(msg) -> None:
    """Parse a message and append it to Bronze, or route it to the DLQ."""
    raw_value = msg.value()
    lineage = {
        "_kafka_topic": msg.topic(),
        "_kafka_partition": msg.partition(),
        "_kafka_offset": msg.offset(),
        "_consumed_at_utc": _now_utc_iso(),
    }

    try:
        payload = json.loads(raw_value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        # Poison message -> DLQ. Keep the raw bytes (best-effort decoded) + reason.
        dlq_record = {
            "raw_value": raw_value.decode("utf-8", errors="replace") if raw_value else None,
            "error": f"{type(exc).__name__}: {exc}",
            **lineage,
        }
        _append_jsonl(DLQ_PATH, dlq_record)
        log.warning("Bad message -> DLQ [partition %s @ offset %s]: %s",
                    msg.partition(), msg.offset(), exc)
        return

    # Store the original transaction under `payload`, lineage columns alongside.
    record = {"payload": payload, **lineage}
    _append_jsonl(BRONZE_PATH, record)
    log.info("Consumed transaction_id=%s -> Bronze [partition %s @ offset %s]",
             payload.get("transaction_id"), msg.partition(), msg.offset())


def build_consumer() -> Consumer:
    """Create the confluent-kafka Consumer with MANUAL offset commits."""
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",   # first run reads from the beginning
        "enable.auto.commit": False,       # we commit only after a durable write
    })


def main() -> None:
    log.info("Starting consumer | broker=%s | topic=%s | group=%s",
             KAFKA_BOOTSTRAP, TOPIC, GROUP_ID)
    log.info("Bronze -> %s", BRONZE_PATH)
    log.info("DLQ    -> %s", DLQ_PATH)

    consumer = build_consumer()
    consumer.subscribe([TOPIC])
    consumed = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue                    # no message this tick

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue                # reached end of partition — normal
                log.error("Consumer error: %s", msg.error())
                continue

            # 1) write durably, 2) THEN commit — never the other way around.
            land_to_bronze(msg)
            consumer.commit(message=msg, asynchronous=False)
            consumed += 1

    except KeyboardInterrupt:
        log.info("CTRL+C received — shutting down.")
    finally:
        # close() commits final state and triggers a clean group rebalance.
        consumer.close()
        log.info("Consumer stopped. Total consumed: %d", consumed)


if __name__ == "__main__":
    main()
