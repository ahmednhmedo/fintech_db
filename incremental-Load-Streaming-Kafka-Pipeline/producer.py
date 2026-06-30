"""
producer.py — pull transactions from the source API and publish them to Kafka.

Flow:
    FastAPI  ->  [ this producer ]  ->  Kafka topic `transactions_raw`

Responsibilities (one event per second):
  * Fetch ONE transaction from the FastAPI endpoint over HTTP.
  * Serialize it as JSON.
  * Publish it to the `transactions_raw` topic, keyed by transaction_id so all
    events for the same transaction land on the same partition (ordering).
  * Confirm delivery via an asynchronous delivery callback.
  * Survive transient failures (API down / Kafka down) without crashing.
  * Log every published event.
  * Stop cleanly on CTRL+C, flushing any in-flight messages first.

Run:
    python producer.py
"""
import json
import logging
import os
import time

import requests
from confluent_kafka import Producer

# -----------------------------------------------------------------------------
# Configuration (env-overridable so the same code runs locally and in a container)
# -----------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "transactions_raw")
SOURCE_API_URL = os.environ.get("SOURCE_API_URL", "http://localhost:8000/transactions/next")
PUBLISH_INTERVAL_SEC = float(os.environ.get("PUBLISH_INTERVAL_SEC", "1.0"))
HTTP_TIMEOUT_SEC = float(os.environ.get("HTTP_TIMEOUT_SEC", "5.0"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | producer | %(message)s",
)
log = logging.getLogger("producer")


def delivery_report(err, msg) -> None:
    """Async callback fired by Kafka once a message is acknowledged (or fails)."""
    if err is not None:
        log.error("Delivery FAILED: %s", err)
    else:
        log.info(
            "Delivered -> %s [partition %s @ offset %s]",
            msg.topic(), msg.partition(), msg.offset(),
        )


def fetch_next_transaction() -> dict | None:
    """Pull one transaction from the source API. Returns None on a transient error."""
    try:
        resp = requests.get(SOURCE_API_URL, timeout=HTTP_TIMEOUT_SEC)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        log.warning("Source API not reachable (%s) — will retry.", exc)
        return None


def build_producer() -> Producer:
    """Create the confluent-kafka Producer."""
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "fintech-transaction-producer",
        # at-least-once friendly defaults; broker acks from all in-sync replicas.
        "acks": "all",
        "enable.idempotence": True,
    })


def main() -> None:
    log.info("Starting producer | api=%s | broker=%s | topic=%s",
             SOURCE_API_URL, KAFKA_BOOTSTRAP, TOPIC)
    producer = build_producer()
    published = 0

    try:
        while True:
            transaction = fetch_next_transaction()
            if transaction is None:
                time.sleep(PUBLISH_INTERVAL_SEC)
                continue

            key = str(transaction.get("transaction_id", "")) or None
            payload = json.dumps(transaction).encode("utf-8")

            try:
                producer.produce(
                    topic=TOPIC,
                    key=key.encode("utf-8") if key else None,
                    value=payload,
                    callback=delivery_report,
                )
            except BufferError:
                # Local queue full: let librdkafka drain, then retry this event.
                log.warning("Local producer queue full — flushing before retry.")
                producer.flush()
                continue

            published += 1
            log.info("Published #%d transaction_id=%s", published, key)

            # poll() serves delivery callbacks without blocking the 1/sec cadence.
            producer.poll(0)
            time.sleep(PUBLISH_INTERVAL_SEC)

    except KeyboardInterrupt:
        log.info("CTRL+C received — shutting down.")
    finally:
        # Block until every queued message is delivered (or its callback fires).
        remaining = producer.flush(timeout=10)
        if remaining:
            log.warning("%d message(s) still undelivered at shutdown.", remaining)
        log.info("Producer stopped. Total published: %d", published)


if __name__ == "__main__":
    main()
