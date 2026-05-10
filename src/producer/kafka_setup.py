"""
Kafka Topic Manager
-------------------
Creates required topics if they do not already exist.
Safe to call multiple times (idempotent).
"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from config.settings import (
    KAFKA_BOOTSTRAP, KAFKA_TOPIC_TXN, KAFKA_TOPIC_ALERTS,
    KAFKA_PARTITIONS, KAFKA_REPLICATION,
)


def ensure_topics(topics: list[str] | None = None) -> None:
    """Create Kafka topics that do not yet exist."""
    if topics is None:
        topics = [KAFKA_TOPIC_TXN, KAFKA_TOPIC_ALERTS]

    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        client_id="banksecure-admin",
        request_timeout_ms=15_000,
    )

    existing = set(admin.list_topics())
    to_create = [
        NewTopic(
            name=t,
            num_partitions=KAFKA_PARTITIONS,
            replication_factor=KAFKA_REPLICATION,
        )
        for t in topics
        if t not in existing
    ]

    if to_create:
        try:
            admin.create_topics(new_topics=to_create, validate_only=False)
            for t in to_create:
                print(f"[kafka] created topic: {t.name}")
        except TopicAlreadyExistsError:
            pass
    else:
        print("[kafka] all topics already exist")

    admin.close()


if __name__ == "__main__":
    ensure_topics()
