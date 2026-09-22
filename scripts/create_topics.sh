#!/usr/bin/env bash
# Create the tickstream Kafka topics. Runs as a one-shot init container.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
TOPIC="${KAFKA_TOPIC:-trades}"
DLQ_TOPIC="${KAFKA_DLQ_TOPIC:-trades_dlq}"
PARTITIONS="${KAFKA_PARTITIONS:-3}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"

create_topic() {
  local name="$1"
  echo "creating topic: ${name}"
  "${KAFKA_BIN}/kafka-topics.sh" \
    --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${name}" \
    --partitions "${PARTITIONS}" \
    --replication-factor 1
}

create_topic "${TOPIC}"
create_topic "${DLQ_TOPIC}"

echo "topics:"
"${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --list
