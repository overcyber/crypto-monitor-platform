#!/usr/bin/env bash
set -euo pipefail
BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
PARTITIONS="${KAFKA_PARTITIONS:-12}"
RAW_MS=$(( ${KAFKA_RAW_RETENTION_HOURS:-24} * 3600000 ))
CLEAN_MS=$(( ${KAFKA_CLEAN_RETENTION_HOURS:-72} * 3600000 ))
create_topic() {
  local topic="$1" retention="$2" partitions="${3:-$PARTITIONS}"
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists --topic "$topic" \
    --partitions "$partitions" --replication-factor 1 \
    --config "retention.ms=$retention" --config cleanup.policy=delete
}
create_topic market.raw "$RAW_MS"
create_topic market.cleaned "$CLEAN_MS"
create_topic market.microstructure "$CLEAN_MS"
create_topic market.replay "$RAW_MS"
create_topic market.replay.microstructure "$RAW_MS"
create_topic market.reconcile.results "$CLEAN_MS" 3
create_topic market.alerts "$CLEAN_MS" 3
create_topic market.deadletter "$CLEAN_MS" 3
create_topic market.control.resync "$CLEAN_MS" 3
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
