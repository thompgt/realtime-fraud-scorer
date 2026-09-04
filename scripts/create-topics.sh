#!/usr/bin/env bash
# Create the Kafka topics this pipeline needs.
#
# KAFKA_AUTO_CREATE_TOPICS_ENABLE is "false" in docker-compose.yml on purpose:
# auto-created topics silently take the broker defaults (1 partition), which
# would cap Flink's keyed parallelism at 1 and be invisible until throughput
# mattered. Every topic therefore comes from this script.
#
# Safe to re-run. Existing topics are reported rather than treated as an error,
# and are grown to the partition count below if they are smaller.
#
#   ./scripts/create-topics.sh
#   KAFKA_CONTAINER=fraud-kafka ./scripts/create-topics.sh

set -euo pipefail

# kafka-topics runs *inside* the broker container, so it must use the internal
# listener. localhost:9092 is the host-side listener and is wrong from in here.
KAFKA_CONTAINER="${KAFKA_CONTAINER:-fraud-kafka}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"

# Single-broker local stack. Anything real would run RF=3 across brokers.
REPLICATION_FACTOR="${KAFKA_REPLICATION_FACTOR:-1}"

# min.insync.replicas is deliberately left unset (broker default 1). Setting it
# above the replication factor makes every acks=all produce fail with
# NOT_ENOUGH_REPLICAS -- the classic single-broker footgun.

# name:partitions:retention_ms
TOPICS=(
  # Main ingest topic. The Flink job keys by cardId, so partition count is the
  # ceiling on useful parallelism -- 6 leaves headroom over the 4 task slots.
  # 7 days of retention is enough to replay a bad job run from the start.
  "transactions:6:604800000"

  # Audit trail of everything the scorer emitted. Mirrors the ingest partition
  # count so a scored record stays co-partitioned with its input.
  "scored-transactions:6:604800000"

  # Alerts are a small fraction of the stream, so fewer partitions; kept 30 days
  # because the batch layer retunes thresholds against recent alert history.
  "alerts:3:2592000000"

  # Malformed-input side output from Flink. Ordering across the whole DLQ is
  # more useful than throughput here, and volume should be ~zero, so 1
  # partition. Kept 30 days -- a poison message may not be noticed for weeks.
  "transactions-dlq:1:2592000000"
)

kt() {
  # MSYS_NO_PATHCONV: Git Bash rewrites arguments that look like absolute paths
  # (--config values, topic names) into C:/Program Files/Git/... before docker
  # exec ever sees them. See the README gotcha.
  MSYS_NO_PATHCONV=1 docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server "$BOOTSTRAP" "$@"
}

echo "Creating topics on ${KAFKA_CONTAINER} (${BOOTSTRAP})"

existing="$(kt --list)"

for spec in "${TOPICS[@]}"; do
  name="${spec%%:*}"
  rest="${spec#*:}"
  partitions="${rest%%:*}"
  retention="${rest#*:}"

  if grep -qx "$name" <<<"$existing"; then
    current="$(kt --describe --topic "$name" | awk '/PartitionCount/ {for (i=1;i<NF;i++) if ($i=="PartitionCount:") print $(i+1)}' | head -1)"
    if [[ -z "$current" ]]; then
      current="$(kt --describe --topic "$name" | grep -c $'\tPartition: ')"
    fi

    if (( current < partitions )); then
      # Kafka can only grow a topic's partition count, never shrink it --
      # shrinking would strand the keys already hashed to the removed
      # partitions. Growing also reshuffles future keys, so it is a deliberate
      # act, not something to do casually on a live topic.
      echo "  ~ $name exists with $current partitions, growing to $partitions"
      kt --alter --topic "$name" --partitions "$partitions"
    else
      echo "  = $name exists with $current partitions (script wants $partitions), leaving alone"
    fi
    continue
  fi

  echo "  + $name (partitions=$partitions rf=$REPLICATION_FACTOR retention=${retention}ms)"
  kt --create \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor "$REPLICATION_FACTOR" \
    --config "retention.ms=$retention"
done

echo
echo "Topics:"
kt --list

for spec in "${TOPICS[@]}"; do
  echo
  kt --describe --topic "${spec%%:*}"
done
