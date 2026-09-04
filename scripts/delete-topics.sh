#!/usr/bin/env bash
# Delete this pipeline's Kafka topics, for resetting local state.
#
# Destructive and unrecoverable, so it refuses to do anything without --yes.
# Re-create with ./scripts/create-topics.sh; consumer group offsets for the
# deleted topics go with them.
#
#   ./scripts/delete-topics.sh --yes

set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-fraud-kafka}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"

TOPICS=(transactions scored-transactions alerts transactions-dlq)

if [[ "${1:-}" != "--yes" ]]; then
  echo "Refusing to delete without --yes."
  echo "This drops all data in: ${TOPICS[*]}"
  echo "  ./scripts/delete-topics.sh --yes"
  exit 1
fi

for name in "${TOPICS[@]}"; do
  # See create-topics.sh for why MSYS_NO_PATHCONV is needed under Git Bash.
  # --if-exists keeps a partial reset (or a second run) from failing.
  echo "  - $name"
  MSYS_NO_PATHCONV=1 docker exec "$KAFKA_CONTAINER" kafka-topics \
    --bootstrap-server "$BOOTSTRAP" --delete --if-exists --topic "$name"
done

echo
echo "Remaining topics:"
MSYS_NO_PATHCONV=1 docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server "$BOOTSTRAP" --list
