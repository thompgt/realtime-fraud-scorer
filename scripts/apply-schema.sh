#!/usr/bin/env bash
#
# Applies sql/schema/V*.sql into the running `postgres` compose service, in
# lexical order, exactly once each. Re-running is a no-op.
#
# Applied files are recorded in schema_migrations with a checksum, so editing a
# file that has already run is treated as an error rather than being silently
# ignored — an edited migration means the deployed schema and the tree have
# diverged, and the fix is a new V-file, not a rewrite of an old one.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCHEMA_DIR="$REPO_ROOT/sql/schema"

DB_NAME="${FRAUD_DB_NAME:-fraud}"
DB_USER="${FRAUD_DB_USER:-fraud}"
SERVICE="${FRAUD_PG_SERVICE:-postgres}"

# MSYS_NO_PATHCONV stops Git Bash rewriting the leading slash of anything that
# looks like an absolute path (psql flags, SQL text) into a Windows path on its
# way into the container. See the "Local gotchas" section of the README. It is
# also why the compose file is reached by cd rather than -f "$REPO_ROOT/...":
# that host-side path would be mangled by the very same setting.
psql_q() {
  ( cd "$REPO_ROOT" && MSYS_NO_PATHCONV=1 docker compose exec -T "$SERVICE" psql -v ON_ERROR_STOP=1 -qtAX -U "$DB_USER" -d "$DB_NAME" "$@" )
}

psql_q -c "
SET client_min_messages = warning;
CREATE TABLE IF NOT EXISTS schema_migrations (
    filename   text        PRIMARY KEY,
    checksum   text        NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
);" >/dev/null

shopt -s nullglob
files=("$SCHEMA_DIR"/V*.sql)
shopt -u nullglob
if [ ${#files[@]} -eq 0 ]; then
  echo "no migrations found in $SCHEMA_DIR" >&2
  exit 1
fi

applied=0
for path in "${files[@]}"; do
  name="$(basename "$path")"
  sum="$(sha256sum "$path" | cut -d' ' -f1)"
  recorded="$(psql_q -c "SELECT checksum FROM schema_migrations WHERE filename = '$name';")"

  if [ -n "$recorded" ]; then
    if [ "$recorded" != "$sum" ]; then
      echo "ERROR: $name was already applied but its checksum changed." >&2
      echo "  applied: $recorded" >&2
      echo "  on disk: $sum" >&2
      echo "Add a new migration instead of editing an applied one." >&2
      exit 1
    fi
    echo "skip    $name (already applied)"
    continue
  fi

  echo "apply   $name"
  # The migration and its bookkeeping row go in one transaction, so a failure
  # halfway through cannot leave the file recorded as applied.
  {
    echo "BEGIN;"
    cat "$path"
    echo "INSERT INTO schema_migrations (filename, checksum) VALUES ('$name', '$sum');"
    echo "COMMIT;"
  } | psql_q >/dev/null
  applied=$((applied + 1))
done

echo "done: $applied applied, $(( ${#files[@]} - applied )) skipped"
