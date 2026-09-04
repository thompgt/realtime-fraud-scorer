-- V001 — initial fraud schema.
--
-- Money is stored exclusively as integer minor units (bigint). Floating point
-- and numeric are both avoided: the former cannot represent decimal cents
-- exactly, and the latter invites arithmetic that silently changes scale.
--
-- No BEGIN/COMMIT here: scripts/apply-schema.sh wraps each file in one
-- transaction together with its schema_migrations bookkeeping row.

-- ---------------------------------------------------------------------------
-- alerts
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS alerts (
    transaction_id     text        PRIMARY KEY,
    card_id            text        NOT NULL,
    score              integer     NOT NULL,
    rule_ids           text[]      NOT NULL DEFAULT '{}',
    evidence           jsonb       NOT NULL DEFAULT '{}'::jsonb,
    amount_minor       bigint      NOT NULL,
    currency           char(3)     NOT NULL,
    merchant_id        text        NOT NULL,
    merchant_category  text,
    country_code       char(2),
    event_time         timestamptz NOT NULL,
    scored_at          timestamptz NOT NULL,
    inserted_at        timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE alerts IS
    'One row per alerting transaction. The transaction id is the primary key so '
    'the Flink JDBC sink can upsert: a replay after a checkpoint restore rewrites '
    'the same row instead of duplicating the alert.';
COMMENT ON COLUMN alerts.score IS
    'Aggregate score across triggered rules; integer because thresholds are '
    'compared exactly and a float would make boundary behaviour non-reproducible.';
COMMENT ON COLUMN alerts.rule_ids IS
    'Rule ids that fired. An array rather than a join table because the set is '
    'small, always read whole, and only ever queried for membership.';
COMMENT ON COLUMN alerts.evidence IS
    'Per-rule supporting facts (window counts, prior country, rolling mean). '
    'Schemaless on purpose — each rule contributes its own shape.';
COMMENT ON COLUMN alerts.amount_minor IS
    'Amount in minor units of `currency` (e.g. cents). Never a decimal type.';
COMMENT ON COLUMN alerts.event_time IS
    'Authorisation time from the payload — the event-time Flink windows on.';
COMMENT ON COLUMN alerts.scored_at IS
    'When the Flink job produced the score. Differs from event_time by the '
    'pipeline lag, and the gap is what the latency dashboards measure.';
COMMENT ON COLUMN alerts.inserted_at IS
    'Wall-clock insert time. On an upserted replay this stays at the original '
    'value, which is what makes a replay detectable after the fact.';

-- Serves Phase 5.3 "alerts by card", which is always ordered newest-first and
-- paginated; the trailing event_time column keeps the sort out of the plan.
CREATE INDEX IF NOT EXISTS alerts_card_id_event_time_idx
    ON alerts (card_id, event_time DESC);

-- Serves Phase 5.3 "alerts by time range" — the unfiltered recent-alerts feed
-- and any range scan not narrowed by card.
CREATE INDEX IF NOT EXISTS alerts_event_time_idx
    ON alerts (event_time DESC);

-- Serves Phase 5.3 "alerts by rule": `rule_ids @> ARRAY[...]`. GIN because the
-- query is array containment, which btree cannot answer.
CREATE INDEX IF NOT EXISTS alerts_rule_ids_gin_idx
    ON alerts USING gin (rule_ids);

-- ---------------------------------------------------------------------------
-- daily_card_agg
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS daily_card_agg (
    agg_date             date        NOT NULL,
    card_id              text        NOT NULL,
    txn_count            bigint      NOT NULL DEFAULT 0,
    alert_count          bigint      NOT NULL DEFAULT 0,
    total_amount_minor   bigint      NOT NULL DEFAULT 0,
    distinct_merchants   integer     NOT NULL DEFAULT 0,
    distinct_countries   integer     NOT NULL DEFAULT 0,
    max_score            integer,
    computed_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (agg_date, card_id)
);

COMMENT ON TABLE daily_card_agg IS
    'Per-card daily rollup maintained by the Airflow `daily_aggregates` DAG. '
    'The (agg_date, card_id) primary key is what makes a re-run for a logical '
    'date idempotent: the DAG can delete-then-insert the date partition, or '
    'ON CONFLICT DO UPDATE, and a backfill is safe either way.';
COMMENT ON COLUMN daily_card_agg.agg_date IS
    'Airflow logical date the row was computed for, not the run date — a '
    'backfill must land on the day it describes.';
COMMENT ON COLUMN daily_card_agg.max_score IS
    'Null when the card had transactions but no scored alert that day.';
COMMENT ON COLUMN daily_card_agg.computed_at IS
    'Last recompute time; the only way to tell a backfilled row from the '
    'original nightly one.';

-- No secondary indexes: every read of this table is either by primary key or a
-- full scan of one agg_date, both of which the PK already covers.

-- ---------------------------------------------------------------------------
-- rule_versions
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS rule_versions (
    id          bigserial   PRIMARY KEY,
    rule_id     text        NOT NULL,
    version     bigint      NOT NULL,
    enabled     boolean     NOT NULL DEFAULT true,
    config      jsonb       NOT NULL,
    updated_by  text        NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now(),
    source      text        NOT NULL,
    CONSTRAINT rule_versions_rule_id_version_key UNIQUE (rule_id, version),
    CONSTRAINT rule_versions_source_check
        CHECK (source IN ('api', 'airflow', 'script'))
);

COMMENT ON TABLE rule_versions IS
    'Append-only audit trail of every rule config pushed to ZooKeeper. '
    'ZooKeeper keeps only the current znode contents, so without this table '
    'there is no way to answer "what were the thresholds last Tuesday".';
COMMENT ON COLUMN rule_versions.version IS
    'Version field from the RuleConfig payload, unique per rule. Two writers '
    'racing on the same version is a bug, and the unique constraint surfaces it '
    'here rather than leaving it invisible in ZooKeeper.';
COMMENT ON COLUMN rule_versions.config IS
    'The exact JSON written to /fraud/rules/<rule_id>, byte-for-byte, so the '
    'znode can be reconstructed from the audit trail.';
COMMENT ON COLUMN rule_versions.source IS
    'Which of the three supported write paths made the change: the admin API, '
    'the nightly retune DAG, or the break-glass shell script.';

-- ---------------------------------------------------------------------------
-- labelled_outcomes
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS labelled_outcomes (
    transaction_id  text        PRIMARY KEY,
    label           text        NOT NULL,
    labelled_at     timestamptz NOT NULL DEFAULT now(),
    labelled_by     text        NOT NULL,
    notes           text,
    CONSTRAINT labelled_outcomes_label_check
        CHECK (label IN ('fraud', 'legit', 'unknown'))
);

COMMENT ON TABLE labelled_outcomes IS
    'Ground truth consumed by the Phase 6 `rule_retune` DAG. Deliberately not a '
    'foreign key onto alerts: a true negative — a transaction that never '
    'alerted — is exactly what the false-positive rate needs to be measured '
    'against.';
COMMENT ON COLUMN labelled_outcomes.label IS
    'The unknown label is a real state, not a placeholder: the retune must '
    'exclude unadjudicated transactions rather than counting them as legitimate.';
