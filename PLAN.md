# Build plan

Nine phases. Each subphase is meant to be one small commit, and every phase
ends at a state where `docker compose up -d` gives a working, demonstrable
system — nothing is left half-wired between phases.

Legend: `[x]` done · `[ ]` outstanding

---

## Phase 0 — Scaffolding and local infrastructure ✅

- [x] **0.1** Multi-module Maven layout (`fraud-common`, `fraud-generator`,
      `fraud-flink-job`, `fraud-api`) under a parent pom that pins every
      dependency version in one place.
- [x] **0.2** Only-script `mvnw` wrapper so no local Maven install and no
      `maven-wrapper.jar` in the tree; exec bit committed, LF endings pinned
      via `.gitattributes`.
- [x] **0.3** `docker-compose.yml` for ZooKeeper, Kafka (ZooKeeper mode),
      Postgres, Flink (1 JobManager + 2 TaskManagers), Airflow
      (LocalExecutor: init, scheduler, webserver).
- [x] **0.4** Healthchecks and `depends_on: condition: service_healthy`
      throughout, so `up -d` is deterministic rather than a race.
- [x] **0.5** Published ports parameterised through `.env` to coexist with
      other local stacks; Postgres init script creating the Airflow database.
- [x] **0.6** Verified: all services healthy, 4 Flink slots, broker registered
      in ZooKeeper, topic create/list/delete round-trip.
- [x] **0.7** README, this plan, licence, GitHub remote.

**Exit criteria:** `docker compose ps` shows every service healthy; Flink UI
reports 4 available slots; Airflow `/health` reports scheduler + metadatabase
healthy. *(Met.)*

---

## Phase 1 — Domain model, contracts and schema

- [ ] **1.1** Finalise the `Transaction` record: id, card, amount (minor units,
      integer — never a float for money), currency, merchant, MCC, country,
      device fingerprint, event time.
- [ ] **1.2** `ScoredTransaction` / `Alert` records: triggered rule ids, score,
      contributing evidence, scoring timestamp.
- [ ] **1.3** JSON serde in `fraud-common` (Jackson + JSR-310, `Instant` as
      epoch millis) with round-trip tests. One serde used by generator, Flink
      job and API so the wire format cannot drift between them.
- [ ] **1.4** `RuleConfig` contract — the exact JSON stored at each
      `/fraud/rules/<ruleId>` znode, with a `version` field and validation that
      rejects nonsense thresholds rather than letting them reach the job.
- [ ] **1.5** Postgres schema in `sql/schema/`: `alerts`, `daily_card_agg`,
      `rule_versions`, `labelled_outcomes`, with indexes for the API access
      patterns. Applied by a versioned migration, not by hand.
- [ ] **1.6** Kafka topic definitions and a `scripts/create-topics.sh` that is
      idempotent: `transactions`, `scored-transactions`, `alerts`, plus a DLQ.

**Exit criteria:** `./mvnw test` green; topics and tables created reproducibly
from scripts.

---

## Phase 2 — Transaction generator

- [ ] **2.1** Deterministic synthetic population: cards, merchants, home
      countries, per-card spend profile, seeded RNG so runs are reproducible.
- [ ] **2.2** Kafka producer — keyed by `cardId` so a card's history lands on
      one partition and Flink keyed state stays local; idempotent producer,
      `acks=all`.
- [ ] **2.3** Configurable rate limiter, and an **open-loop** load mode (fixed
      arrival schedule, no waiting on the consumer) so measured latency is not
      distorted by coordinated omission.
- [ ] **2.4** Injectable fraud scenarios, on a schedule, so the pipeline has
      something to actually catch: card-testing velocity bursts, geo-impossible
      pairs, and merchant-category anomalies.
- [ ] **2.5** `docker/Dockerfile.generator` — multi-stage
      (`maven:3.9-eclipse-temurin-17` → `eclipse-temurin:17-jre`), added to
      compose with a healthcheck.

**Exit criteria:** `kafka-console-consumer` on `transactions` shows a steady,
well-formed stream including seeded fraud patterns.

---

## Phase 3 — Flink scoring job: core topology

- [ ] **3.1** `KafkaSource` with an event-time watermark strategy and a
      documented bounded-out-of-orderness; deliberate handling for late events
      rather than silently dropping them.
- [ ] **3.2** Key by `cardId`; sliding-window velocity rule ("more than N
      authorisations in 60s") using a `ProcessWindowFunction`.
- [ ] **3.3** Amount-anomaly rule against a per-card rolling mean held in
      keyed state, with TTL so state does not grow without bound.
- [ ] **3.4** Geo-impossibility rule via `KeyedProcessFunction`: two countries
      inside a travel-time-infeasible gap, using state + timers.
- [ ] **3.5** Card-testing pattern via **Flink CEP**: several small
      authorisations followed by a large one within a window.
- [ ] **3.6** Score aggregation combining rule hits into one
      `ScoredTransaction`, with a side output for the DLQ on malformed input.
- [ ] **3.7** Checkpointing tuned (interval, timeout, min-pause) and
      documented; verified to actually restore after a TaskManager kill.

**Exit criteria:** job runs on the cluster with the seeded fraud scenarios
detected and visible in the Flink UI metrics.

---

## Phase 4 — ZooKeeper as the dynamic control plane

This is the phase that makes ZooKeeper do real work rather than just sit
underneath Kafka.

- [ ] **4.1** Curator client bootstrap with a sane retry policy and correct
      lifecycle inside a Flink operator (`open`/`close`, not static state).
- [ ] **4.2** `/fraud/rules/*` znode tree; a `TreeCache`-backed rule store that
      keeps the latest validated config in memory.
- [ ] **4.3** Broadcast the rule updates through the topology using Flink's
      **broadcast state** pattern, so every parallel instance sees a consistent
      rule set instead of each subtask polling ZooKeeper on its own.
- [ ] **4.4** Version and validate on read: reject a malformed or out-of-range
      update, keep serving the last good config, and log loudly.
- [ ] **4.5** `scripts/set-rule.sh` to change a threshold from the shell, and a
      demo showing detection behaviour change within seconds — no redeploy, no
      job restart.
- [ ] **4.6** Handle ZooKeeper session loss: what the job does while
      disconnected (keep last known good) and how it reconciles on reconnect.

**Exit criteria:** changing a threshold in ZooKeeper visibly changes alert
volume on a running job.

---

## Phase 5 — Sinks and the query API

- [ ] **5.1** JDBC sink writing alerts to Postgres, batched, with an upsert
      keyed on transaction id so replays are idempotent.
- [ ] **5.2** Kafka sink for `alerts` (downstream consumers) and
      `scored-transactions` (audit trail).
- [ ] **5.3** Spring Boot read endpoints: alerts by card, by time range, by
      rule; paginated.
- [ ] **5.4** Rule-admin endpoints that write validated configs into ZooKeeper
      via Curator — the supported path for changing thresholds, with the shell
      script kept as the break-glass alternative.
- [ ] **5.5** Actuator health indicators for Kafka, Postgres and ZooKeeper, and
      `docker/Dockerfile.api` wired into compose.

**Exit criteria:** `curl` a card id and get back its alerts; `PUT` a threshold
and watch the Flink job pick it up.

---

## Phase 6 — Airflow batch layer

- [ ] **6.1** Mount `airflow/dags`, add a connections/variables bootstrap so
      the DAGs configure themselves on a fresh stack.
- [ ] **6.2** `daily_aggregates` DAG: roll up the previous day's alerts and
      transactions into `daily_card_agg`, idempotent per logical date so
      backfills are safe.
- [ ] **6.3** `rule_retune` DAG: read labelled outcomes, recompute thresholds
      that hold the false-positive rate to target, and **write the new config
      into ZooKeeper** — closing the batch to streaming loop.
- [ ] **6.4** `flink_job_manage` DAG: submit, savepoint and cancel-with-
      savepoint via the Flink REST API, so redeploys keep state.
- [ ] **6.5** Data-quality checks and a real failure path (retries, alerting on
      DAG failure) rather than tasks that always pass.
- [ ] **6.6** Backfill demonstration over a synthetic week of history.

**Exit criteria:** a nightly retune runs end to end and the Flink job behaviour
changes as a result, with the whole path visible in the Airflow UI.

---

## Phase 7 — Resilience and operations

- [ ] **7.1** Leader election among N generator replicas using Curator's
      `LeaderLatch`, so exactly one produces at a time — plus a documented
      `docker stop` failover demo.
- [ ] **7.2** Flink recovery drill: kill a TaskManager mid-stream, confirm
      restore from checkpoint with no duplicate alerts.
- [ ] **7.3** ZooKeeper failure drill: stop ZooKeeper, confirm the job keeps
      scoring on the last known good rules and recovers cleanly.
- [ ] **7.4** Prometheus + Grafana: Flink metrics, consumer lag, alert rate,
      end-to-end latency **as percentiles from histograms** — never averaged
      percentiles.
- [ ] **7.5** `docs/runbook.md` covering each failure mode and its symptoms.

**Exit criteria:** every drill is a scripted, repeatable command with a
documented expected outcome.

---

## Phase 8 — Testing and CI

- [ ] **8.1** Unit tests for serde, rule evaluation and config validation.
- [ ] **8.2** Flink operator tests with the test harnesses, covering windows,
      timers and late data.
- [ ] **8.3** ZooKeeper integration tests against `curator-test` in-process
      `TestingServer`.
- [ ] **8.4** Testcontainers end-to-end: Kafka + Postgres + ZooKeeper, produce
      a known fraud scenario, assert the alert lands in Postgres.
- [ ] **8.5** GitHub Actions: build, test, and a compose smoke test that boots
      the stack and asserts every service reports healthy.
- [ ] **8.6** Airflow DAG import/validation test so a broken DAG fails CI
      rather than the scheduler.

**Exit criteria:** CI green on a clean clone, with no manual setup steps.

---

## Phase 9 — Documentation and demo

- [ ] **9.1** `docs/architecture.md`: the design, and why each Apache component
      is there rather than a simpler alternative.
- [ ] **9.2** `docs/versions.md`: the pinning rationale (ZooKeeper-mode Kafka,
      Java 17, connector/Flink version pairing) and how to upgrade safely.
- [ ] **9.3** `scripts/demo.sh` — a single scripted walkthrough of the whole
      system.
- [ ] **9.4** Screenshots or a short capture of the Flink and Airflow UIs.
- [ ] **9.5** Honest limitations section: what this is not (no real model, no
      exactly-once to Postgres beyond the idempotent upsert, single broker).

---

## Sequencing notes

- Phases 1–3 are the critical path; 4 depends on 3, and 6 depends on 5.
- Phase 7 leader election (7.1) only needs Phase 2, so it can be pulled
  forward if the ZooKeeper coordination story is the interesting part.
- Phase 8 tests are written alongside each phase, not saved to the end; the
  phase exists to close gaps and add CI.
