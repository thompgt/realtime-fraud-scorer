# realtime-fraud-scorer

A real-time payments fraud scorer built to exercise **Apache ZooKeeper, Apache
Kafka, Apache Flink and Apache Airflow** together, all running on Docker.

Synthetic card transactions flow into Kafka. A Flink (Java) job scores them
against velocity and geo-impossibility rules held in **ZooKeeper**, so a
threshold change propagates to every TaskManager without a redeploy. Scored
alerts land in Postgres, where a Spring Boot API serves them. **Airflow** runs
the batch half: it re-derives thresholds from the previous day's labelled
outcomes, writes them back into ZooKeeper, and maintains daily aggregates.

```
                                  ┌──────────────┐
                                  │  ZooKeeper   │  /fraud/rules/*  (thresholds)
                                  └──┬────────┬──┘  /fraud/leader   (generator election)
                          watches    │        │    Kafka metadata
                        ┌────────────┘        └──────────────┐
                        │                                    │
┌───────────┐     ┌─────┴─────┐     ┌────────────────┐   ┌───┴────┐
│ generator ├────►│   Kafka   ├────►│  Flink job     ├──►│Postgres│◄── fraud-api
└───────────┘     └───────────┘     │ windows + CEP  │   └───┬────┘      (REST)
                                    └────────────────┘       │
                                                     ┌───────┴────────┐
                                                     │    Airflow     │
                                                     │ nightly retune ├──► writes
                                                     └────────────────┘     to ZK
```

## Status

Phase 0 (scaffolding and local infrastructure) is complete and verified. See
[PLAN.md](PLAN.md) for the full phase breakdown and where work stands.

## Quick start

Requirements: Docker Desktop and a JDK 17+. Maven is **not** required — the
repo ships an only-script `mvnw` wrapper.

```bash
cp .env.example .env          # optional: only if the default ports clash
docker compose up -d
docker compose ps             # every service should reach (healthy)
```

| Service | URL / address | Notes |
|---|---|---|
| Flink UI | http://localhost:8081 | 2 TaskManagers, 4 slots |
| Airflow UI | http://localhost:8088 | login `airflow` / `airflow` |
| Kafka | `localhost:9092` | from the host; `kafka:29092` in-network |
| ZooKeeper | `localhost:2181` | |
| Postgres | `localhost:5434` | db `fraud`, user/pass `fraud` |

Tear down with `docker compose down` (add `-v` to drop the volumes too).

Build the JVM modules:

```bash
./mvnw clean package
```

## Repository layout

| Path | Purpose |
|---|---|
| `fraud-common/` | Shared domain model, JSON serde, ZooKeeper rule-config contract |
| `fraud-generator/` | Synthetic transaction generator publishing to Kafka |
| `fraud-flink-job/` | Flink DataStream scoring job (shaded job jar) |
| `fraud-api/` | Spring Boot query and rule-admin API |
| `airflow/dags/` | Airflow DAGs for the batch layer |
| `sql/` | Schema and Postgres init scripts |
| `docker/` | Application Dockerfiles |
| `docs/` | Design notes, version pinning rationale, runbook |

## Local gotchas

- **Published ports are configurable.** Several services default to ports that
  commonly clash with other local stacks, so every published port reads from an
  optional `.env` (see `.env.example`). Postgres defaults to host **5434**.
- **Kafka has two listeners.** Containers must use `kafka:29092`; the host must
  use `localhost:9092`. Mixing them up produces a connection that appears to
  succeed and then times out.
- **Git Bash mangles ZooKeeper paths.** `zookeeper-shell ... ls /brokers/ids`
  fails with `Path must start with / character` because MSYS rewrites the
  leading slash into a Windows path. Prefix the command with
  `MSYS_NO_PATHCONV=1`.
- **Kafka runs in ZooKeeper mode deliberately**, not KRaft — ZooKeeper is one of
  the technologies this project is meant to exercise. That pins the broker to
  Confluent Platform 7.6.x (Kafka 3.6), since Kafka 4.0 removes ZooKeeper mode.
- **The build targets Java 17**, matching the `flink:1.20.1-...-java17` runtime
  image, even though a newer JDK may be installed locally.

## Licence

MIT — see [LICENSE](LICENSE).
