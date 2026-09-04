package com.thompgt.fraud.flink;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;
import java.time.Duration;

/**
 * Every knob the job takes, resolved once on the client and shipped to the operators.
 *
 * <p>Values come from {@code --flag value} arguments first and then the {@code FRAUD_*} environment
 * variables that compose already sets, so the same jar runs unchanged from the Flink UI, from
 * {@code flink run}, and from the Airflow REST submission in Phase 6.4.
 *
 * <p>Deliberately <em>not</em> where rule thresholds live: those come from ZooKeeper at runtime
 * (Phase 4) precisely so that changing one does not mean a redeploy.
 */
public record JobConfig(
        String bootstrapServers,
        String transactionsTopic,
        String dlqTopic,
        String consumerGroup,
        int alertThreshold,
        Duration maxOutOfOrderness,
        Duration idleSourceTimeout,
        Duration mergeGrace,
        Duration checkpointInterval) implements Serializable {

    public static JobConfig from(String[] args) {
        ParameterTool p = ParameterTool.fromArgs(args);
        return new JobConfig(
                get(p, "bootstrap-servers", "FRAUD_KAFKA_BOOTSTRAP", "kafka:29092"),
                get(p, "transactions-topic", "FRAUD_TRANSACTIONS_TOPIC", "transactions"),
                get(p, "dlq-topic", "FRAUD_DLQ_TOPIC", "transactions-dlq"),
                get(p, "consumer-group", "FRAUD_CONSUMER_GROUP", "fraud-scorer"),
                Integer.parseInt(get(p, "alert-threshold", "FRAUD_ALERT_THRESHOLD", "40")),
                // 5s of slack for out-of-order arrivals. Wide enough to absorb producer and broker
                // jitter, narrow enough that windows still close promptly; anything later is
                // handled explicitly as late data rather than by widening this further.
                Duration.ofSeconds(Long.parseLong(
                        get(p, "max-out-of-orderness-seconds", "FRAUD_MAX_OOO_SECONDS", "5"))),
                // Without this, one idle Kafka partition holds the whole job's watermark back and
                // no window ever closes -- the single most common "my Flink job emits nothing"
                // cause, and a real risk here since the generator keys by card.
                Duration.ofSeconds(Long.parseLong(
                        get(p, "idle-source-seconds", "FRAUD_IDLE_SOURCE_SECONDS", "30"))),
                Duration.ofSeconds(Long.parseLong(
                        get(p, "merge-grace-seconds", "FRAUD_MERGE_GRACE_SECONDS", "1"))),
                Duration.ofSeconds(Long.parseLong(
                        get(p, "checkpoint-interval-seconds", "FRAUD_CHECKPOINT_SECONDS", "10"))));
    }

    private static String get(ParameterTool p, String flag, String envVar, String fallback) {
        if (p.has(flag)) {
            return p.get(flag);
        }
        String env = System.getenv(envVar);
        return env == null || env.isBlank() ? fallback : env;
    }
}
