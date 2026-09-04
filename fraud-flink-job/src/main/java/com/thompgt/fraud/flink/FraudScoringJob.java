package com.thompgt.fraud.flink;

import com.thompgt.fraud.common.model.RuleId;
import com.thompgt.fraud.common.model.ScoredTransaction;
import com.thompgt.fraud.common.model.Transaction;
import com.thompgt.fraud.common.rules.RuleConfig;
import com.thompgt.fraud.common.rules.RuleConfigValidator;
import com.thompgt.fraud.flink.serde.JsonTypeInfo;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * The scoring topology.
 *
 * <pre>
 *   transactions ──▶ parse ──┬──▶ DLQ (unparseable)
 *                            ├──▶ ModelEnricher (stateless, unkeyed) ──┐
 *                            │                                        │
 *                            ▼  watermarks, keyBy cardId               │
 *                   ┌────────┴────────┐                                │
 *                   │                 │                                │
 *          RuleEvaluator       CardTestingPattern (CEP)                │
 *      velocity, amount, geo        multi-event                       │
 *                   │                 └────────────▶ union ◀──────────┘
 *                   │                                  │
 *                   └──────────────▶ ScoreMerger ◀──────┘   keyBy transactionId
 *                            │
 *                            ▼
 *                     ScoredTransaction ──▶ alerts (score >= threshold)
 * </pre>
 *
 * <p>Phase 3 scores against the static bootstrap thresholds and logs its alerts. Phase 4 replaces
 * the static config with ZooKeeper-driven broadcast state, and Phase 5 replaces the log with the
 * Kafka and Postgres sinks.
 */
public final class FraudScoringJob {

    private static final Logger LOG = LoggerFactory.getLogger(FraudScoringJob.class);

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.from(args);
        LOG.info("Starting fraud scoring job with {}", config);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureCheckpointing(env, config);

        Map<String, RuleConfig> rules = RuleConfigValidator.defaults();

        KafkaSource<byte[]> source = KafkaSource.<byte[]>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(config.transactionsTopic())
                .setGroupId(config.consumerGroup())
                // Earliest, not latest: a job restarted without a savepoint should re-derive its
                // state from the retained history rather than come up with empty per-card state and
                // silently under-score every card for the next hour.
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new ByteArrayDeserializationSchema())
                .build();

        // No watermarks at the source. Per-split watermarking would be better -- it tracks each
        // Kafka partition's own progress -- but it requires knowing each record's event time, which
        // means deserialising inside the source, which is exactly what makes a single malformed
        // record unsurvivable (see ParseTransaction). Watermarks are therefore assigned after the
        // parse, and the DLQ is worth the loss.
        DataStream<byte[]> raw = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "transactions")
                .uid("kafka-source");

        SingleOutputStreamOperator<Transaction> parsed = raw
                .process(new ParseTransaction())
                .returns(JsonTypeInfo.of(Transaction.class))
                .name("parse")
                .uid("parse");

        parsed.getSideOutput(ParseTransaction.DLQ)
                .sinkTo(dlqSink(config))
                .name("dlq")
                .uid("dlq");

        DataStream<KeyedRuleHit> modelHits = parsed
                .process(new ModelEnricher(rules.get(RuleId.MODEL_SCORE)))
                .returns(JsonTypeInfo.of(KeyedRuleHit.class))
                .name("model-enricher")
                .uid("model-enricher");

        KeyedStream<Transaction, String> keyed = parsed
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(
                                        config.maxOutOfOrderness())
                                .withTimestampAssigner((t, ts) -> t.eventTimeMillis())
                                // A quiet subtask must not hold the whole job's watermark back:
                                // with no idleness the first gap in traffic stops every timer and
                                // the job goes silent without failing.
                                .withIdleness(config.idleSourceTimeout()))
                .name("watermarks")
                .uid("watermarks")
                .keyBy(Transaction::cardId);

        DataStream<ScoredTransaction> evaluated = keyed
                .process(new RuleEvaluator(rules))
                .returns(JsonTypeInfo.of(ScoredTransaction.class))
                .name("rule-evaluator")
                .uid("rule-evaluator");

        DataStream<KeyedRuleHit> cardTesting =
                CardTestingPattern.apply(keyed, rules.get(RuleId.CARD_TESTING));

        DataStream<ScoredTransaction> scored = evaluated
                .keyBy(s -> s.transaction().transactionId())
                .connect(cardTesting.union(modelHits).keyBy(KeyedRuleHit::transactionId))
                .process(new ScoreMerger(config.mergeGrace().toMillis()))
                .returns(JsonTypeInfo.of(ScoredTransaction.class))
                .name("score-merger")
                .uid("score-merger");

        // Phase 5 replaces this with the Kafka and Postgres sinks. Until then the alert stream is
        // observable through the operator's counters in the Flink UI and its log output.
        scored.filter(s -> s.alerting(config.alertThreshold()))
                .name("alert-filter")
                .uid("alert-filter")
                .process(new LogAlert())
                .name("alert-log")
                .uid("alert-log");

        env.execute("fraud-scoring");
    }

    private static void configureCheckpointing(StreamExecutionEnvironment env, JobConfig config) {
        env.enableCheckpointing(config.checkpointInterval().toMillis());
        CheckpointConfig cp = env.getCheckpointConfig();

        // Min pause, not just interval: under backpressure a checkpoint can take longer than the
        // interval, and without a pause the job spends all its time checkpointing and none of it
        // processing -- which looks like a throughput collapse with no obvious cause.
        cp.setMinPauseBetweenCheckpoints(config.checkpointInterval().toMillis() / 2);
        cp.setCheckpointTimeout(60_000);
        cp.setMaxConcurrentCheckpoints(1);

        // A checkpoint failure is usually transient (a slow sink, a GC pause). Failing the job on
        // the first one turns a hiccup into a restart, which costs far more than the missed
        // checkpoint; three in a row is a real problem worth failing on.
        cp.setTolerableCheckpointFailureNumber(3);

        // Keep the last checkpoint when the job is cancelled, so a cancel-and-resubmit (which is
        // what the Phase 6 Airflow DAG does on a redeploy) can restore rather than start cold.
        cp.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }

    private static KafkaSink<String> dlqSink(JobConfig config) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(config.dlqTopic())
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                // At-least-once, not exactly-once: the DLQ is a diagnostic channel, and paying for
                // a transactional producer -- plus its transaction timeout coupling to the
                // checkpoint interval -- to avoid a duplicate copy of an already-broken message is
                // not a sensible trade.
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    private FraudScoringJob() {
    }
}
