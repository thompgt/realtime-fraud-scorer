package com.thompgt.fraud.flink;

import com.thompgt.fraud.common.model.RuleId;
import com.thompgt.fraud.common.model.Transaction;
import com.thompgt.fraud.common.rules.RuleConfig;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises {@link ModelEnricher#processElement} directly rather than through Flink's operator
 * test harness: the harness verifies output serializability by copying each record with Kryo, and
 * Kryo's {@code FieldSerializer} cannot take a field offset on a record class ({@link KeyedRuleHit}
 * is one) under JDK 17+ -- a pre-existing incompatibility between old Kryo and Java records, not
 * anything specific to this operator. {@code ctx} is unused by {@link ModelEnricher}, so {@code null}
 * is a faithful stand-in.
 */
class ModelEnricherTest {

    private static final Instant ODD_HOUR = Instant.parse("2026-01-01T02:30:00Z");

    private static Transaction risky() {
        return new Transaction("tx-1", "card-1", 150_000, "USD", "merchant-1", "7995",
                "US", null, ODD_HOUR);
    }

    private static Transaction ordinary() {
        return new Transaction("tx-2", "card-1", 3_000, "USD", "merchant-2", "5411",
                "US", "device-1", ODD_HOUR);
    }

    private static RuleConfig config(boolean enabled, double probThreshold) {
        return new RuleConfig(RuleId.MODEL_SCORE, 1, enabled, 35,
                Map.of("probThreshold", probThreshold), "test", Instant.EPOCH);
    }

    private static List<KeyedRuleHit> run(RuleConfig config, Transaction txn) throws Exception {
        List<KeyedRuleHit> collected = new ArrayList<>();
        Collector<KeyedRuleHit> out = new Collector<>() {
            @Override
            public void collect(KeyedRuleHit record) {
                collected.add(record);
            }

            @Override
            public void close() {
            }
        };
        ModelEnricher enricher = new ModelEnricher(config);
        ProcessFunction<Transaction, KeyedRuleHit>.Context noContext = null;
        enricher.processElement(txn, noContext, out);
        return collected;
    }

    @Test
    void aTransactionAboveTheThresholdEmitsAHit() throws Exception {
        List<KeyedRuleHit> hits = run(config(true, 0.75), risky());

        assertThat(hits).hasSize(1);
        assertThat(hits.get(0).transactionId()).isEqualTo("tx-1");
        assertThat(hits.get(0).hit().ruleId()).isEqualTo(RuleId.MODEL_SCORE);
        assertThat(hits.get(0).hit().weight()).isEqualTo(35);
    }

    @Test
    void aTransactionBelowTheThresholdEmitsNothing() throws Exception {
        assertThat(run(config(true, 0.75), ordinary())).isEmpty();
    }

    @Test
    void aDisabledRuleEmitsNothingEvenForARiskyTransaction() throws Exception {
        assertThat(run(config(false, 0.5), risky())).isEmpty();
    }
}
