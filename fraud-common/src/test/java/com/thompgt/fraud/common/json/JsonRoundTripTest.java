package com.thompgt.fraud.common.json;

import com.thompgt.fraud.common.model.Alert;
import com.thompgt.fraud.common.model.RuleHit;
import com.thompgt.fraud.common.model.RuleId;
import com.thompgt.fraud.common.model.ScoredTransaction;
import com.thompgt.fraud.common.model.Transaction;
import com.thompgt.fraud.common.rules.RuleConfig;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The wire format is a contract between three modules, so these tests assert the encoding itself,
 * not only that a value survives a round trip. A round trip alone would still pass if both sides
 * agreed on a format the other services do not speak.
 */
class JsonRoundTripTest {

    private static final Instant EVENT_TIME = Instant.ofEpochMilli(1_767_225_600_123L);

    private static Transaction transaction() {
        return new Transaction("txn-1", "card-42", 12_345L, "USD", "merch-9", "5411", "US",
                "dev-abc", EVENT_TIME);
    }

    @Test
    void transactionSurvivesRoundTrip() {
        Transaction original = transaction();
        assertThat(Json.fromJson(Json.toJson(original), Transaction.class)).isEqualTo(original);
    }

    @Test
    void instantsAreEncodedAsEpochMillis() {
        // Not ISO-8601 and not seconds-with-nanos. Every consumer reads these as a long.
        assertThat(Json.toJson(transaction())).contains("\"eventTime\":1767225600123");
    }

    @Test
    void amountIsEncodedAsAnIntegerNotAFloat() {
        assertThat(Json.toJson(transaction()))
                .contains("\"amountMinor\":12345")
                .doesNotContain("12345.0");
    }

    @Test
    void nullFieldsAreOmitted() {
        Transaction noDevice = new Transaction("txn-2", "card-42", 100L, "USD", "merch-9", "5411",
                "US", null, EVENT_TIME);
        assertThat(Json.toJson(noDevice)).doesNotContain("deviceFingerprint");
    }

    @Test
    void unknownFieldsAreTolerated() {
        // A newer producer adding a field must not break an older consumer.
        String json = Json.toJson(transaction()).replaceFirst("\\{", "{\"futureField\":\"x\",");
        assertThat(Json.fromJson(json, Transaction.class).transactionId()).isEqualTo("txn-1");
    }

    @Test
    void scoredTransactionSurvivesRoundTrip() {
        ScoredTransaction original = new ScoredTransaction(
                transaction(), 70,
                List.of(RuleHit.of(RuleId.VELOCITY, 30, 3L, Map.of("count", 9, "threshold", 5)),
                        RuleHit.of(RuleId.GEO_IMPOSSIBLE, 40, 1L, Map.of("from", "US", "to", "SG"))),
                Instant.ofEpochMilli(1_767_225_600_500L));

        ScoredTransaction parsed =
                Json.fromJson(Json.toJson(original), ScoredTransaction.class);

        assertThat(parsed.score()).isEqualTo(70);
        assertThat(parsed.transaction()).isEqualTo(original.transaction());
        assertThat(parsed.hits()).extracting(RuleHit::ruleId)
                .containsExactly(RuleId.VELOCITY, RuleId.GEO_IMPOSSIBLE);
        assertThat(parsed.hits().get(0).ruleVersion()).isEqualTo(3L);
    }

    @Test
    void alertFlattensAScoredTransactionAndSortsRuleIds() {
        ScoredTransaction scored = new ScoredTransaction(
                transaction(), 70,
                List.of(RuleHit.of(RuleId.VELOCITY, 30, 1L, Map.of()),
                        RuleHit.of(RuleId.AMOUNT_ANOMALY, 40, 1L, Map.of())),
                Instant.ofEpochMilli(1_767_225_600_500L));

        Alert alert = Alert.from(scored);

        assertThat(alert.transactionId()).isEqualTo("txn-1");
        assertThat(alert.cardId()).isEqualTo("card-42");
        assertThat(alert.amountMinor()).isEqualTo(12_345L);
        assertThat(alert.ruleIds()).containsExactly(RuleId.AMOUNT_ANOMALY, RuleId.VELOCITY);
        assertThat(Json.fromJson(Json.toJson(alert), Alert.class)).isEqualTo(alert);
    }

    @Test
    void ruleConfigSurvivesRoundTrip() {
        RuleConfig original = new RuleConfig(RuleId.VELOCITY, 7, true, 30,
                Map.of("windowSeconds", 60.0, "maxCount", 5.0), "airflow", EVENT_TIME);
        assertThat(Json.fromJson(Json.toJson(original), RuleConfig.class)).isEqualTo(original);
    }

    @Test
    void malformedInputRaisesACodecExceptionRatherThanEscapingAsIoException() {
        // The Flink job catches exactly this to route a record to the DLQ.
        assertThatThrownBy(() -> Json.fromJson("{not json", Transaction.class))
                .isInstanceOf(JsonCodecException.class);
    }

    @Test
    void aTransactionMissingARequiredFieldIsRejected() {
        assertThatThrownBy(() -> Json.fromJson("{\"transactionId\":\"t\"}", Transaction.class))
                .isInstanceOf(JsonCodecException.class);
    }

    @Test
    void negativeAmountsAreRejectedAtTheBoundary() {
        assertThatThrownBy(() -> new Transaction("t", "c", -1L, "USD", "m", "5411", "US", null,
                EVENT_TIME))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("amountMinor");
    }
}
