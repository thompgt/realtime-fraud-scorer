package com.thompgt.fraud.flink;

import com.thompgt.fraud.common.model.Transaction;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class ModelScoreTest {

    private static final Instant ORDINARY_HOUR = Instant.parse("2026-01-01T14:00:00Z");
    private static final Instant ODD_HOUR = Instant.parse("2026-01-01T02:30:00Z");

    private static Transaction txn(long amountMinor, String mcc, String device, Instant eventTime) {
        return new Transaction("tx-1", "card-1", amountMinor, "USD", "merchant-1", mcc,
                "US", device, eventTime);
    }

    private static Transaction ordinary() {
        return txn(3_000, "5411", "device-1", ORDINARY_HOUR);
    }

    @Test
    void anOrdinaryDaytimeGroceryPurchaseScoresLow() {
        assertThat(ModelScore.probability(ordinary())).isLessThan(0.5);
    }

    @Test
    void everySingleRiskFeatureAloneStaysBelowTheBootstrapThreshold() {
        // No single weak signal should be able to trip the 0.75 bootstrap cutoff on its own --
        // otherwise the model just duplicates whichever rule already covers that one signal.
        assertThat(ModelScore.probability(txn(3_000, "7995", "device-1", ORDINARY_HOUR)))
                .isLessThan(0.75);
        assertThat(ModelScore.probability(txn(3_000, "5411", null, ORDINARY_HOUR)))
                .isLessThan(0.75);
        assertThat(ModelScore.probability(txn(3_000, "5411", "device-1", ODD_HOUR)))
                .isLessThan(0.75);
        assertThat(ModelScore.probability(txn(150_000, "5411", "device-1", ORDINARY_HOUR)))
                .isLessThan(0.75);
    }

    @Test
    void twoRiskFeaturesTogetherCrossTheBootstrapThreshold() {
        Transaction risky = txn(150_000, "7995", null, ODD_HOUR);
        assertThat(ModelScore.probability(risky)).isGreaterThan(0.75);
    }

    @Test
    void probabilityIsAlwaysAValidRange() {
        assertThat(ModelScore.probability(ordinary())).isBetween(0.0, 1.0);
        assertThat(ModelScore.probability(txn(150_000, "7995", null, ODD_HOUR)))
                .isBetween(0.0, 1.0);
    }
}
