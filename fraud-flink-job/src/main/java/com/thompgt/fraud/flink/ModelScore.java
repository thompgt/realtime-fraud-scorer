package com.thompgt.fraud.flink;

import com.thompgt.fraud.common.model.Transaction;

import java.time.ZoneOffset;
import java.util.Set;

/**
 * A fixed-coefficient logistic model over features available on a single transaction, standing in
 * for a trained risk model until one exists.
 *
 * <p><b>Why fixed coefficients instead of a trained artifact.</b> There is no labelled training
 * set yet — {@code labelled_outcomes} only gets populated once real alerts are reviewed — so a
 * hand-picked logistic model is what makes the enricher slot exist and be wired into the topology
 * now. Swapping this class for one that loads a PMML/ONNX artifact and calls into a runtime is a
 * drop-in replacement later: {@link ModelEnricher} only depends on {@link #probability(Transaction)}
 * returning a 0..1 score, not on how it is computed.
 *
 * <p><b>Why these four features.</b> Each is a single-transaction signal the per-card stateful
 * rules in {@link RuleEvaluator} cannot see, either because they need no history (merchant
 * category, hour of day, missing device) or because they deliberately do not duplicate a rule that
 * already covers the same ground with per-card state (the rolling-mean anomaly). A card-not-present
 * transaction with no device fingerprint, at an odd UTC hour, at a merchant category outside the
 * small set of everyday categories, is exactly the shape a synthetic-identity or stolen-card test
 * transaction takes.
 */
final class ModelScore {

    /**
     * Merchant categories the generator's legitimate population never shops in (see
     * {@code Population.EVERYDAY_MCCS} in fraud-generator). Not an exhaustive real-world high-risk
     * MCC list, since one does not exist here yet — treated as "unusual for this pipeline's
     * traffic" rather than "objectively risky".
     */
    private static final Set<String> UNUSUAL_MCCS =
            Set.of("5967", "6051", "7995", "5933", "4829");

    private static final long LARGE_AMOUNT_MINOR = 100_000L; // 1,000.00 in a 2-decimal currency

    // Logistic coefficients, chosen so that no single feature alone crosses the 0.75 bootstrap
    // threshold but two together do -- a model that fires on one weak signal would just duplicate
    // whichever single-feature rule already covers it.
    private static final double INTERCEPT = -3.0;
    private static final double WEIGHT_UNUSUAL_MCC = 1.6;
    private static final double WEIGHT_NO_DEVICE = 1.4;
    private static final double WEIGHT_ODD_HOUR = 1.2;
    private static final double WEIGHT_LARGE_AMOUNT = 1.3;

    private ModelScore() {
    }

    /** @return a probability in [0, 1] that this transaction is fraudulent. */
    static double probability(Transaction txn) {
        double logit = INTERCEPT
                + (UNUSUAL_MCCS.contains(txn.merchantCategory()) ? WEIGHT_UNUSUAL_MCC : 0.0)
                + (txn.deviceFingerprint() == null ? WEIGHT_NO_DEVICE : 0.0)
                + (isOddHour(txn) ? WEIGHT_ODD_HOUR : 0.0)
                + (txn.amountMinor() >= LARGE_AMOUNT_MINOR ? WEIGHT_LARGE_AMOUNT : 0.0);

        return 1.0 / (1.0 + Math.exp(-logit));
    }

    /** 1am-4am UTC: no timezone travels with the event, so this is a coarse proxy at best. */
    private static boolean isOddHour(Transaction txn) {
        int hour = txn.eventTime().atZone(ZoneOffset.UTC).getHour();
        return hour >= 1 && hour <= 4;
    }
}
