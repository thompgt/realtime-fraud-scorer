package com.thompgt.fraud.common.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Every transaction after scoring, alerting or not.
 *
 * <p>The full stream goes to {@code scored-transactions}, not just the alerts, because the Phase 6
 * retune needs the negatives too: you cannot compute a false-positive rate from a topic that only
 * contains positives.
 */
public record ScoredTransaction(
        @JsonProperty("transaction") Transaction transaction,
        /** Sum of the weights of the rules that fired. Zero means clean. */
        @JsonProperty("score") int score,
        @JsonProperty("hits") List<RuleHit> hits,
        /** Wall-clock time the job scored it; subtract event time for end-to-end latency. */
        @JsonProperty("scoredAt") Instant scoredAt) {

    @JsonCreator
    public ScoredTransaction {
        Objects.requireNonNull(transaction, "transaction");
        Objects.requireNonNull(scoredAt, "scoredAt");
        hits = hits == null ? List.of() : List.copyOf(hits);
    }

    public boolean alerting(int alertThreshold) {
        return score >= alertThreshold;
    }
}
