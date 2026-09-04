package com.thompgt.fraud.common.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * A scored transaction that crossed the alert threshold.
 *
 * <p>Flattened relative to {@link ScoredTransaction} because this is what lands in Postgres and
 * what the API serves: the fields the query patterns filter on (card, time, rule) sit at the top
 * level so they can be indexed, and only the evidence stays nested as jsonb.
 *
 * <p>There is no separate alert id — the transaction id <em>is</em> the key. That is what lets the
 * JDBC sink upsert, so a Flink restart replaying from the last checkpoint overwrites rows instead
 * of duplicating them.
 */
public record Alert(
        @JsonProperty("transactionId") String transactionId,
        @JsonProperty("cardId") String cardId,
        @JsonProperty("score") int score,
        /** Sorted, so two alerts with the same rules compare and store identically. */
        @JsonProperty("ruleIds") List<String> ruleIds,
        @JsonProperty("hits") List<RuleHit> hits,
        @JsonProperty("amountMinor") long amountMinor,
        @JsonProperty("currency") String currency,
        @JsonProperty("merchantId") String merchantId,
        @JsonProperty("merchantCategory") String merchantCategory,
        @JsonProperty("countryCode") String countryCode,
        @JsonProperty("eventTime") Instant eventTime,
        @JsonProperty("scoredAt") Instant scoredAt) {

    @JsonCreator
    public Alert {
        Objects.requireNonNull(transactionId, "transactionId");
        Objects.requireNonNull(cardId, "cardId");
        ruleIds = ruleIds == null ? List.of() : List.copyOf(ruleIds);
        hits = hits == null ? List.of() : List.copyOf(hits);
    }

    public static Alert from(ScoredTransaction scored) {
        Transaction t = scored.transaction();
        return new Alert(
                t.transactionId(),
                t.cardId(),
                scored.score(),
                scored.hits().stream().map(RuleHit::ruleId).sorted().toList(),
                scored.hits(),
                t.amountMinor(),
                t.currency(),
                t.merchantId(),
                t.merchantCategory(),
                t.countryCode(),
                t.eventTime(),
                scored.scoredAt());
    }
}
