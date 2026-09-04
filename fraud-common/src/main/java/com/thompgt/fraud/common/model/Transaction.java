package com.thompgt.fraud.common.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;

/**
 * A single card authorisation request as it arrives on the {@code transactions} topic.
 *
 * <p>This is the wire contract between the generator, the Flink job and the API. It is
 * deliberately flat and free of behaviour: anything derived (rolling means, velocity counts)
 * lives in Flink keyed state, not on the event.
 */
public record Transaction(
        @JsonProperty("transactionId") String transactionId,
        @JsonProperty("cardId") String cardId,
        /*
         * Money is an integer count of minor units (cents, pence, yen). Never a double: binary
         * floating point cannot represent 0.10 exactly, and a pipeline that sums thousands of
         * amounts would accumulate visible error. Currency travels alongside because the
         * minor-unit scale differs between currencies (JPY has none).
         */
        @JsonProperty("amountMinor") long amountMinor,
        @JsonProperty("currency") String currency,
        @JsonProperty("merchantId") String merchantId,
        /** ISO 18245 merchant category code, e.g. 5411 grocery, 5967 direct marketing. */
        @JsonProperty("merchantCategory") String merchantCategory,
        /** ISO 3166-1 alpha-2 country of the acquirer. Drives the geo-impossibility rule. */
        @JsonProperty("countryCode") String countryCode,
        /** Stable per-device hash. Null for card-not-present traffic with no device signal. */
        @JsonProperty("deviceFingerprint") String deviceFingerprint,
        /** Authorisation time at the source, not ingest time. Flink watermarks key off this. */
        @JsonProperty("eventTime") Instant eventTime) {

    @JsonCreator
    public Transaction {
        Objects.requireNonNull(transactionId, "transactionId");
        Objects.requireNonNull(cardId, "cardId");
        Objects.requireNonNull(currency, "currency");
        Objects.requireNonNull(merchantId, "merchantId");
        Objects.requireNonNull(countryCode, "countryCode");
        Objects.requireNonNull(eventTime, "eventTime");
        if (amountMinor < 0) {
            // Refunds and reversals are negative, but they arrive as a different message type
            // upstream; one showing up here means a producer bug, so fail loudly rather than
            // scoring a nonsense amount.
            throw new IllegalArgumentException("amountMinor must be >= 0, got " + amountMinor);
        }
    }

    public long eventTimeMillis() {
        return eventTime.toEpochMilli();
    }
}
