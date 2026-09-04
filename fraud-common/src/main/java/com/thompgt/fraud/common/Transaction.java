package com.thompgt.fraud.common;

import java.time.Instant;

/**
 * A single card authorisation as it arrives on the {@code transactions} topic.
 *
 * <p>Placeholder shape for the scaffold; the field set is settled in Phase 1.
 */
public record Transaction(
        String transactionId,
        String cardId,
        long amountCents,
        String currency,
        String merchantId,
        String merchantCategory,
        String countryCode,
        Instant eventTime) {
}
