package com.thompgt.fraud.flink;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.thompgt.fraud.common.model.RuleHit;

/**
 * A rule hit produced away from the main scoring operator, addressed to the transaction it belongs
 * to.
 *
 * <p>{@code eventTimeMillis} is carried explicitly rather than read from the record's Flink
 * timestamp, because CEP's output timestamps depend on the match strategy and are easy to get
 * subtly wrong; the merge below turns on this value, so it is worth making it a field the
 * originating operator sets on purpose.
 */
record KeyedRuleHit(
        @JsonProperty("transactionId") String transactionId,
        @JsonProperty("eventTimeMillis") long eventTimeMillis,
        @JsonProperty("hit") RuleHit hit) {
}
