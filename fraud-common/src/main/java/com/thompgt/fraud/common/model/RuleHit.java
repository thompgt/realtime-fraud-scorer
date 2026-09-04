package com.thompgt.fraud.common.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

/**
 * One rule firing on one transaction.
 *
 * <p>{@code evidence} carries the numbers the rule actually saw — the observed count against the
 * threshold, the rolling mean, the two country codes. Without it an alert is unreviewable: an
 * analyst can see that a card scored 70 but not why, and the Phase 6 retune has nothing to learn
 * from. Keep the map small and scalar; it is stored as jsonb on the alert row.
 */
public record RuleHit(
        @JsonProperty("ruleId") String ruleId,
        /** Contribution to the total score, taken from the rule's ZooKeeper config at eval time. */
        @JsonProperty("weight") int weight,
        /** The config version that produced this hit, so an alert can be traced to its rules. */
        @JsonProperty("ruleVersion") long ruleVersion,
        @JsonProperty("evidence") Map<String, Object> evidence) {

    @JsonCreator
    public RuleHit {
        Objects.requireNonNull(ruleId, "ruleId");
        evidence = evidence == null ? Map.of() : Map.copyOf(evidence);
    }

    public static RuleHit of(
            String ruleId, int weight, long ruleVersion, Map<String, Object> evidence) {
        return new RuleHit(ruleId, weight, ruleVersion, evidence);
    }
}
