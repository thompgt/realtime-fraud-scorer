package com.thompgt.fraud.common.rules;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * The exact JSON stored at {@code /fraud/rules/<ruleId>} in ZooKeeper.
 *
 * <p>This is a published contract with three writers — the admin API, the Airflow retune DAG and
 * the break-glass shell script — and one reader, the Flink job. Changing a field name breaks a
 * running cluster, so it changes only with a migration of the znode contents.
 *
 * <p>Thresholds live in a generic {@code params} map rather than as typed fields per rule. That is
 * a deliberate trade: it means one record covers all four rules and adding a fifth needs no schema
 * change, at the cost of losing compile-time checking — which {@link RuleConfigValidator} buys back
 * at read time, where it matters, because the value came from outside the process anyway.
 */
public record RuleConfig(
        @JsonProperty("ruleId") String ruleId,
        /**
         * Monotonically increasing. The job uses it to ignore a stale update arriving out of order
         * after a ZooKeeper reconnect, and it is stamped onto every {@code RuleHit} so an alert can
         * be traced back to the exact config that produced it.
         */
        @JsonProperty("version") long version,
        /** A disabled rule is still distributed, so turning it back on needs no re-push. */
        @JsonProperty("enabled") boolean enabled,
        /** Points this rule contributes to the score when it fires. */
        @JsonProperty("weight") int weight,
        /** Rule-specific thresholds. Doubles throughout; counts are validated as whole numbers. */
        @JsonProperty("params") Map<String, Double> params,
        @JsonProperty("updatedBy") String updatedBy,
        @JsonProperty("updatedAt") Instant updatedAt) {

    @JsonCreator
    public RuleConfig {
        Objects.requireNonNull(ruleId, "ruleId");
        params = params == null ? Map.of() : Map.copyOf(params);
    }

    public double param(String name) {
        Double v = params.get(name);
        if (v == null) {
            // Unreachable for a validated config; a loud failure here means something bypassed
            // the validator rather than that an operator typed a bad number.
            throw new IllegalStateException("Rule " + ruleId + " has no param " + name);
        }
        return v;
    }

    public int intParam(String name) {
        return (int) param(name);
    }

    public long longParam(String name) {
        return (long) param(name);
    }

    /** True if this config should replace {@code other} — same rule, strictly newer version. */
    public boolean supersedes(RuleConfig other) {
        return other == null || (ruleId.equals(other.ruleId) && version > other.version);
    }

    public RuleConfig withVersion(long newVersion) {
        return new RuleConfig(ruleId, newVersion, enabled, weight, params, updatedBy, updatedAt);
    }
}
