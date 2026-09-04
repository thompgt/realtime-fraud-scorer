package com.thompgt.fraud.common.model;

import java.util.Set;

/**
 * The rule identifiers, in one place.
 *
 * <p>These strings are load-bearing in three separate systems — they are the znode names under
 * {@code /fraud/rules}, the values stored in {@code alerts.rule_ids}, and the keys the API filters
 * on — so they live here as constants rather than as magic strings scattered across modules.
 * Renaming one is a breaking change to stored data, not a refactor.
 */
public final class RuleId {

    /** Too many authorisations on one card inside a short sliding window. */
    public static final String VELOCITY = "velocity";

    /** Amount far above the card's own rolling mean. */
    public static final String AMOUNT_ANOMALY = "amount_anomaly";

    /** Two countries inside a travel-time-infeasible gap. */
    public static final String GEO_IMPOSSIBLE = "geo_impossible";

    /** Several small probing authorisations followed by a large one. */
    public static final String CARD_TESTING = "card_testing";

    public static final Set<String> ALL =
            Set.of(VELOCITY, AMOUNT_ANOMALY, GEO_IMPOSSIBLE, CARD_TESTING);

    private RuleId() {
    }
}
