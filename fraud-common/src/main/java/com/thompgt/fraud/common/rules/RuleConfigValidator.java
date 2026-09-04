package com.thompgt.fraud.common.rules;

import com.thompgt.fraud.common.model.RuleId;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Bounds-checks a {@link RuleConfig} before it is allowed to reach the scoring job.
 *
 * <p>ZooKeeper stores bytes; it has no opinion about whether {@code maxCount} is 5 or -1. Without a
 * gate here, a fat-fingered {@code set-rule.sh} would either silence detection entirely or alert on
 * every transaction in the stream, and the first sign of it would be the alert graph. Ranges are
 * therefore deliberately narrow: wide enough for genuine tuning, tight enough that an obviously
 * wrong number cannot get through.
 *
 * <p>Unknown params are rejected rather than ignored, because the usual cause is a typo in a name
 * the job then silently falls back to a default for.
 */
public final class RuleConfigValidator {

    /** An allowed numeric param: inclusive bounds, and whether fractional values make sense. */
    private record ParamSpec(String name, double min, double max, boolean wholeNumber) {
    }

    private static final int MAX_WEIGHT = 100;

    private static final Map<String, List<ParamSpec>> SPECS = specs();

    private static Map<String, List<ParamSpec>> specs() {
        Map<String, List<ParamSpec>> m = new LinkedHashMap<>();

        // Velocity: N authorisations inside a sliding window. Below 2 the rule fires on every
        // single transaction; the window is capped at an hour because anything longer is a batch
        // question, not a streaming one.
        m.put(RuleId.VELOCITY, List.of(
                new ParamSpec("windowSeconds", 5, 3600, true),
                new ParamSpec("maxCount", 2, 1000, true)));

        // Amount anomaly: this amount against the card's own rolling mean. minObservations stops
        // the rule firing on a card's second-ever transaction, when the mean is meaningless.
        m.put(RuleId.AMOUNT_ANOMALY, List.of(
                new ParamSpec("meanMultiplier", 1.5, 50, false),
                new ParamSpec("minObservations", 3, 1000, true),
                new ParamSpec("minAmountMinor", 0, 100_000_000L, true),
                new ParamSpec("stateTtlSeconds", 60, 2_592_000L, true)));

        // Geo-impossibility: implied travel speed between consecutive countries. 1200 km/h is about
        // a subsonic airliner, so the ceiling is set above it and the floor above driving speed.
        m.put(RuleId.GEO_IMPOSSIBLE, List.of(
                new ParamSpec("maxKmPerHour", 100, 2000, false),
                new ParamSpec("maxGapSeconds", 60, 86_400, true)));

        // Card testing: a burst of small probes followed by a large charge. probeMaxAmountMinor
        // must stay well under largeAmountMinor for the pattern to mean anything; that relationship
        // is checked below rather than expressible as a range.
        m.put(RuleId.CARD_TESTING, List.of(
                new ParamSpec("probeCount", 2, 50, true),
                new ParamSpec("probeMaxAmountMinor", 1, 50_000, true),
                new ParamSpec("largeAmountMinor", 1000, 100_000_000L, true),
                new ParamSpec("windowSeconds", 10, 3600, true)));

        return Map.copyOf(m);
    }

    private RuleConfigValidator() {
    }

    /** @return the config unchanged, so calls can be chained onto a parse. */
    public static RuleConfig validate(RuleConfig config) {
        List<String> problems = problems(config);
        if (!problems.isEmpty()) {
            throw new RuleConfigException(config.ruleId(), problems);
        }
        return config;
    }

    /** Non-throwing form, for the API to turn into a 400 with a field-by-field body. */
    public static List<String> problems(RuleConfig config) {
        List<String> problems = new ArrayList<>();

        List<ParamSpec> specs = SPECS.get(config.ruleId());
        if (specs == null) {
            problems.add("unknown ruleId (known: " + new TreeSet<>(RuleId.ALL) + ")");
            return problems;
        }

        if (config.version() < 1) {
            problems.add("version must be >= 1, got " + config.version());
        }
        if (config.weight() < 0 || config.weight() > MAX_WEIGHT) {
            problems.add("weight must be 0.." + MAX_WEIGHT + ", got " + config.weight());
        }

        Set<String> allowed = new TreeSet<>();
        for (ParamSpec spec : specs) {
            allowed.add(spec.name());
            Double value = config.params().get(spec.name());
            if (value == null) {
                problems.add("missing param " + spec.name());
                continue;
            }
            if (value.isNaN() || value.isInfinite()) {
                problems.add(spec.name() + " must be finite, got " + value);
                continue;
            }
            if (value < spec.min() || value > spec.max()) {
                problems.add(spec.name() + " must be " + spec.min() + ".." + spec.max()
                        + ", got " + value);
                continue;
            }
            if (spec.wholeNumber() && value != Math.rint(value)) {
                problems.add(spec.name() + " must be a whole number, got " + value);
            }
        }

        for (String name : new TreeSet<>(config.params().keySet())) {
            if (!allowed.contains(name)) {
                problems.add("unknown param " + name + " (allowed: " + allowed + ")");
            }
        }

        crossFieldChecks(config, problems);
        return problems;
    }

    /** Constraints between two params, which the per-param ranges cannot express. */
    private static void crossFieldChecks(RuleConfig config, List<String> problems) {
        if (RuleId.CARD_TESTING.equals(config.ruleId())) {
            Double probe = config.params().get("probeMaxAmountMinor");
            Double large = config.params().get("largeAmountMinor");
            if (probe != null && large != null && probe >= large) {
                problems.add("probeMaxAmountMinor (" + probe + ") must be < largeAmountMinor ("
                        + large + "), otherwise the pattern cannot match");
            }
        }
    }

    /**
     * The configs a fresh stack starts with, and the fallback the job scores on if ZooKeeper is
     * unreachable at startup. Tuned against the Phase 2 generator so the seeded fraud scenarios
     * actually trip; they are a starting point for the Phase 6 retune, not a recommendation.
     */
    public static Map<String, RuleConfig> defaults() {
        Map<String, RuleConfig> m = new LinkedHashMap<>();
        m.put(RuleId.VELOCITY, new RuleConfig(RuleId.VELOCITY, 1, true, 30,
                Map.of("windowSeconds", 60.0, "maxCount", 5.0), "bootstrap", null));
        m.put(RuleId.AMOUNT_ANOMALY, new RuleConfig(RuleId.AMOUNT_ANOMALY, 1, true, 25,
                Map.of("meanMultiplier", 6.0, "minObservations", 5.0,
                        "minAmountMinor", 5_000.0, "stateTtlSeconds", 604_800.0),
                "bootstrap", null));
        m.put(RuleId.GEO_IMPOSSIBLE, new RuleConfig(RuleId.GEO_IMPOSSIBLE, 1, true, 40,
                Map.of("maxKmPerHour", 900.0, "maxGapSeconds", 21_600.0), "bootstrap", null));
        m.put(RuleId.CARD_TESTING, new RuleConfig(RuleId.CARD_TESTING, 1, true, 45,
                Map.of("probeCount", 3.0, "probeMaxAmountMinor", 500.0,
                        "largeAmountMinor", 50_000.0, "windowSeconds", 300.0),
                "bootstrap", null));
        m.values().forEach(RuleConfigValidator::validate);
        return Map.copyOf(m);
    }
}
