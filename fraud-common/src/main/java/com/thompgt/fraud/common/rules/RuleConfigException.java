package com.thompgt.fraud.common.rules;

import java.util.List;

/**
 * Thrown when a {@link RuleConfig} read from ZooKeeper (or submitted to the admin API) is not
 * usable.
 *
 * <p>Carries <em>every</em> problem found, not just the first. An operator fixing a threshold by
 * hand should see all of what is wrong in one pass instead of rediscovering it one push at a time.
 */
public class RuleConfigException extends RuntimeException {

    private final List<String> problems;

    public RuleConfigException(String ruleId, List<String> problems) {
        super("Invalid rule config for " + ruleId + ": " + String.join("; ", problems));
        this.problems = List.copyOf(problems);
    }

    public List<String> problems() {
        return problems;
    }
}
