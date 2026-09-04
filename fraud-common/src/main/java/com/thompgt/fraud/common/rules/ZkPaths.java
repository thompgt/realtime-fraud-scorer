package com.thompgt.fraud.common.rules;

/**
 * The ZooKeeper namespace, defined once.
 *
 * <p>Four separate processes read and write this tree; a path typed by hand in any one of them is
 * a silent no-op that looks like "my change did not take effect".
 */
public final class ZkPaths {

    public static final String ROOT = "/fraud";

    /** Parent of one znode per rule. Watched with a Curator TreeCache by the Flink job. */
    public static final String RULES = ROOT + "/rules";

    /** Leader election among generator replicas, so exactly one produces at a time (Phase 7.1). */
    public static final String GENERATOR_LEADER = ROOT + "/leader/generator";

    private ZkPaths() {
    }

    public static String rule(String ruleId) {
        return RULES + "/" + ruleId;
    }
}
