package com.thompgt.fraud.common.rules;

import com.thompgt.fraud.common.json.Json;
import com.thompgt.fraud.common.model.RuleId;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RuleConfigValidatorTest {

    private static RuleConfig velocity(Map<String, Double> params) {
        return new RuleConfig(RuleId.VELOCITY, 1, true, 30, params, "test", Instant.EPOCH);
    }

    private static Map<String, Double> goodVelocityParams() {
        Map<String, Double> p = new HashMap<>();
        p.put("windowSeconds", 60.0);
        p.put("maxCount", 5.0);
        return p;
    }

    @Test
    void everyBootstrapDefaultIsValid() {
        // Guards the fallback path: if the built-in defaults were invalid, a stack that came up
        // with ZooKeeper unreachable would have nothing to score on.
        assertThatCode(RuleConfigValidator::defaults).doesNotThrowAnyException();
        assertThat(RuleConfigValidator.defaults().keySet())
                .containsExactlyInAnyOrderElementsOf(RuleId.ALL);
    }

    @Test
    void aWellFormedConfigPasses() {
        assertThat(RuleConfigValidator.problems(velocity(goodVelocityParams()))).isEmpty();
    }

    @Test
    void anUnknownRuleIdIsRejected() {
        RuleConfig config = new RuleConfig("nonsense", 1, true, 10, Map.of(), "test", Instant.EPOCH);
        assertThat(RuleConfigValidator.problems(config))
                .singleElement().asString().contains("unknown ruleId");
    }

    @Test
    void aMissingParamIsRejected() {
        Map<String, Double> p = goodVelocityParams();
        p.remove("maxCount");
        assertThat(RuleConfigValidator.problems(velocity(p)))
                .singleElement().asString().contains("missing param maxCount");
    }

    @Test
    void anOutOfRangeThresholdIsRejected() {
        Map<String, Double> p = goodVelocityParams();
        p.put("maxCount", 1.0); // below 2 the rule would fire on every single transaction
        assertThat(RuleConfigValidator.problems(velocity(p)))
                .singleElement().asString().contains("maxCount must be");
    }

    @Test
    void aMistypedParamNameIsRejectedRatherThanIgnored() {
        // The whole point: silently ignoring this would leave the job on its old threshold while
        // the operator believes the change landed.
        Map<String, Double> p = goodVelocityParams();
        p.put("windowSecond", 60.0);
        assertThat(RuleConfigValidator.problems(velocity(p)))
                .singleElement().asString().contains("unknown param windowSecond");
    }

    @Test
    void aFractionalCountIsRejected() {
        Map<String, Double> p = goodVelocityParams();
        p.put("maxCount", 5.5);
        assertThat(RuleConfigValidator.problems(velocity(p)))
                .singleElement().asString().contains("whole number");
    }

    @Test
    void nonFiniteValuesAreRejected() {
        Map<String, Double> p = goodVelocityParams();
        p.put("windowSeconds", Double.NaN);
        assertThat(RuleConfigValidator.problems(velocity(p)))
                .singleElement().asString().contains("finite");
    }

    @Test
    void versionMustStartAtOne() {
        RuleConfig config = new RuleConfig(RuleId.VELOCITY, 0, true, 30, goodVelocityParams(),
                "test", Instant.EPOCH);
        assertThat(RuleConfigValidator.problems(config))
                .singleElement().asString().contains("version must be >= 1");
    }

    @Test
    void everyProblemIsReportedNotJustTheFirst() {
        Map<String, Double> p = new HashMap<>();
        p.put("maxCount", 1.0);
        p.put("bogus", 1.0);
        RuleConfig config = new RuleConfig(RuleId.VELOCITY, 0, true, 500, p, "test", Instant.EPOCH);

        assertThat(RuleConfigValidator.problems(config)).hasSize(5); // version, weight,
        // missing windowSeconds, maxCount range, unknown param
    }

    @Test
    void cardTestingProbeAmountMustBeBelowTheLargeAmount() {
        Map<String, Double> p = new HashMap<>();
        p.put("probeCount", 3.0);
        p.put("probeMaxAmountMinor", 50_000.0);
        p.put("largeAmountMinor", 50_000.0);
        p.put("windowSeconds", 300.0);
        RuleConfig config = new RuleConfig(RuleId.CARD_TESTING, 1, true, 45, p, "test",
                Instant.EPOCH);

        assertThat(RuleConfigValidator.problems(config))
                .singleElement().asString().contains("must be < largeAmountMinor");
    }

    @Test
    void validateThrowsCarryingEveryProblem() {
        Map<String, Double> p = goodVelocityParams();
        p.put("maxCount", 1.0);
        p.put("bogus", 1.0);

        assertThatThrownBy(() -> RuleConfigValidator.validate(velocity(p)))
                .isInstanceOf(RuleConfigException.class)
                .satisfies(e -> assertThat(((RuleConfigException) e).problems()).hasSize(2));
    }

    @Test
    void aConfigParsedFromZnodeBytesValidates() {
        // End to end over the actual contract: what set-rule.sh writes is what the job reads.
        String znodeJson = Json.toJson(RuleConfigValidator.defaults().get(RuleId.GEO_IMPOSSIBLE));
        RuleConfig parsed = RuleConfigValidator.validate(
                Json.fromJson(znodeJson, RuleConfig.class));

        assertThat(parsed.intParam("maxGapSeconds")).isEqualTo(21_600);
        assertThat(parsed.param("maxKmPerHour")).isEqualTo(900.0);
    }

    @Test
    void modelScoreProbThresholdBelowACoinFlipIsRejected() {
        RuleConfig config = new RuleConfig(RuleId.MODEL_SCORE, 1, true, 35,
                Map.of("probThreshold", 0.4), "test", Instant.EPOCH);
        assertThat(RuleConfigValidator.problems(config))
                .singleElement().asString().contains("probThreshold must be");
    }

    @Test
    void supersedesOnlyAcceptsAStrictlyNewerVersionOfTheSameRule() {
        RuleConfig v1 = velocity(goodVelocityParams());
        RuleConfig v2 = v1.withVersion(2);

        assertThat(v2.supersedes(v1)).isTrue();
        assertThat(v1.supersedes(v2)).isFalse();
        assertThat(v1.supersedes(v1)).isFalse();
        // A stale update replayed after a ZooKeeper reconnect must not roll the job back.
        assertThat(v1.supersedes(null)).isTrue();
    }
}
