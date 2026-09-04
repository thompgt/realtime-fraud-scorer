package com.thompgt.fraud.flink;

import com.thompgt.fraud.common.model.RuleHit;
import com.thompgt.fraud.common.model.RuleId;
import com.thompgt.fraud.common.model.Transaction;
import com.thompgt.fraud.common.rules.RuleConfig;
import com.thompgt.fraud.flink.serde.JsonTypeInfo;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Card testing, expressed as a Flink CEP pattern: a run of small probing authorisations on one card
 * followed by a large one, all inside a window.
 *
 * <p><b>Why CEP here and not in {@link RuleEvaluator}.</b> The other three rules are aggregate
 * questions — a count, a mean, a last-seen location — that a bit of keyed state answers directly.
 * This one is a question about a <em>sequence</em>, and hand-rolling it means tracking a partial
 * match per card, expiring it on a timer, and resetting it correctly when a non-matching
 * transaction interrupts the run. CEP already does that, including the within-window expiry that is
 * the part most often got wrong by hand.
 *
 * <p>The pattern is deliberately non-strict ({@code followedBy}): a fraudster's probes are
 * interleaved with the cardholder's own legitimate traffic, so requiring strict contiguity would
 * miss nearly every real instance.
 */
final class CardTestingPattern {

    private CardTestingPattern() {
    }

    static DataStream<KeyedRuleHit> apply(KeyedStream<Transaction, String> keyed, RuleConfig config) {
        long probeMax = config.longParam("probeMaxAmountMinor");
        long largeMin = config.longParam("largeAmountMinor");
        int probeCount = config.intParam("probeCount");
        Duration window = Duration.ofSeconds(config.longParam("windowSeconds"));

        Pattern<Transaction, ?> pattern = Pattern.<Transaction>begin("probes")
                .where(SimpleCondition.of(t -> t.amountMinor() <= probeMax))
                // times(n) matches n or more; the run is what matters, not an exact count.
                .times(probeCount)
                .followedBy("large")
                .where(SimpleCondition.of(t -> t.amountMinor() >= largeMin))
                .within(window);

        return CEP.pattern(keyed, pattern)
                .process(new EmitCardTestingHit(config))
                .returns(JsonTypeInfo.of(KeyedRuleHit.class))
                .name("card-testing-cep")
                .uid("card-testing-cep");
    }

    private static class EmitCardTestingHit extends PatternProcessFunction<Transaction, KeyedRuleHit> {

        private final RuleConfig config;

        EmitCardTestingHit(RuleConfig config) {
            this.config = config;
        }

        @Override
        public void processMatch(Map<String, List<Transaction>> match, Context ctx,
                                 Collector<KeyedRuleHit> out) {
            List<Transaction> probes = match.get("probes");
            Transaction large = match.get("large").get(0);

            // The hit is attributed to the large transaction, not to the probes: that is the one
            // worth blocking, and the probes on their own are indistinguishable from small
            // legitimate spend until the large charge lands.
            RuleHit hit = RuleHit.of(RuleId.CARD_TESTING, config.weight(), config.version(),
                    Map.of("probeCount", probes.size(),
                            "probeTotalMinor", probes.stream().mapToLong(Transaction::amountMinor).sum(),
                            "largeAmountMinor", large.amountMinor(),
                            "spanSeconds",
                            (large.eventTimeMillis() - probes.get(0).eventTimeMillis()) / 1000));

            out.collect(new KeyedRuleHit(large.transactionId(), large.eventTimeMillis(), hit));
        }
    }
}
