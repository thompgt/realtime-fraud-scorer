package com.thompgt.fraud.flink;

import com.thompgt.fraud.common.model.RuleHit;
import com.thompgt.fraud.common.model.RuleId;
import com.thompgt.fraud.common.model.Transaction;
import com.thompgt.fraud.common.rules.RuleConfig;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * Scores every transaction against {@link ModelScore} and emits a hit for the ones above the
 * configured probability cutoff.
 *
 * <p>Runs directly on the parsed, unkeyed stream rather than after {@code keyBy(cardId)}: the model
 * looks only at fields on the one transaction in front of it, so there is no per-card state to
 * localise and keying first would just add an unnecessary shuffle before this operator. Its output
 * joins {@link CardTestingPattern}'s output the same way -- both are {@link KeyedRuleHit} streams
 * addressed to a transaction id, unioned before {@link ScoreMerger} keys them by it.
 */
final class ModelEnricher extends ProcessFunction<Transaction, KeyedRuleHit> {

    private final RuleConfig config;

    ModelEnricher(RuleConfig config) {
        this.config = config;
    }

    @Override
    public void processElement(Transaction txn, Context ctx, Collector<KeyedRuleHit> out) {
        if (!config.enabled()) {
            return;
        }

        double probability = ModelScore.probability(txn);
        if (probability < config.param("probThreshold")) {
            return;
        }

        RuleHit hit = RuleHit.of(RuleId.MODEL_SCORE, config.weight(), config.version(),
                Map.of("probability", probability));
        out.collect(new KeyedRuleHit(txn.transactionId(), txn.eventTimeMillis(), hit));
    }
}
