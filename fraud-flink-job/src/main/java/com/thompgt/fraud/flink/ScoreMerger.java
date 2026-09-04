package com.thompgt.fraud.flink;

import com.thompgt.fraud.common.model.RuleHit;
import com.thompgt.fraud.common.model.ScoredTransaction;
import com.thompgt.fraud.flink.serde.JsonTypeInfo;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Reassembles the score for one transaction from the two branches that can contribute to it: the
 * main per-card evaluator and the CEP pattern.
 *
 * <p>Both branches are keyed here by transaction id and buffered until the watermark passes the
 * transaction's event time plus a small grace, then merged and emitted once. The grace exists
 * because the two branches take different paths through the job and can arrive in either order —
 * emitting the evaluator's verdict on arrival would publish a score that a CEP hit then contradicts
 * milliseconds later, and downstream would see two different scores for one transaction.
 *
 * <p>Watermarks make this exact rather than a race: the merged stream's watermark is the minimum
 * across both inputs, so once it passes a timestamp, <em>both</em> branches have finished emitting
 * for it. The grace only absorbs the CEP operator's own emission lag, not correctness.
 *
 * <p>The cost is that every transaction is held for the grace period, so it is a direct trade of
 * end-to-end latency for a single consistent verdict. One second is cheap against a payments
 * authorisation, but it is a real choice and is configurable.
 */
class ScoreMerger extends KeyedCoProcessFunction<String, ScoredTransaction, KeyedRuleHit, ScoredTransaction> {

    private static final Logger LOG = LoggerFactory.getLogger(ScoreMerger.class);

    private final long graceMillis;

    private transient ValueState<ScoredTransaction> pending;
    private transient ListState<RuleHit> extraHits;
    private transient ValueState<Long> timerTimestamp;

    private transient Counter orphanedHits;
    private transient Counter mergedHits;

    ScoreMerger(long graceMillis) {
        this.graceMillis = graceMillis;
    }

    @Override
    public void open(OpenContext ctx) {
        pending = getRuntimeContext().getState(new ValueStateDescriptor<>(
                "pendingScore", JsonTypeInfo.of(ScoredTransaction.class)));
        extraHits = getRuntimeContext().getListState(new ListStateDescriptor<>(
                "extraHits", JsonTypeInfo.of(RuleHit.class)));
        timerTimestamp = getRuntimeContext().getState(
                new ValueStateDescriptor<>("timerTimestamp", org.apache.flink.api.common.typeinfo.Types.LONG));

        orphanedHits = getRuntimeContext().getMetricGroup().counter("orphanedRuleHits");
        mergedHits = getRuntimeContext().getMetricGroup().counter("mergedRuleHits");
    }


    @Override
    public void processElement1(ScoredTransaction scored, Context ctx,
                                Collector<ScoredTransaction> out) throws Exception {
        pending.update(scored);
        scheduleFlush(scored.transaction().eventTimeMillis(), ctx);
    }

    @Override
    public void processElement2(KeyedRuleHit keyedHit, Context ctx,
                                Collector<ScoredTransaction> out) throws Exception {
        extraHits.add(keyedHit.hit());
        scheduleFlush(keyedHit.eventTimeMillis(), ctx);
    }

    /** One timer per transaction; whichever branch arrives first sets it. */
    private void scheduleFlush(long eventTime, Context ctx) throws Exception {
        if (timerTimestamp.value() != null) {
            return;
        }
        long fireAt = eventTime + graceMillis;
        timerTimestamp.update(fireAt);
        ctx.timerService().registerEventTimeTimer(fireAt);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<ScoredTransaction> out)
            throws Exception {
        ScoredTransaction scored = pending.value();
        List<RuleHit> extra = new ArrayList<>();
        extraHits.get().forEach(extra::add);

        if (scored == null) {
            // A CEP hit with no matching scored transaction. Should not happen -- both branches
            // read the same source -- so it means the evaluator dropped a record or the ids do not
            // line up, and the counter is what makes that visible rather than silent.
            if (!extra.isEmpty()) {
                orphanedHits.inc(extra.size());
                LOG.warn("Discarding {} rule hit(s) for transaction {} with no scored record",
                        extra.size(), ctx.getCurrentKey());
            }
        } else if (extra.isEmpty()) {
            out.collect(scored);
        } else {
            mergedHits.inc(extra.size());
            List<RuleHit> all = new ArrayList<>(scored.hits());
            all.addAll(extra);
            int score = all.stream().mapToInt(RuleHit::weight).sum();
            out.collect(new ScoredTransaction(scored.transaction(), score, all, scored.scoredAt()));
        }

        // Nothing about this transaction is needed again, and leaving it would make state grow with
        // every transaction ever seen rather than with the in-flight window.
        pending.clear();
        extraHits.clear();
        timerTimestamp.clear();
    }
}
