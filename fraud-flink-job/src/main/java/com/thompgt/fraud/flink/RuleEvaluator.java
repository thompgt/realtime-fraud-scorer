package com.thompgt.fraud.flink;

import com.thompgt.fraud.common.model.RuleHit;
import com.thompgt.fraud.common.model.RuleId;
import com.thompgt.fraud.common.model.ScoredTransaction;
import com.thompgt.fraud.common.model.Transaction;
import com.thompgt.fraud.common.rules.RuleConfig;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The three per-card stateful rules, evaluated together on the keyed stream: velocity, amount
 * anomaly and geo-impossibility.
 *
 * <p><b>Why one operator rather than three.</b> All three need the same keyed state locality and
 * must emit a verdict for <em>every</em> transaction, so splitting them would mean three shuffles
 * of the same stream and then a join to reassemble one score per transaction. Card testing is the
 * exception and does run separately, because it is a multi-event pattern that only fires
 * occasionally — see {@link CardTestingPattern}.
 *
 * <p><b>Why velocity is not a {@code ProcessWindowFunction}.</b> A sliding window emits once per
 * window, but scoring has to emit once per transaction, with the count as it stood at that
 * transaction. Keeping the recent timestamps in {@link ListState} and pruning them on arrival gives
 * exactly the sliding-window count without the impedance mismatch, and it makes the window length
 * a runtime value rather than one baked into the topology — which is what lets Phase 4 change it
 * from ZooKeeper without a redeploy.
 */
class RuleEvaluator extends KeyedProcessFunction<String, Transaction, ScoredTransaction> {

    /**
     * Phase 3 scores against the static bootstrap defaults. Phase 4 replaces this field with a
     * broadcast-state lookup; keeping the shape as a map from ruleId to config means that swap does
     * not touch any of the rule logic below.
     */
    private final Map<String, RuleConfig> rules;

    private final long stateTtlSeconds;

    private transient ListState<Long> recentEventTimes;
    private transient ValueState<Long> amountCount;
    private transient ValueState<Double> amountMean;
    private transient ValueState<String> lastCountry;
    private transient ValueState<Long> lastCountryTime;

    private transient Counter lateEvents;
    private transient Map<String, Counter> hitCounters;

    RuleEvaluator(Map<String, RuleConfig> rules) {
        this.rules = Map.copyOf(rules);
        RuleConfig anomaly = rules.get(RuleId.AMOUNT_ANOMALY);
        this.stateTtlSeconds = anomaly == null ? 604_800L : anomaly.longParam("stateTtlSeconds");
    }

    @Override
    public void open(OpenContext ctx) {
        // TTL is set on the descriptor, which Flink fixes at operator initialisation. So unlike the
        // thresholds, this one value genuinely cannot be changed from ZooKeeper on a running job --
        // changing it needs a restart from a savepoint. Called out here so the Phase 4 story is not
        // oversold.
        StateTtlConfig ttl = StateTtlConfig
                .newBuilder(Duration.ofSeconds(stateTtlSeconds))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(1000)
                .build();

        ListStateDescriptor<Long> recent = new ListStateDescriptor<>("recentEventTimes", Types.LONG);
        ValueStateDescriptor<Long> count = new ValueStateDescriptor<>("amountCount", Types.LONG);
        ValueStateDescriptor<Double> mean = new ValueStateDescriptor<>("amountMean", Types.DOUBLE);
        ValueStateDescriptor<String> country = new ValueStateDescriptor<>("lastCountry", Types.STRING);
        ValueStateDescriptor<Long> countryTime =
                new ValueStateDescriptor<>("lastCountryTime", Types.LONG);
        // Without TTL this state grows by one entry per card seen, forever. Cards go dormant and
        // never signal it, so nothing else would ever remove them.
        List.of(recent, count, mean, country, countryTime).forEach(d -> d.enableTimeToLive(ttl));

        recentEventTimes = getRuntimeContext().getListState(recent);
        amountCount = getRuntimeContext().getState(count);
        amountMean = getRuntimeContext().getState(mean);
        lastCountry = getRuntimeContext().getState(country);
        lastCountryTime = getRuntimeContext().getState(countryTime);

        lateEvents = getRuntimeContext().getMetricGroup().counter("lateEvents");
        hitCounters = new HashMap<>();
        for (String ruleId : RuleId.ALL) {
            hitCounters.put(ruleId,
                    getRuntimeContext().getMetricGroup().counter("ruleHits." + ruleId));
        }
    }


    @Override
    public void processElement(Transaction txn, Context ctx, Collector<ScoredTransaction> out)
            throws Exception {

        long eventTime = txn.eventTimeMillis();

        // Late data is scored, not dropped. A transaction that arrives behind the watermark is
        // still a real authorisation someone has to answer for, and the alternative -- discarding
        // it -- turns a monitoring problem into a missed-fraud problem. What it does lose is
        // ordering, so the geo rule below refuses to reason about a negative time gap.
        if (eventTime < ctx.timerService().currentWatermark()) {
            lateEvents.inc();
        }

        List<RuleHit> hits = new ArrayList<>(3);
        velocity(txn, eventTime, hits);
        amountAnomaly(txn, hits);
        geoImpossible(txn, eventTime, hits);

        int score = hits.stream().mapToInt(RuleHit::weight).sum();
        hits.forEach(h -> hitCounters.get(h.ruleId()).inc());

        out.collect(new ScoredTransaction(txn, score, hits, Instant.now()));
    }

    /** More than {@code maxCount} authorisations on this card inside {@code windowSeconds}. */
    private void velocity(Transaction txn, long eventTime, List<RuleHit> hits) throws Exception {
        RuleConfig config = rules.get(RuleId.VELOCITY);
        if (config == null || !config.enabled()) {
            return;
        }
        long windowMillis = config.longParam("windowSeconds") * 1000L;
        long cutoff = eventTime - windowMillis;

        // Rewriting the whole list on every event is fine at this cardinality: the list only ever
        // holds one window's worth of timestamps for one card, which the threshold itself bounds to
        // tens of entries. A card that did hold thousands would be alerting long before the list
        // mattered.
        List<Long> kept = new ArrayList<>();
        for (Long ts : recentEventTimes.get()) {
            if (ts > cutoff) {
                kept.add(ts);
            }
        }
        kept.add(eventTime);
        recentEventTimes.update(kept);

        int max = config.intParam("maxCount");
        if (kept.size() > max) {
            hits.add(RuleHit.of(RuleId.VELOCITY, config.weight(), config.version(),
                    Map.of("count", kept.size(),
                            "maxCount", max,
                            "windowSeconds", config.intParam("windowSeconds"))));
        }
    }

    /** This amount against the card's own rolling mean, once the mean means anything. */
    private void amountAnomaly(Transaction txn, List<RuleHit> hits) throws Exception {
        RuleConfig config = rules.get(RuleId.AMOUNT_ANOMALY);
        if (config == null || !config.enabled()) {
            return;
        }
        long count = amountCount.value() == null ? 0L : amountCount.value();
        double mean = amountMean.value() == null ? 0.0 : amountMean.value();

        long minObservations = config.longParam("minObservations");
        double multiplier = config.param("meanMultiplier");
        long minAmount = config.longParam("minAmountMinor");

        // Evaluate against the mean as it stood *before* this transaction, then update. Folding the
        // amount in first would let a single large charge drag the mean up toward itself and mask
        // the very anomaly being tested for.
        if (count >= minObservations
                && txn.amountMinor() >= minAmount
                && txn.amountMinor() > mean * multiplier) {
            hits.add(RuleHit.of(RuleId.AMOUNT_ANOMALY, config.weight(), config.version(),
                    Map.of("amountMinor", txn.amountMinor(),
                            "rollingMeanMinor", Math.round(mean),
                            "meanMultiplier", multiplier,
                            "observations", count)));
        }

        // Incremental mean: keeping a running sum would overflow on a long-lived card, and storing
        // the history to average it would be unbounded state for no extra information.
        long newCount = count + 1;
        amountCount.update(newCount);
        amountMean.update(mean + (txn.amountMinor() - mean) / newCount);
    }

    /** Two countries far enough apart that the gap between them implies impossible travel. */
    private void geoImpossible(Transaction txn, long eventTime, List<RuleHit> hits)
            throws Exception {
        RuleConfig config = rules.get(RuleId.GEO_IMPOSSIBLE);
        if (config == null || !config.enabled()) {
            return;
        }
        String previous = lastCountry.value();
        Long previousTime = lastCountryTime.value();

        if (previous != null && previousTime != null && !previous.equals(txn.countryCode())) {
            long gapMillis = eventTime - previousTime;
            long maxGapMillis = config.longParam("maxGapSeconds") * 1000L;

            // A non-positive gap means the two events arrived out of order, so the implied speed is
            // meaningless -- abstain rather than emit a hit built on a negative duration.
            if (gapMillis > 0 && gapMillis <= maxGapMillis) {
                double km = Countries.distanceKm(previous, txn.countryCode());
                if (km >= 0) {
                    double hours = gapMillis / 3_600_000.0;
                    double kmPerHour = km / hours;
                    if (kmPerHour > config.param("maxKmPerHour")) {
                        hits.add(RuleHit.of(RuleId.GEO_IMPOSSIBLE, config.weight(),
                                config.version(),
                                Map.of("fromCountry", previous,
                                        "toCountry", txn.countryCode(),
                                        "distanceKm", Math.round(km),
                                        "gapSeconds", gapMillis / 1000,
                                        "impliedKmPerHour", Math.round(kmPerHour))));
                    }
                }
            }
        }

        // Only advance the last-seen location when this event is genuinely the newest one for the
        // card. Otherwise a late arrival would rewrite history and make the *next* legitimate
        // transaction look impossible.
        if (previousTime == null || eventTime >= previousTime) {
            lastCountry.update(txn.countryCode());
            lastCountryTime.update(eventTime);
        }
    }
}
