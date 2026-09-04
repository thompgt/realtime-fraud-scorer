package com.thompgt.fraud.flink;

import com.thompgt.fraud.common.model.Alert;
import com.thompgt.fraud.common.model.ScoredTransaction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Placeholder terminal operator for Phase 3: counts alerts so they show in the Flink UI and logs
 * one line each so the seeded fraud scenarios can be read out of the TaskManager log.
 *
 * <p>Replaced in Phase 5 by the Kafka and Postgres sinks. It stays a {@code ProcessFunction} rather
 * than a {@code print()} sink so the counter exists — a printed line is invisible to metrics, and
 * "is the job detecting anything" should be answerable from the dashboard, not by tailing a log.
 */
class LogAlert extends ProcessFunction<ScoredTransaction, ScoredTransaction> {

    private static final Logger LOG = LoggerFactory.getLogger(LogAlert.class);

    private transient Counter alerts;

    @Override
    public void open(OpenContext ctx) {
        alerts = getRuntimeContext().getMetricGroup().counter("alertsEmitted");
    }


    @Override
    public void processElement(ScoredTransaction scored, Context ctx,
                               Collector<ScoredTransaction> out) {
        alerts.inc();
        Alert alert = Alert.from(scored);
        LOG.info("ALERT card={} txn={} score={} rules={} amountMinor={} evidence={}",
                alert.cardId(), alert.transactionId(), alert.score(), alert.ruleIds(),
                alert.amountMinor(), alert.hits());
        out.collect(scored);
    }
}
