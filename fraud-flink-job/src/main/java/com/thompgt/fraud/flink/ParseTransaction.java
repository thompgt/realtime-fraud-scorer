package com.thompgt.fraud.flink;

import com.thompgt.fraud.common.json.Json;
import com.thompgt.fraud.common.model.Transaction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * Turns raw Kafka bytes into {@link Transaction}s, diverting anything unparseable to a side output.
 *
 * <p>The alternative — deserialising inside the {@code KafkaSource} — has no way to reject a single
 * bad record: the source either throws, which restarts the job into the same poison message on the
 * same offset forever, or swallows the error and loses the record silently. Parsing as a separate
 * operator with a DLQ tag is the only version where a malformed message is both survivable and
 * still visible afterwards.
 */
class ParseTransaction extends ProcessFunction<byte[], Transaction> {

    private static final Logger LOG = LoggerFactory.getLogger(ParseTransaction.class);

    /** Records that could not be parsed, republished to the DLQ topic with the failure reason. */
    static final OutputTag<String> DLQ = new OutputTag<>("dlq") {
    };

    private transient Counter parsed;
    private transient Counter rejected;

    @Override
    public void open(OpenContext ctx) {
        // Both counters are visible per-subtask in the Flink UI. A non-zero rejected count is the
        // signal that a producer changed the wire format without telling anyone.
        parsed = getRuntimeContext().getMetricGroup().counter("transactionsParsed");
        rejected = getRuntimeContext().getMetricGroup().counter("transactionsRejected");
    }


    @Override
    public void processElement(byte[] value, Context ctx, Collector<Transaction> out) {
        try {
            out.collect(Json.fromBytes(value, Transaction.class));
            parsed.inc();
        } catch (RuntimeException e) {
            rejected.inc();
            String payload = value == null ? "" : new String(value, StandardCharsets.UTF_8);
            // Truncated: a DLQ record exists to identify the producer and the shape of the problem,
            // and an unbounded payload copy would let one runaway message fill the topic.
            String snippet = payload.length() > 1024 ? payload.substring(0, 1024) + "..." : payload;
            LOG.warn("Routing unparseable record to DLQ: {}", e.getMessage());
            ctx.output(DLQ, Json.toJson(new DlqRecord(e.getMessage(), snippet)));
        }
    }

    record DlqRecord(String error, String payload) {
    }
}
