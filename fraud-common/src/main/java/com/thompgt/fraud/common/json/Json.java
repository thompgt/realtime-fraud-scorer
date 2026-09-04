package com.thompgt.fraud.common.json;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * The one JSON codec for the whole system.
 *
 * <p>The generator, the Flink job and the API all go through here. If each module built its own
 * {@link ObjectMapper} the wire format would drift silently — one side writing ISO-8601 timestamps
 * while the other expects epoch millis is the classic version of that bug, and it only shows up in
 * production traffic.
 *
 * <p>The mapper is immutable once configured and Jackson's is thread-safe, so a single static
 * instance is correct even inside parallel Flink operators.
 */
public final class Json {

    private static final ObjectMapper MAPPER = newMapper();

    private Json() {
    }

    /**
     * Builds the canonical mapper. Public because Spring Boot needs to register this exact
     * configuration as its own bean rather than let the starter auto-configure a different one.
     */
    public static ObjectMapper newMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());

        // Instants as epoch millis, not ISO-8601 strings. Flink compares event times as longs
        // millions of times a second, and Kafka records are cheaper to move without the extra
        // ~20 bytes per timestamp. The two features must be set together: WRITE_DATES_AS_TIMESTAMPS
        // alone gives seconds-with-nanos as a decimal, which is not what a millis reader expects.
        mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.disable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS);
        mapper.disable(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS);

        // Tolerate unknown fields on read. Producers and consumers are deployed independently, so
        // a new field has to be able to appear on the wire before every reader knows about it.
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        // Omit nulls: deviceFingerprint is absent for most card-not-present traffic and there is no
        // reason to pay for it on every record.
        mapper.setSerializationInclusion(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL);

        return mapper;
    }

    public static ObjectMapper mapper() {
        return MAPPER;
    }

    public static byte[] toBytes(Object value) {
        try {
            return MAPPER.writeValueAsBytes(value);
        } catch (IOException e) {
            // Serialising our own records should never fail; if it does the model is broken, not
            // the data, so this is a bug rather than a bad message.
            throw new IllegalStateException("Failed to serialise " + value.getClass().getName(), e);
        }
    }

    public static String toJson(Object value) {
        return new String(toBytes(value), StandardCharsets.UTF_8);
    }

    /**
     * @throws JsonCodecException on malformed input — callers route these to the DLQ rather than
     *     failing the job, since one bad record must not stop the stream.
     */
    public static <T> T fromBytes(byte[] bytes, Class<T> type) {
        try {
            return MAPPER.readValue(bytes, type);
        } catch (IOException | IllegalArgumentException e) {
            throw new JsonCodecException("Failed to parse " + type.getSimpleName(), e);
        }
    }

    public static <T> T fromJson(String json, Class<T> type) {
        return fromBytes(json.getBytes(StandardCharsets.UTF_8), type);
    }
}
