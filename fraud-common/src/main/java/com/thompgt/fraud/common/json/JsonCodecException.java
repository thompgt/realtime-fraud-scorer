package com.thompgt.fraud.common.json;

/**
 * Thrown when a payload cannot be parsed into a domain record.
 *
 * <p>Unchecked on purpose: it is thrown from inside Flink map/process functions where a checked
 * exception would have to be caught and rethrown at every call site. The Flink job catches it once
 * and routes the offending record to the DLQ side output.
 */
public class JsonCodecException extends RuntimeException {

    public JsonCodecException(String message, Throwable cause) {
        super(message, cause);
    }
}
