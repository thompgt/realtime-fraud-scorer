package com.thompgt.fraud.flink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;

/**
 * Hands the Kafka value through untouched.
 *
 * <p>The source deliberately does no parsing: a deserialiser that throws inside a {@code
 * KafkaSource} fails the job, which restarts, reads the same offset, and fails again — a poison
 * message becomes an outage. Keeping bytes here lets {@link ParseTransaction} reject one record
 * without stopping the stream.
 */
class ByteArrayDeserializationSchema implements DeserializationSchema<byte[]> {

    @Override
    public byte[] deserialize(byte[] message) {
        return message;
    }

    @Override
    public boolean isEndOfStream(byte[] nextElement) {
        return false;
    }

    @Override
    public TypeInformation<byte[]> getProducedType() {
        return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
    }
}
