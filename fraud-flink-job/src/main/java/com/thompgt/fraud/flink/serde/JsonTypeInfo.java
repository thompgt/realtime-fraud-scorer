package com.thompgt.fraud.flink.serde;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Objects;

/**
 * {@link TypeInformation} that routes a type to {@link JsonTypeSerializer}.
 *
 * <p>Attached with {@code .returns(...)} at each point a domain record enters the topology, and
 * passed explicitly to every state descriptor holding one. Both are needed: a stream's declared
 * type does not propagate into state descriptors.
 */
public final class JsonTypeInfo<T> extends TypeInformation<T> {

    private final Class<T> type;

    private JsonTypeInfo(Class<T> type) {
        this.type = type;
    }

    public static <T> JsonTypeInfo<T> of(Class<T> type) {
        return new JsonTypeInfo<>(type);
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        // Opaque to Flink's field-expression machinery, so it counts as a single field. That does
        // mean keyBy("someField") will not work on these types -- use a lambda key selector, which
        // is checked at compile time anyway.
        return 1;
    }

    @Override
    public Class<T> getTypeClass() {
        return type;
    }

    @Override
    public boolean isKeyType() {
        // JSON key ordering is not guaranteed stable, so these must never be used as a key
        // directly; every keyBy in this job extracts a String.
        return false;
    }

    @Override
    public TypeSerializer<T> createSerializer(SerializerConfig config) {
        return new JsonTypeSerializer<>(type);
    }

    @Override
    @Deprecated
    public TypeSerializer<T> createSerializer(ExecutionConfig config) {
        return new JsonTypeSerializer<>(type);
    }

    @Override
    public String toString() {
        return "Json(" + type.getSimpleName() + ")";
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof JsonTypeInfo<?> other && type.equals(other.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type);
    }

    @Override
    public boolean canEqual(Object o) {
        return o instanceof JsonTypeInfo;
    }
}
