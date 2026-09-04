package com.thompgt.fraud.flink.serde;

import com.thompgt.fraud.common.json.Json;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Objects;

/**
 * Moves the domain records between operators and in and out of state as JSON.
 *
 * <p><b>Why not just let Flink do it.</b> The domain types are Java records. Flink's POJO analyser
 * rejects them — no no-arg constructor, no setters — and silently falls back to Kryo, which is both
 * slower than the POJO path people assume they are getting and a state-compatibility hazard: a
 * Kryo-serialised savepoint is tied to field ordering, so adding a field to {@code Transaction}
 * would make an existing savepoint unrestorable. Being explicit here turns that into a compile-time
 * choice instead of a runtime surprise, and it means state and the wire format use the same codec,
 * so a schema change is tolerated in exactly one place ({@code Json}'s unknown-field handling)
 * rather than two.
 *
 * <p>The cost is real — JSON is bulkier and slower than a binary encoding — and at genuinely high
 * throughput the answer would be a schema system (Avro, Protobuf) with generated classes rather
 * than hand-written serializers. At this project's scale the trade favours one codec everywhere.
 */
public final class JsonTypeSerializer<T> extends TypeSerializer<T> {

    private final Class<T> type;

    public JsonTypeSerializer(Class<T> type) {
        this.type = type;
    }

    Class<T> type() {
        return type;
    }

    @Override
    public boolean isImmutableType() {
        // Every type this serialises is a record. Flink can then skip defensive copies entirely.
        return true;
    }

    @Override
    public TypeSerializer<T> duplicate() {
        return this;
    }

    @Override
    public T createInstance() {
        // Only used by serializers that deserialize into a reused instance; ours always allocates.
        return null;
    }

    @Override
    public T copy(T from) {
        return from;
    }

    @Override
    public T copy(T from, T reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return -1; // variable length
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        byte[] bytes = Json.toBytes(record);
        target.writeInt(bytes.length);
        target.write(bytes);
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        byte[] bytes = new byte[source.readInt()];
        source.readFully(bytes);
        return Json.fromBytes(bytes, type);
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        // Byte-for-byte, without a parse round trip in between.
        int length = source.readInt();
        target.writeInt(length);
        target.write(source, length);
    }

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        return new JsonSerializerSnapshot<>(type);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof JsonTypeSerializer<?> other && type.equals(other.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type);
    }
}
