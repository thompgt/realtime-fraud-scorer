package com.thompgt.fraud.flink.serde;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * State-compatibility metadata for {@link JsonTypeSerializer}.
 *
 * <p>Only the class name is written, because that is the only thing that can make a restore
 * invalid. Field-level evolution is already handled by the codec itself: {@code Json} ignores
 * unknown fields on read, so a savepoint written before a field existed restores cleanly against
 * the newer record. That is the whole reason for preferring JSON over Kryo in state — a Kryo
 * snapshot would have to be declared incompatible for the same change.
 */
public final class JsonSerializerSnapshot<T> implements TypeSerializerSnapshot<T> {

    private static final int VERSION = 1;

    private Class<T> type;

    /** Required by Flink, which instantiates the snapshot reflectively before reading it. */
    public JsonSerializerSnapshot() {
    }

    JsonSerializerSnapshot(Class<T> type) {
        this.type = type;
    }

    @Override
    public int getCurrentVersion() {
        return VERSION;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
        out.writeUTF(type.getName());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
            throws IOException {
        String className = in.readUTF();
        try {
            // The user-code classloader, not this class's: on a session cluster the job jar is
            // loaded separately from the Flink runtime and Class.forName would miss it.
            type = (Class<T>) Class.forName(className, false, userCodeClassLoader);
        } catch (ClassNotFoundException e) {
            throw new IOException("Cannot restore state: " + className + " is no longer on the "
                    + "classpath. Renaming or removing a state type needs a state migration.", e);
        }
    }

    @Override
    public TypeSerializer<T> restoreSerializer() {
        return new JsonTypeSerializer<>(type);
    }

    @Override
    public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
            TypeSerializerSnapshot<T> oldSerializerSnapshot) {
        if (!(oldSerializerSnapshot instanceof JsonSerializerSnapshot<T> old)) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }
        return type.equals(old.type)
                ? TypeSerializerSchemaCompatibility.compatibleAsIs()
                : TypeSerializerSchemaCompatibility.incompatible();
    }
}
