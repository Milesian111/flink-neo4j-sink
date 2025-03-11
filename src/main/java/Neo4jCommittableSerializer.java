import org.apache.flink.core.io.SimpleVersionedSerializer;
import java.io.*;
import java.util.List;

public class Neo4jCommittableSerializer implements SimpleVersionedSerializer<Neo4jCommittable> {

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(Neo4jCommittable committable) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(committable.getStatements());
            return bos.toByteArray();
        }
    }

    @Override
    public Neo4jCommittable deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            @SuppressWarnings("unchecked")
            List<CypherStatement> statements = (List<CypherStatement>) ois.readObject();
            return new Neo4jCommittable(statements);
        } catch (ClassNotFoundException e) {
            throw new IOException("Deserialization failed", e);
        }
    }
}
