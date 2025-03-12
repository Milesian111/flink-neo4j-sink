package hirson.sink.neo4j;

import org.apache.flink.api.connector.sink2.*;
import java.io.IOException;
import java.util.Map;

public class Neo4jSink implements Sink<Map<String, Object>> {
    private final String uri;
    private final String user;
    private final String password;
    private final int batchSize;

    public Neo4jSink(String uri, String user, String password, int batchSize) {
        this.uri = uri;
        this.user = user;
        this.password = password;
        this.batchSize = batchSize;
    }

    @Override
    public SinkWriter<Map<String, Object>> createWriter(InitContext initContext) throws IOException {
        return new Neo4jSinkWriter(uri, user, password, batchSize);
    }
}