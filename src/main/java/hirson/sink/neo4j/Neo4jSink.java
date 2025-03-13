package hirson.sink.neo4j;

import org.apache.flink.api.connector.sink2.*;
import org.apache.flink.types.Row;

import java.io.IOException;


public class Neo4jSink implements Sink<Row> {
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
    public SinkWriter<Row> createWriter(InitContext initContext) throws IOException {
        return new Neo4jSinkWriter(uri, user, password, batchSize);
    }
}