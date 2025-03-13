
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;

public class Neo4jSink implements Sink<CypherStatement> {
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
    public SinkWriter<CypherStatement> createWriter(InitContext initContext) throws IOException {
        return new Neo4jSinkWriter(uri, user, password, batchSize);
    }
}
