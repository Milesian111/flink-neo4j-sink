import org.apache.flink.api.connector.sink2.*;

import java.io.IOException;

public class Neo4jSink implements Sink<CypherStatement> {


    @Override
    public SinkWriter<CypherStatement> createWriter(InitContext initContext) throws IOException {
        return new Neo4jSinkWriter();
    }

    @Override
    public SinkWriter<CypherStatement> createWriter(WriterInitContext context) throws IOException {
        return Sink.super.createWriter(context);
    }

}
