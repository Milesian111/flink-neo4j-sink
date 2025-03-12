package hirson.sink.neo4j;


import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Neo4jSinkBuilder implements Serializable {
    private String uri;
    private String user ;
    private String password ;
    private int batchSize;

    public Neo4jSinkBuilder setUri(String uri) {
        this.uri = uri;
        return this;
    }

    public Neo4jSinkBuilder setUser(String user) {
        this.user = user;
        return this;
    }

    public Neo4jSinkBuilder setPassword(String password) {
        this.password = password;
        return this;
    }

    public Neo4jSinkBuilder setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public Neo4jSink build() {
        return new Neo4jSink(uri, user, password, batchSize);
    }
}