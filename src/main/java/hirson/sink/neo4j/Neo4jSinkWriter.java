package hirson.sink.neo4j;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.neo4j.driver.*;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Neo4jSinkWriter implements SinkWriter<Map<String, Object>>, Serializable {
    private transient Driver driver;
    private transient Session session;
    private transient Transaction transaction;
    private final List<Map<String, Object>> batch = new ArrayList<>();
    private final String uri;
    private final String user;
    private final String password;
    private final int batchSize;

    public Neo4jSinkWriter(String uri, String user, String password, int batchSize) {
        this.uri = uri;
        this.user = user;
        this.password = password;
        this.batchSize = batchSize;
    }

    @Override
    public void write(Map<String, Object> element, Context context) throws IOException, InterruptedException {
        // 数据格式校验
        if (!element.containsKey("query") || !element.containsKey("parameters")) {
            throw new IllegalArgumentException("Map must contain 'query' and 'parameters' keys");
        }
        if (!(element.get("query") instanceof String) || !(element.get("parameters") instanceof Map)) {
            throw new IllegalArgumentException("'query' must be String and 'parameters' must be Map");
        }

        batch.add(element);
        if (batch.size() >= batchSize) {
            flush(true);
        }
    }

    @Override
    public void flush(boolean b) throws IOException, InterruptedException {
        if (driver == null) {
            driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
            session = driver.session();
            transaction = session.beginTransaction();
        }

        try {
            for (Map<String, Object> stmt : batch) {
                String query = (String) stmt.get("query");
                Map<String, Object> parameters = (Map<String, Object>) stmt.get("parameters");
                transaction.run(query, parameters);
            }
            transaction.commit();
        } catch (Exception e) {
            transaction.rollback();
            throw new RuntimeException("Neo4j write failed", e);
        } finally {
            batch.clear();
            transaction.close();
            session.close();
            driver.close();
        }
    }

    @Override
    public void close() throws Exception {
        if (driver != null) {
            driver.close();
        }
    }
}