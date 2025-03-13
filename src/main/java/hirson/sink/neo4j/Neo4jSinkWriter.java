package hirson.sink.neo4j;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.types.Row;
import org.neo4j.driver.*;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class Neo4jSinkWriter implements SinkWriter<Row>, Serializable {
    private transient Driver driver;
    private transient Session session;
    private transient Transaction transaction;
    private final List<Row> batch = new ArrayList<>();
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
    public void write(Row element, Context context) throws IOException, InterruptedException {
        // 数据格式校验
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
            for (Row stmt : batch) {
                String query = (String) stmt.getField(0);
                Map<String, Object> parameters = convertRowToMap(stmt);
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

    private Map<String, Object> convertRowToMap(Row stmt) {
        Map<String, Object> params = new HashMap<>();
        Row paramsKeyRow = (Row)stmt.getField(1);
        Row paramsValueRow = (Row)stmt.getField(2);
        for (int pos = 0; pos < paramsKeyRow.getArity(); pos++) {
            String key =(String) paramsKeyRow.getField(pos);
            Object value = paramsValueRow.getField(pos);
            params.put(key, value);
        }
        return params;
    }

    @Override
    public void close() throws Exception {
        if (driver != null) {
            driver.close();
        }
    }
}