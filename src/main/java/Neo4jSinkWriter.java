import org.apache.flink.api.connector.sink2.SinkWriter;


import org.neo4j.driver.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class Neo4jSinkWriter implements SinkWriter<CypherStatement>, Serializable {
    private transient Driver driver;
    private transient Session session;
    private transient Transaction transaction;
    private final List<CypherStatement> batch = new ArrayList<>();
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
    public void write(CypherStatement element, Context context) throws IOException, InterruptedException {
        batch.add(element);
        if (batch.size() >= batchSize) { // 批量写入，提升性能
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
            for (CypherStatement stmt : batch) {
                transaction.run(stmt.getQuery(), stmt.getParameters());
            }
            transaction.commit(); // 提交当前事务
        } catch (Exception e) {
            transaction.rollback(); // 失败回滚
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
