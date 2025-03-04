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

    @Override
    public void write(CypherStatement element, Context context) throws IOException, InterruptedException {
        batch.add(element);
        if (batch.size() >= 100) { // 批量写入，提升性能
            flush(true);
        }
    }

    @Override
    public void flush(boolean b) throws IOException, InterruptedException {
        String dbUri = "bolt://localhost:7687";
        String dbUser = "neo4j";
        String dbPassword = "12345678";

        if (driver == null) {
            driver = GraphDatabase.driver(dbUri, AuthTokens.basic(dbUser, dbPassword));
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
