import org.apache.flink.api.connector.sink2.Committer;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;

import java.io.IOException;
import java.util.Collection;

public  class Neo4jCommitter implements Committer<Neo4jCommittable> {

    private final Neo4jConfig config;
    private Driver driver;

    public Neo4jCommitter(Neo4jConfig config) {
        this.config = config;
    }

    @Override
    public void commit(Collection<CommitRequest<Neo4jCommittable>> committables) {
        if (driver == null) {
            driver = GraphDatabase.driver(
                    config.getUri(),
                    AuthTokens.basic(config.getUser(), config.getPassword()));
        }

        try (Session session = driver.session(config.getSessionConfig())) {
            for (CommitRequest<Neo4jCommittable> request : committables) {
                executeTransaction(session, request.getCommittable());
            }
        }
    }

    private void executeTransaction(Session session, Neo4jCommittable committable) {
        int retries = config.getMaxRetries();
        while (retries-- > 0) {
            try (Transaction tx = session.beginTransaction()) {
                for (CypherStatement stmt : committable.getStatements()) {
                    tx.run(stmt.getQuery(), stmt.getParameters());
                }
                tx.commit();
                return;
            } catch (Exception e) {
                if (retries == 0) {
                    throw new RuntimeException("Transaction failed after retries", e);
                }
                try {
                    Thread.sleep(config.getRetryInterval());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during retry", ie);
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (driver != null) {
            driver.closeAsync();
        }
    }
}