import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.neo4j.driver.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Neo4jSinkWriter
        implements CommittingSinkWriter<CypherStatement,Neo4jCommittable>, CheckpointedFunction {

    private final Neo4jConfig config;
    private transient Driver driver;
    private transient Session session;
    private List<CypherStatement> batch = new ArrayList<>();
    private transient ListState<CypherStatement> checkpointedState;

    public Neo4jSinkWriter(Neo4jConfig config) {
        this.config = config;
        initializeDriver();
    }

    private void initializeDriver() {
        if (driver == null) {
            driver = GraphDatabase.driver(
                    config.getUri(),
                    AuthTokens.basic(config.getUser(), config.getPassword()),
                    Config.builder()
                            .withMaxConnectionPoolSize(config.getPoolSize())
                            .build());
        }
        if (session == null || !session.isOpen()) {
            session = driver.session(config.getSessionConfig());
        }
    }

    @Override
    public void write(CypherStatement element, Context context) throws IOException {
        initializeDriver();
        batch.add( element);
        if (batch.size() >= config.getBatchSize()) {
            flush(false);
        }
    }


    @Override
    public void flush(boolean endOfInput) throws IOException {
        if (endOfInput && !batch.isEmpty()) {
            // 处理流结束时的剩余数据
            List<Neo4jCommittable> finalCommits = prepareCommit();
            if (!finalCommits.isEmpty()) {

                // 这里应该将最终提交传递给committer
                // 实际应用中需要调用committer的commit方法
            }
        }
    }

    @Override
    public List<Neo4jCommittable> prepareCommit() throws IOException {
        if (batch.isEmpty()) {
            return Collections.emptyList();
        }
        List<CypherStatement> toCommit = new ArrayList<>(batch);
        batch.clear();
        return Collections.singletonList(new Neo4jCommittable(toCommit));
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        for (CypherStatement stmt : batch) {
            checkpointedState.add(stmt);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        checkpointedState = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>("neo4j-statements", CypherStatement.class));

        if (context.isRestored()) {
            batch.clear();
            for (CypherStatement stmt : checkpointedState.get()) {
                batch.add(stmt);
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (session != null && session.isOpen()) {
            session.close();
        }
        if (driver != null) {
            driver.closeAsync();
        }
    }
}