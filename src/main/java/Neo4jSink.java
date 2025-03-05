import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.neo4j.driver.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Neo4jSink<T> extends RichSinkFunction<T> implements CheckpointedFunction, CheckpointListener {

    private transient Driver neo4jDriver;
    private transient Session session;
    private final Neo4jStatementBuilder<T> statementBuilder;
    private final String uri;
    private final String username;
    private final String password;
    private final TypeInformation<T> typeInformation;

    // 状态相关
    private transient ListState<T> checkpointedState;
    private List<T> bufferedElements = new ArrayList<>();
    private final int batchSize;

    public Neo4jSink(String uri,
                     String username,
                     String password,
                     int batchSize,
                     Neo4jStatementBuilder<T> statementBuilder,
                     TypeInformation<T> typeInformation) {
        this.uri = uri;
        this.username = username;
        this.password = password;
        this.batchSize = batchSize;
        this.statementBuilder = statementBuilder;
        this.typeInformation = typeInformation;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        neo4jDriver = GraphDatabase.driver(uri, AuthTokens.basic(username, password));
        session = neo4jDriver.session();
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        bufferedElements.add(value);
        if (bufferedElements.size() >= batchSize) {
            flush();
        }
    }

    private void flush() {
        if (!bufferedElements.isEmpty()) {
            try (Transaction tx = session.beginTransaction()) {
                for (T element : bufferedElements) {
                    CypherStatement statement = statementBuilder.build(element);
                    tx.run(statement.getQuery(), statement.getParameters());
                }
                tx.commit();
            }
            bufferedElements.clear();
        }
    }

    // CheckpointedFunction 实现
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        // 将当前缓冲数据添加到检查点状态
        for (T element : bufferedElements) {
            checkpointedState.add(element);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<T> descriptor =
                new ListStateDescriptor<>(
                        "neo4jSinkState",
                        typeInformation);

        checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            // 从检查点恢复时加载状态
            for (T element : checkpointedState.get()) {
                bufferedElements.add(element);
            }
        }
    }

    // CheckpointListener 实现
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // 可选：在检查点完成后执行额外操作
    }

    @Override
    public void close() throws Exception {
        // 最后刷新剩余数据
        flush();
        if (session != null) session.close();
        if (neo4jDriver != null) neo4jDriver.close();
        super.close();
    }

    @FunctionalInterface
    public interface Neo4jStatementBuilder<T> extends Serializable {
        CypherStatement build(T element);
    }
}
