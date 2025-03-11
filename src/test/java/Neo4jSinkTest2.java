import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Neo4jSinkTest2 {
    public static void main(String[] args) throws Exception {
        // 创建执行环境并启用检查点
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000); // 启用检查点，确保精确一次处理


        HashMap<String, Object> parameters = new HashMap<>();
        parameters.put("name", "Alice");
        String query = "CREATE (n:Person {name: $name})";
        CypherStatement statement = new CypherStatement(query, parameters);

        DataStream<CypherStatement> stream = env.fromElements(statement);

        // 创建并使用 Sink
        Neo4jConfig config = new Neo4jConfig();
        config.setUri("bolt://localhost:7687");
        config.setUser("neo4j");
        config.setPassword("12345678");
        config.setBatchSize(500);

        // 4. 写入 Neo4j
        stream.sinkTo(new Neo4jSink(config));

        // 执行作业
        env.execute("Simple Sink Job");
    }
}
