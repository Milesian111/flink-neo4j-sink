import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

public class Neo4jSinkTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 生成测试数据流
        DataStream<User> userStream = env.fromElements(
                new User("u1", "Alice", 30),
                new User("u2", "Bob", 25),
                new User("u3", "Charlie", 35)
        );

        // 3. 转换为 Cypher 语句
        DataStream<CypherStatement> cypherStream = userStream.map(user -> {
            // 创建用户节点的 Cypher
            String query = "MERGE (u:User {id: $id}) SET u.name = $name, u.age = $age";
            Map<String, Object> params = new HashMap<>();
            params.put("id", user.getId());
            params.put("name", user.getName());
            params.put("age", user.getAge());
            return new CypherStatement(query, params);
        });

        // 4. 写入 Neo4j
        cypherStream.sinkTo(new Neo4jSink());

        // 5. 执行任务
        env.execute("Flink Neo4j Sink Demo");
    }


}
