import hirson.sink.neo4j.CypherStatement;
import hirson.sink.neo4j.Neo4jSink;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

public class Neo4jSinkTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 生成测试数据流
        DataStream<User> userStream = env.fromElements(
                new User("u1", "aaa", 29),
                new User("u2", "bbb", 25),
                new User("u4", "ccc", 35)
        );

//        // 3. 转换为 Cypher 语句
//        DataStream<hirson.sink.neo4j.CypherStatement> cypherStream = userStream.map(user -> {
//            // 创建用户节点的 Cypher
//            String query = "MERGE (u:User {id: $id}) SET u.name = $name, u.age = $age";
//            Map<String, Object> params = new HashMap<>();
//            params.put("id", user.getId());
//            params.put("name", user.getName());
//            params.put("age", user.getAge());
//            return new hirson.sink.neo4j.CypherStatement(query, params);
//        });

        // 4. 写入 Neo4j
        userStream.addSink(new Neo4jSink<User>("bolt://localhost:7687",
                "neo4j",
                "12345678",
                100,
                user -> {
            // 创建用户节点的 Cypher
            String query = "MERGE (u:User {id: $id}) SET u.name = $name, u.age = $age";
            Map<String, Object> params = new HashMap<>();
            params.put("id", user.getId());
            params.put("name", user.getName());
            params.put("age", user.getAge());
            return new CypherStatement(query, params);
        },
                TypeInformation.of(User.class)
                ));

        // 5. 执行任务
        env.execute("Flink Neo4j Sink Demo");
    }


}
