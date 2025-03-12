import hirson.sink.neo4j.Neo4jSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

public class Neo4jSinkTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 生成测试数据流
        String query = "MERGE (u:User {id: $id}) SET u.name = $name, u.age = $age";
//        String query = "MERGE (u:User {id: $id}) DELETE u";
        Map<String, Object> params1 = new HashMap<>();
        Map<String, Object> params2 = new HashMap<>();
        Map<String, Object> params3 = new HashMap<>();
        params1.put("id", "u1");
        params1.put("name", "666");
        params1.put("age", 30);

        params2.put("id", "u2");
        params2.put("name", "123");
        params2.put("age", 25);

        params3.put("id", "u3");
        params3.put("name", "777");
        params3.put("age", 35);

        Map<String, Object> cypherStatement1 = new HashMap<>();
        Map<String, Object> cypherStatement2 = new HashMap<>();
        Map<String, Object> cypherStatement3 = new HashMap<>();
        cypherStatement1.put("query",query);
        cypherStatement1.put("parameters", params1);
        cypherStatement2.put("query",query);
        cypherStatement2.put("parameters", params2);
        cypherStatement3.put("query",query);
        cypherStatement3.put("parameters", params3);

        DataStream<Map<String, Object>> cypherStream = env.fromElements(
                cypherStatement1,cypherStatement2,cypherStatement3
        );

        // 3. 转换为 Cypher 语句
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

        cypherStream.sinkTo(new Neo4jSink("bolt://localhost:7687", "neo4j", "12345678", 100));

        // 5. 执行任务
        env.execute("Flink Neo4j Sink Demo");
    }


}
