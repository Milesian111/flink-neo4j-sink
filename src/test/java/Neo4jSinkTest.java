import hirson.sink.neo4j.Neo4jSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class Neo4jSinkTest {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 生成测试数据流
        String query = "MERGE (u:User {id: $id}) SET u.name = $name, u.age = $age";
        Row keyRow1 = Row.of("id","name","age");
        Row valueRow1 = Row.of( "1001", "Alice", 18);

        Row cypherStatement1 =Row.of(query,keyRow1,valueRow1);

        DataStream<Row> cypherStream = env.fromElements(
                cypherStatement1
        );

        // 4. 写入 Neo4j

        cypherStream.sinkTo(new Neo4jSink("bolt://localhost:7687", "neo4j", "12345678", 100));

        // 5. 执行任务
        env.execute("Flink Neo4j Sink Demo");
    }


}
