package hirson.sink.neo4j;

import java.io.Serializable;


/** example usage:
 *  user -> {
 *  String query = "MERGE (u:User {id: $id}) SET u.name = $name, u.age = $age";
 *  Map<String, Object> params = new HashMap<>();
 *  params.put("id", user.getId());
 *  params.put("name", user.getName());
 *  params.put("age", user.getAge());
 *  return new hirson.sink.neo4j.CypherStatement(query, params);
 * @param <T>
 */
public interface Neo4jStatementBuilder<T> extends Serializable {
    CypherStatement build(T element);
}