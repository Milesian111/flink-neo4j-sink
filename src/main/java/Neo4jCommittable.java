import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Neo4jCommittable implements Serializable {
    private final List<CypherStatement> statements;

    public Neo4jCommittable(List<CypherStatement> statements) {
        this.statements = new ArrayList<>(statements); // 创建副本，避免外部修改
    }

    public List<CypherStatement> getStatements() {
        return statements;
    }
}