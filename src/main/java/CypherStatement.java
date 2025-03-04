

import java.util.Map;

public class CypherStatement {
    private final String query;
    private final Map<String, Object> parameters;

    public CypherStatement(String query, Map<String, Object> parameters) {
        this.query = query;
        this.parameters = parameters;
    }

    // Getters

    public String getQuery() {
        return query;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }
}