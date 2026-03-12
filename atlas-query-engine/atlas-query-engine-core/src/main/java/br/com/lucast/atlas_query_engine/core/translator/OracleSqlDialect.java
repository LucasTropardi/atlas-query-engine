package br.com.lucast.atlas_query_engine.core.translator;

public class OracleSqlDialect implements SqlDialect {

    @Override
    public String dialectName() {
        return "ORACLE";
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public String qualifyTable(String schemaName, String tableName) {
        return schemaName + "." + tableName;
    }

    @Override
    public String renderPagination(int limit, int offset) {
        return "OFFSET " + offset + " ROWS FETCH NEXT " + limit + " ROWS ONLY";
    }
}
