package br.com.lucast.atlas_query_engine.core.translator;

public interface SqlDialect {

    String dialectName();

    String quoteIdentifier(String identifier);

    String qualifyTable(String schemaName, String tableName);

    String renderPagination(int limit, int offset);
}
