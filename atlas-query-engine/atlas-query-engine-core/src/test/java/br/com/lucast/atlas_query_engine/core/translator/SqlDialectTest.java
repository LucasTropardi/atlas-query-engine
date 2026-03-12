package br.com.lucast.atlas_query_engine.core.translator;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SqlDialectTest {

    @Test
    void shouldQuoteIdentifiersPerDialect() {
        assertThat(new PostgresSqlDialect().quoteIdentifier("totalAmount")).isEqualTo("\"totalAmount\"");
        assertThat(new MySqlSqlDialect().quoteIdentifier("totalAmount")).isEqualTo("`totalAmount`");
        assertThat(new OracleSqlDialect().quoteIdentifier("totalAmount")).isEqualTo("\"totalAmount\"");
    }

    @Test
    void shouldRenderPaginationPerDialect() {
        assertThat(new PostgresSqlDialect().renderPagination(50, 10)).isEqualTo("LIMIT 50 OFFSET 10");
        assertThat(new MySqlSqlDialect().renderPagination(50, 10)).isEqualTo("LIMIT 50 OFFSET 10");
        assertThat(new OracleSqlDialect().renderPagination(50, 10))
                .isEqualTo("OFFSET 10 ROWS FETCH NEXT 50 ROWS ONLY");
    }
}
