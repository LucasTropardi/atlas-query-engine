package br.com.lucast.atlas_query_engine.core.executor;

import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcQueryExecutorTest {

    @Test
    void shouldRenderSqlWithParamsForDebugLogging() {
        String rendered = JdbcQueryExecutor.renderSqlWithParams(
                "SELECT * FROM orders WHERE status = ? AND id = ?",
                List.of("PAID", 10)
        );

        assertThat(rendered).isEqualTo("SELECT * FROM orders WHERE status = 'PAID' AND id = 10");
    }
}
