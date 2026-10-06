package br.com.lucast.atlas_query_engine.core.executor;

import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.core.translator.SqlQuery;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import static org.assertj.core.api.Assertions.assertThat;

class JdbcQueryExecutorTest {
    @Test
    void shouldExecuteLiteralParametersContainingRegexReplacementCharacters() {
        var database = new EmbeddedDatabaseBuilder().setType(EmbeddedDatabaseType.H2).generateUniqueName(true).build();
        try {
            var executor = new JdbcQueryExecutor(new JdbcTemplate(database));
            String value = "$10\\path? O'Reilly";
            var result = executor.execute(new QueryRequest(),
                    new SqlQuery("SELECT CAST(? AS VARCHAR) AS value_text", List.of(value)));
            assertThat(result.getRows()).containsExactly(List.of(value));
        } finally {
            database.shutdown();
        }
    }
}
