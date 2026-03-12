package br.com.lucast.atlas_query_engine.core.translator;

import br.com.lucast.atlas_query_engine.core.model.ColumnExpression;
import br.com.lucast.atlas_query_engine.core.model.ExistsFilterRequest;
import br.com.lucast.atlas_query_engine.core.model.FilterGroupRequest;
import br.com.lucast.atlas_query_engine.core.model.FilterOperator;
import br.com.lucast.atlas_query_engine.core.model.FilterRequest;
import br.com.lucast.atlas_query_engine.core.model.FunctionExpression;
import br.com.lucast.atlas_query_engine.core.model.JoinRequest;
import br.com.lucast.atlas_query_engine.core.model.LiteralExpression;
import br.com.lucast.atlas_query_engine.core.model.MetricOperation;
import br.com.lucast.atlas_query_engine.core.model.MetricRequest;
import br.com.lucast.atlas_query_engine.core.model.OperationExpression;
import br.com.lucast.atlas_query_engine.core.model.ProjectionRequest;
import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.core.model.SortDirection;
import br.com.lucast.atlas_query_engine.core.model.SortRequest;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DirectSqlTranslatorTest {

    private final DirectSqlTranslator translator = new DirectSqlTranslator();

    @Test
    void shouldTranslateDirectQueryWithJoinAndStructuredFilters() {
        QueryRequest request = new QueryRequest();
        request.setConnection("vetcare_pg");
        request.setSchema("public");
        request.setTable("customer_companies");
        request.setAlias("cc");
        request.setSelect(List.of(
                "cc.id",
                "cc.tutor_id",
                "cc.legal_name",
                "cc.trade_name",
                "cc.cnpj",
                "cc.phone",
                "cc.email",
                "cc.active",
                "cc.created_at",
                "cc.updated_at",
                "cca.zip_code",
                "cca.street",
                "cca.number",
                "cca.complement",
                "cca.neighborhood",
                "cca.city_name",
                "cca.city_ibge",
                "cca.state_uf",
                "cca.country"
        ));
        request.setJoins(List.of(
                new JoinRequest("public", "customer_company_address", "cca",
                        br.com.lucast.atlas_query_engine.core.model.JoinType.LEFT,
                        "cc.id", "cca.customer_company_id")
        ));
        request.setFilterTree(new FilterGroupRequest(
                br.com.lucast.atlas_query_engine.core.model.LogicalOperator.AND,
                List.of(
                        new FilterRequest("cc.active", FilterOperator.EQUALS, true),
                        new FilterRequest("cca.state_uf", FilterOperator.IN, List.of("SP", "RJ", "MG"))
                )
        ));
        request.setSort(List.of(new SortRequest("cc.legal_name", SortDirection.ASC)));
        request.setPage(1);
        request.setPageSize(100);

        SqlQuery sqlQuery = translator.translate(request, new PostgresSqlDialect());

        assertThat(sqlQuery.getSql()).isEqualTo(
                "SELECT \"cc\".\"id\", \"cc\".\"tutor_id\", \"cc\".\"legal_name\", \"cc\".\"trade_name\", "
                        + "\"cc\".\"cnpj\", \"cc\".\"phone\", \"cc\".\"email\", \"cc\".\"active\", "
                        + "\"cc\".\"created_at\", \"cc\".\"updated_at\", \"cca\".\"zip_code\", \"cca\".\"street\", "
                        + "\"cca\".\"number\", \"cca\".\"complement\", \"cca\".\"neighborhood\", "
                        + "\"cca\".\"city_name\", \"cca\".\"city_ibge\", \"cca\".\"state_uf\", \"cca\".\"country\" "
                        + "FROM public.customer_companies \"cc\" LEFT JOIN public.customer_company_address \"cca\" "
                        + "ON \"cc\".\"id\" = \"cca\".\"customer_company_id\" "
                        + "WHERE \"cc\".\"active\" = ? AND \"cca\".\"state_uf\" IN (?, ?, ?) "
                        + "ORDER BY \"cc\".\"legal_name\" ASC LIMIT 100 OFFSET 0"
        );
        assertThat(sqlQuery.getParameters()).containsExactly(true, "SP", "RJ", "MG");
    }

    @Test
    void shouldTranslateProjectionMetricExpressionAndExistsFilter() {
        QueryRequest request = new QueryRequest();
        request.setTable("customer_companies");
        request.setAlias("cc");
        request.setProjections(List.of(
                new ProjectionRequest(
                        "display_name",
                        new FunctionExpression("coalesce", List.of(
                                new ColumnExpression("cc.trade_name"),
                                new ColumnExpression("cc.legal_name")
                        ))
                )
        ));
        MetricRequest metric = new MetricRequest();
        metric.setOperation(MetricOperation.SUM);
        metric.setAlias("score");
        metric.setExpression(new OperationExpression("+", List.of(
                new FunctionExpression("coalesce", List.of(
                        new ColumnExpression("cc.id"),
                        new LiteralExpression(0)
                )),
                new LiteralExpression(10)
        )));
        request.setMetrics(List.of(metric));
        request.setFilterTree(new FilterGroupRequest(
                br.com.lucast.atlas_query_engine.core.model.LogicalOperator.AND,
                List.of(
                        new ExistsFilterRequest(
                                null,
                                "customer_company_address",
                                "cca",
                                br.com.lucast.atlas_query_engine.core.model.JoinType.INNER,
                                "cc.id",
                                "cca.customer_company_id",
                                List.of(),
                                FilterGroupRequest.empty()
                        )
                )
        ));
        SortRequest sortRequest = new SortRequest();
        sortRequest.setDirection(SortDirection.ASC);
        sortRequest.setExpression(new FunctionExpression("coalesce", List.of(
                new ColumnExpression("cc.trade_name"),
                new ColumnExpression("cc.legal_name")
        )));
        request.setSort(List.of(sortRequest));
        request.setPage(1);
        request.setPageSize(10);

        SqlQuery sqlQuery = translator.translate(request, new PostgresSqlDialect());

        assertThat(sqlQuery.getSql()).isEqualTo(
                "SELECT COALESCE(\"cc\".\"trade_name\", \"cc\".\"legal_name\") AS \"display_name\", "
                        + "SUM((COALESCE(\"cc\".\"id\", ?) + ?)) AS \"score\" "
                        + "FROM \"customer_companies\" \"cc\" "
                        + "WHERE EXISTS (SELECT 1 FROM \"customer_company_address\" \"cca\" "
                        + "WHERE \"cca\".\"customer_company_id\" = \"cc\".\"id\") "
                        + "ORDER BY COALESCE(\"cc\".\"trade_name\", \"cc\".\"legal_name\") ASC LIMIT 10 OFFSET 0"
        );
        assertThat(sqlQuery.getParameters()).containsExactly(0, 10);
    }
}
