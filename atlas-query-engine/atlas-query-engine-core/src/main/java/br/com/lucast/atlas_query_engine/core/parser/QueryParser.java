package br.com.lucast.atlas_query_engine.core.parser;

import br.com.lucast.atlas_query_engine.core.model.FilterRequest;
import br.com.lucast.atlas_query_engine.core.model.MetricRequest;
import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.core.model.SortDirection;
import br.com.lucast.atlas_query_engine.core.model.SortRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class QueryParser {

    public QueryRequest parse(QueryRequest request) {
        QueryRequest normalized = new QueryRequest();
        normalized.setDataset(trimToNull(request.getDataset()));
        normalized.setSelect(normalizeStrings(request.getSelect()));
        normalized.setFilters(normalizeFilters(request.getFilters()));
        normalized.setMetrics(normalizeMetrics(request.getMetrics()));
        normalized.setGroupBy(normalizeStrings(request.getGroupBy()));
        normalized.setSort(normalizeSort(request.getSort()));
        normalized.setPage(request.getPage() <= 0 ? 1 : request.getPage());
        normalized.setPageSize(request.getPageSize() <= 0 ? 50 : request.getPageSize());
        return normalized;
    }

    private List<String> normalizeStrings(List<String> values) {
        List<String> normalized = new ArrayList<>();
        if (values == null) {
            return normalized;
        }
        for (String value : values) {
            normalized.add(trimToNull(value));
        }
        return normalized;
    }

    private List<FilterRequest> normalizeFilters(List<FilterRequest> filters) {
        List<FilterRequest> normalized = new ArrayList<>();
        if (filters == null) {
            return normalized;
        }
        for (FilterRequest filter : filters) {
            normalized.add(new FilterRequest(
                    trimToNull(filter.getField()),
                    filter.getOperator(),
                    filter.getValue()
            ));
        }
        return normalized;
    }

    private List<MetricRequest> normalizeMetrics(List<MetricRequest> metrics) {
        List<MetricRequest> normalized = new ArrayList<>();
        if (metrics == null) {
            return normalized;
        }
        for (MetricRequest metric : metrics) {
            String field = trimToNull(metric.getField());
            String alias = trimToNull(metric.getAlias());
            if (alias == null && metric.getOperation() != null && field != null) {
                alias = metric.getOperation().getValue() + "_" + field;
            }
            normalized.add(new MetricRequest(field, metric.getOperation(), alias));
        }
        return normalized;
    }

    private List<SortRequest> normalizeSort(List<SortRequest> sortItems) {
        List<SortRequest> normalized = new ArrayList<>();
        if (sortItems == null) {
            return normalized;
        }
        for (SortRequest sortItem : sortItems) {
            SortDirection direction = Objects.requireNonNullElse(sortItem.getDirection(), SortDirection.ASC);
            normalized.add(new SortRequest(trimToNull(sortItem.getField()), direction));
        }
        return normalized;
    }

    private String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
