package br.com.lucast.atlas_query_engine.core.model;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class QueryRequest {

    private String connection;

    private String dataset;

    private String schema;

    private String table;

    private String alias;

    @NotNull
    private List<@NotBlank String> select = new ArrayList<>();

    @NotNull
    @Valid
    private List<ProjectionRequest> projections = new ArrayList<>();

    @NotNull
    private List<FilterRequest> filters = new ArrayList<>();

    @JsonIgnore
    private FilterNode filterTree = FilterGroupRequest.empty();

    @JsonIgnore
    private boolean structuredFilters;

    @NotNull
    @Valid
    private List<MetricRequest> metrics = new ArrayList<>();

    @NotNull
    private List<@NotBlank String> groupBy = new ArrayList<>();

    @NotNull
    @Valid
    private List<SortRequest> sort = new ArrayList<>();

    @NotNull
    @Valid
    private List<JoinRequest> joins = new ArrayList<>();

    @Min(1)
    private int page = 1;

    @Min(1)
    @Max(500)
    private int pageSize = 50;

    public String getDataset() {
        return dataset;
    }

    public String getConnection() {
        return connection;
    }

    public void setConnection(String connection) {
        this.connection = connection;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public List<String> getSelect() {
        return select;
    }

    public void setSelect(List<String> select) {
        this.select = select == null ? new ArrayList<>() : new ArrayList<>(select);
    }

    public List<ProjectionRequest> getProjections() {
        return projections;
    }

    public void setProjections(List<ProjectionRequest> projections) {
        this.projections = projections == null ? new ArrayList<>() : new ArrayList<>(projections);
    }

    @JsonIgnore
    public List<FilterRequest> getFilters() {
        return filters;
    }

    @JsonIgnore
    public void setFilters(List<FilterRequest> filters) {
        this.filters = filters == null ? new ArrayList<>() : new ArrayList<>(filters);
        this.structuredFilters = false;
        this.filterTree = null;
    }

    @JsonSetter("filters")
    public void setFiltersPayload(FilterNode filters) {
        this.filterTree = filters == null ? FilterGroupRequest.empty() : filters;
        this.filters = flattenSimpleFilters(this.filterTree);
        this.structuredFilters = true;
    }

    @JsonGetter("filters")
    public Object getFiltersPayload() {
        if (structuredFilters) {
            return getFilterTree();
        }
        return filters;
    }

    @JsonIgnore
    @NotNull
    @Valid
    public FilterNode getFilterTree() {
        if (!structuredFilters) {
            return FilterGroupRequest.and(filters);
        }
        return filterTree == null ? FilterGroupRequest.empty() : filterTree;
    }

    public void setFilterTree(FilterNode filterTree) {
        setFiltersPayload(filterTree);
    }

    public List<MetricRequest> getMetrics() {
        return metrics;
    }

    public void setMetrics(List<MetricRequest> metrics) {
        this.metrics = metrics == null ? new ArrayList<>() : new ArrayList<>(metrics);
    }

    public List<String> getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(List<String> groupBy) {
        this.groupBy = groupBy == null ? new ArrayList<>() : new ArrayList<>(groupBy);
    }

    public List<SortRequest> getSort() {
        return sort;
    }

    public void setSort(List<SortRequest> sort) {
        this.sort = sort == null ? new ArrayList<>() : new ArrayList<>(sort);
    }

    public List<JoinRequest> getJoins() {
        return joins;
    }

    public void setJoins(List<JoinRequest> joins) {
        this.joins = joins == null ? new ArrayList<>() : new ArrayList<>(joins);
    }

    public int getPage() {
        return page;
    }

    public void setPage(int page) {
        this.page = page;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    @JsonIgnore
    public boolean isDirectQuery() {
        return table != null && !table.isBlank();
    }

    @JsonIgnore
    public String getTargetName() {
        return dataset != null && !dataset.isBlank() ? dataset : table;
    }

    private List<FilterRequest> flattenSimpleFilters(FilterNode node) {
        List<FilterRequest> flattened = new ArrayList<>();
        collectSimpleFilters(node, flattened);
        return flattened;
    }

    private void collectSimpleFilters(FilterNode node, List<FilterRequest> flattened) {
        if (node instanceof FilterRequest filterRequest) {
            flattened.add(filterRequest);
            return;
        }
        if (node instanceof FilterGroupRequest groupRequest) {
            for (FilterNode condition : groupRequest.getConditions()) {
                collectSimpleFilters(condition, flattened);
            }
        }
    }
}
