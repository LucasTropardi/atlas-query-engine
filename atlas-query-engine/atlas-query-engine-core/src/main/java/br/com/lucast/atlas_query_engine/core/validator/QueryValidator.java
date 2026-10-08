package br.com.lucast.atlas_query_engine.core.validator;

import br.com.lucast.atlas_query_engine.core.catalog.DatasetCatalog;
import br.com.lucast.atlas_query_engine.core.catalog.DatasetDefinition;
import br.com.lucast.atlas_query_engine.core.exception.DatasetNotFoundException;
import br.com.lucast.atlas_query_engine.core.exception.InvalidQueryException;
import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import java.util.Set;
import java.util.stream.Collectors;
import static br.com.lucast.atlas_query_engine.core.validator.QueryValidationRules.validateMetricGroupingConsistency;

public class QueryValidator {
    private final DatasetCatalog datasetCatalog;
    private final Validator validator;
    private final DirectQueryValidator directValidator = new DirectQueryValidator();
    private final DatasetQueryValidator datasetValidator = new DatasetQueryValidator();

    public QueryValidator(DatasetCatalog datasetCatalog, Validator validator) {
        this.datasetCatalog = datasetCatalog;
        this.validator = validator;
    }

    public DatasetDefinition validate(QueryRequest request) {
        Set<ConstraintViolation<QueryRequest>> violations = validator.validate(request);
        if (!violations.isEmpty()) {
            String message = violations.stream()
                    .map(violation -> violation.getPropertyPath() + " " + violation.getMessage())
                    .sorted()
                    .collect(Collectors.joining(", "));
            throw new InvalidQueryException(message);
        }

        if (request.getSelect().isEmpty() && request.getProjections().isEmpty() && request.getMetrics().isEmpty()) {
            throw new InvalidQueryException("Query must define at least one selected field or metric");
        }

        boolean hasDataset = request.getDataset() != null && !request.getDataset().isBlank();
        if (hasDataset == request.isDirectQuery()) {
            throw new InvalidQueryException("Query must define exactly one of dataset or table");
        }

        request.getOffset();
        HavingValidator.validate(request);
        validateDistinctSort(request);

        if (request.isDirectQuery()) {
            directValidator.validate(request);
            validateMetricGroupingConsistency(request);
            return null;
        }

        DatasetDefinition dataset = datasetCatalog.findByName(request.getDataset())
                .orElseThrow(() -> new DatasetNotFoundException(request.getDataset()));

        datasetValidator.validate(dataset, request);
        validateMetricGroupingConsistency(request);
        return dataset;
    }

    private void validateDistinctSort(QueryRequest request) {
        if (!request.isDistinct()) return;
        Set<String> selected = new java.util.HashSet<>(request.getSelect());
        request.getProjections().forEach(projection -> selected.add(projection.getAlias()));
        request.getMetrics().forEach(metric -> selected.add(metric.getAlias()));
        for (var sort : request.getSort()) {
            if (sort.getExpression() != null || !selected.contains(sort.getField())) {
                throw new InvalidQueryException("DISTINCT sort must reference a selected field or output alias");
            }
        }
    }
}
