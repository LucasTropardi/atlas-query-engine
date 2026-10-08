package br.com.lucast.atlas_query_engine.demo.controller;

import br.com.lucast.atlas_query_engine.core.catalog.DatasetCatalog;
import br.com.lucast.atlas_query_engine.core.catalog.DatasetDefinition;
import br.com.lucast.atlas_query_engine.core.catalog.FieldType;
import br.com.lucast.atlas_query_engine.core.model.MetricOperation;
import java.util.Comparator;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/datasets")
public class DatasetController {
    private final DatasetCatalog catalog;

    public DatasetController(DatasetCatalog catalog) {
        this.catalog = catalog;
    }

    @GetMapping
    public List<Summary> list() {
        return catalog.findAll().stream().map(dataset -> new Summary(dataset.getName())).toList();
    }

    @GetMapping("/{name}")
    public Details describe(@PathVariable("name") String name) {
        DatasetDefinition dataset = catalog.findByName(name)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Dataset not found: " + name));
        List<Field> fields = dataset.getDimensions().values().stream()
                .map(field -> new Field(field.getLogicalName(), field.getFieldType(), field.isFilterable(), field.isSortable()))
                .sorted(Comparator.comparing(Field::name)).toList();
        List<Metric> metrics = dataset.getMetrics().values().stream()
                .map(metric -> new Metric(metric.getLogicalField(), metric.getFieldType(),
                        metric.getSupportedOperations().stream().sorted().toList()))
                .sorted(Comparator.comparing(Metric::field)).toList();
        return new Details(dataset.getName(), fields, metrics);
    }

    public record Summary(String name) {}
    public record Details(String name, List<Field> fields, List<Metric> metrics) {}
    public record Field(String name, FieldType type, boolean filterable, boolean sortable) {}
    public record Metric(String field, FieldType type, List<MetricOperation> operations) {}
}
