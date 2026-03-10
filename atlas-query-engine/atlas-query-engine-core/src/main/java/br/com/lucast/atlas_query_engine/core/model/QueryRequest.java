package br.com.lucast.atlas_query_engine.core.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class QueryRequest {

    @NotBlank
    private String dataset;

    @NotNull
    private List<@NotBlank String> select = new ArrayList<>();

    @NotNull
    @Valid
    private List<FilterRequest> filters = new ArrayList<>();

    @NotNull
    @Valid
    private List<MetricRequest> metrics = new ArrayList<>();

    @NotNull
    private List<@NotBlank String> groupBy = new ArrayList<>();

    @NotNull
    @Valid
    private List<SortRequest> sort = new ArrayList<>();

    @Min(1)
    private int page = 1;

    @Min(1)
    @Max(500)
    private int pageSize = 50;
}
