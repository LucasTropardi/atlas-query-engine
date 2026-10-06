package br.com.lucast.atlas_query_engine.core.catalog;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class InMemoryDatasetCatalogTest {
    @Test
    void shouldUseOnlySuppliedDefinitionsAndRejectDuplicateNames() {
        DatasetDefinition definition = new DatasetDefinition("custom", new SourceDefinition("test", "postgresql"),
                "public", "custom", Map.of(), Map.of(), List.of());
        var definitions = new ArrayList<>(List.of(definition));
        var catalog = new InMemoryDatasetCatalog(definitions);
        definitions.clear();
        assertThat(catalog.findByName("custom")).contains(definition);
        assertThat(catalog.findByName("orders")).isEmpty();
        assertThat(new InMemoryDatasetCatalog().findByName("custom")).isEmpty();
        assertThatThrownBy(() -> new InMemoryDatasetCatalog(List.of(definition, definition)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Duplicate dataset");
    }
}
