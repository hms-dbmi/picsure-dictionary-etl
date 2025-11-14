package edu.harvard.dbmi.avillach.dictionaryetl.facet.dto;

import java.util.List;

public record Result(int categoriesCreated, int categoriesUpdated, int facetsCreated, int facetsUpdated,
                     List<String> createdCategoryNames, List<FacetNameNested> createdFacetNames,
                     List<FacetMappingBreakdown> facetMappings) {
}
