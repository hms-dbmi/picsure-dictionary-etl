package edu.harvard.dbmi.avillach.dictionaryetl.facetloader.dto;

import java.util.List;

public record LoadAccum(List<String> createdCategoryNames,
                        List<FacetNameNested> createdFacetNames,
                        List<FacetMappingBreakdown> facetMappings) {
}
