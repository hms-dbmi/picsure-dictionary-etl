package edu.harvard.dbmi.avillach.dictionaryetl.facet.dto;

import java.util.List;

public record LeafFacetSpec(Long facetId, List<List<FacetExpressionDTO>> groups) {}

