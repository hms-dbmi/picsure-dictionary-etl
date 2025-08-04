package edu.harvard.dbmi.avillach.dictionaryetl.concept.csv;

import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetModel;

import java.util.List;

public record FacetsAndPairs(
    String category,
    List<NameDisplayCategory> facets,
    String conceptPath
) {}
