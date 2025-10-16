package edu.harvard.dbmi.avillach.dictionaryetl.facetloader;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FacetCategoryWrapper {
    @JsonProperty("Facet_Category")
    public FacetCategoryDTO facetCategory;
}
