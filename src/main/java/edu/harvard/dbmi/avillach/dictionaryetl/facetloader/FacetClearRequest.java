package edu.harvard.dbmi.avillach.dictionaryetl.facetloader;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Payload for clearing facets and/or facet categories by name.
 * Expected JSON example:
 * {
 *   "Facet_Categories": ["CategoryA", "CategoryB"],
 *   "Facets": ["Facet1", "Facet2"]
 * }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FacetClearRequest {

    @JsonProperty("Facet_Categories")
    public List<String> facetCategories;

    @JsonProperty("Facets")
    public List<String> facets;
}
