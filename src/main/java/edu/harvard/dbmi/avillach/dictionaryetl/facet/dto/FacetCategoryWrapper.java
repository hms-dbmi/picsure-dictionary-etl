package edu.harvard.dbmi.avillach.dictionaryetl.facet.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request wrapper used by the /api/facet/loader/load endpoint.
 * Accepts either "Facet_Category" (preferred) or "facetCategory".
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record FacetCategoryWrapper(
        @JsonProperty("Facet_Category")
        @JsonAlias({ "facetCategory", "FacetCategory" })
        FacetCategoryDTO facetCategory
) {
}