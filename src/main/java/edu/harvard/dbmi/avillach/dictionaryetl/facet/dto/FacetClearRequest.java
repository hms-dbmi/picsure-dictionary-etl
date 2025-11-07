package edu.harvard.dbmi.avillach.dictionaryetl.facet.dto;

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
public record FacetClearRequest(@JsonProperty("Facet_Categories") List<String> facetCategories,
                               @JsonProperty("Facets") List<String> facets) {}
