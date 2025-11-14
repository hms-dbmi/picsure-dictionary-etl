package edu.harvard.dbmi.avillach.dictionaryetl.facet.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/** Top-level category container for facets. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record FacetCategoryDTO(
        @JsonProperty("Name") @JsonAlias("name") String name,
        @JsonProperty("Display") @JsonAlias("display") String display,
        @JsonProperty("Description") @JsonAlias("description") String description,
        @JsonProperty("Facets") @JsonAlias("facets") List<FacetDTO> facets
) {
}