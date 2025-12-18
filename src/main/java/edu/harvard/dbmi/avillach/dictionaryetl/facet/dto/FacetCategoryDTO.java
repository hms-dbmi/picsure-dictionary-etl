package edu.harvard.dbmi.avillach.dictionaryetl.facet.dto;

import com.fasterxml.jackson.annotation.*;

import java.util.List;

/** Top-level category container for facets. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record FacetCategoryDTO(
        @JsonProperty("Name") @JsonAlias("name") String name,
        @JsonProperty("Display") @JsonAlias("display") String display,
        @JsonProperty("Description") @JsonAlias("description") String description,
        @JsonProperty("Facets") @JsonAlias("facets") List<FacetDTO> facets,
        @JsonProperty("Metadata") @JsonAlias("metadata") @JsonSetter(nulls = Nulls.AS_EMPTY) List<FacetCategoryMetaDTO> metadata
) {
    public FacetCategoryDTO(String name, String display, String description, List<FacetDTO> facets) {
        this(name, display, description, facets, List.of());
    }
}