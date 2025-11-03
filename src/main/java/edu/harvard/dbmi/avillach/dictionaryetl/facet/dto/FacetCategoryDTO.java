package edu.harvard.dbmi.avillach.dictionaryetl.facet.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/** Top-level category container for facets. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FacetCategoryDTO {

    @JsonProperty("Name") @JsonAlias("name")
    public String name;

    @JsonProperty("Display") @JsonAlias("display")
    public String display;

    @JsonProperty("Description") @JsonAlias("description")
    public String description;

    @JsonProperty("Facets") @JsonAlias("facets")
    public List<FacetDTO> facets;
}