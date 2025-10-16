package edu.harvard.dbmi.avillach.dictionaryetl.facetloader;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FacetCategoryDTO {
    @JsonProperty("Name")
    public String name;
    @JsonProperty("Display")
    public String display;
    @JsonProperty("Description")
    public String description;
    @JsonProperty("Facets")
    public List<FacetDTO> facets;
}
