package edu.harvard.dbmi.avillach.dictionaryetl.facetloader;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FacetDTO {
    @JsonProperty("Name")
    public String name;
    @JsonProperty("Display")
    public String display;
    @JsonProperty("Description")
    public String description;
    @JsonProperty("Study_ID")
    public String studyId;
    @JsonProperty("Facets")
    public List<FacetDTO> facets;
    @JsonProperty("Expressions")
    public List<FacetExpressionDTO> expressions;
}
