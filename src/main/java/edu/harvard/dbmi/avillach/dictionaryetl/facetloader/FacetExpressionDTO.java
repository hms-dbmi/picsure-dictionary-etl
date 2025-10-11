package edu.harvard.dbmi.avillach.dictionaryetl.facetloader;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FacetExpressionDTO {
    @JsonProperty("Logic")
    public String logic;
    @JsonProperty("Regex")
    public String regex;
    // Node position index within concept path nodes (zero-based; negative allowed)
    @JsonProperty("Node_Position")
    public Integer nodePosition;
}
