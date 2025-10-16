package edu.harvard.dbmi.avillach.dictionaryetl.facetloader;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FacetExpressionDTO {
    // New simpler expression entry format. Only one of exactly/contains/regex is typically provided per entry.
    @JsonProperty("exactly")
    public String exactly;

    @JsonProperty("contains")
    public String contains;

    @JsonProperty("regex")
    public String regex;

    // Node position index within concept path nodes (zero-based; negative allowed)
    @JsonProperty("node")
    public Integer node;
}
