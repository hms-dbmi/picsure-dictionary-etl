package edu.harvard.dbmi.avillach.dictionaryetl.facetloader;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ExpressionEntry {

    @JsonProperty("exactly")
    public String exactly;

    @JsonProperty("contains")
    public String contains;

    @JsonProperty("regex")
    public String regex;

    @JsonProperty("node")
    public Integer node; // zero-based, negatives allowed (-1 is last)

    public boolean isEmpty() {
        return (exactly == null || exactly.isBlank())
                && (contains == null || contains.isBlank())
                && (regex == null || regex.isBlank());
    }
}
