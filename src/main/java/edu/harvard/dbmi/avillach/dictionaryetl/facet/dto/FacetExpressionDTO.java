package edu.harvard.dbmi.avillach.dictionaryetl.facet.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Single matching rule. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FacetExpressionDTO {

    @JsonProperty("exactly")
    public String exactly;

    @JsonProperty("contains")
    public String contains;

    @JsonProperty("regex")
    public String regex;

    /** Node index. Supports negatives (e.g., -1 = last). */
    @JsonProperty("node")
    public Integer node;
}