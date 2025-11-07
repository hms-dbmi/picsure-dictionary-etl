package edu.harvard.dbmi.avillach.dictionaryetl.facet.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Single matching rule. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record FacetExpressionDTO(
        @JsonProperty("exactly") String exactly,
        @JsonProperty("contains") String contains,
        @JsonProperty("regex") String regex,
        /** Node index. Supports negatives (e.g., -1 = last). */
        @JsonProperty("node") Integer node
) {
}