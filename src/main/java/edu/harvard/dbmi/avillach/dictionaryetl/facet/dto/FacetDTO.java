package edu.harvard.dbmi.avillach.dictionaryetl.facet.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Facet definition.
 *
 * Semantics:
 * - If Expression_Groups present and non-empty: OR across groups, AND within each group.
 * - Else if Expressions present: AND across entries (legacy).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FacetDTO {

    @JsonProperty("Name") @JsonAlias("name")
    public String name;

    @JsonProperty("Display") @JsonAlias("display")
    public String display;

    @JsonProperty("Description") @JsonAlias("description")
    public String description;

    /** Legacy flat expressions (AND). */
    @JsonProperty("Expressions") @JsonAlias("expressions")
    public List<FacetExpressionDTO> expressions;

    /**
     * New grouped expressions.
     * Outer list OR, inner list AND.
     */
    @JsonProperty("Expression_Groups")
    @JsonAlias({ "expressionGroups", "ExpressionGroups" })
    public List<List<FacetExpressionDTO>> expressionGroups;

    /** Nested children. */
    @JsonProperty("Facets") @JsonAlias("facets")
    public List<FacetDTO> facets;
}