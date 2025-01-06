package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record InitializeRequest(
        @JsonProperty("datasetName") String datasetName,
        @JsonProperty("csvPath") String csvPath,
        @JsonProperty("errorDirectory") String errorDirectory,
        @JsonProperty("includeDefaultFacets") Boolean includeDefaultFacets,
        @JsonProperty("clearDatabase") Boolean clearDatabase
) {}
