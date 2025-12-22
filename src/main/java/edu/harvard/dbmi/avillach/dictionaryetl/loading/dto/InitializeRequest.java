package edu.harvard.dbmi.avillach.dictionaryetl.loading.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record InitializeRequest(
        @JsonProperty("csvPath") String csvPath,
        @JsonProperty("errorDirectory") String errorDirectory,
        @JsonProperty("includeDefaultFacets") boolean includeDefaultFacets,
        @JsonProperty("clearDatabase") boolean clearDatabase,
        @JsonProperty("studies") List<String> studies,
        @JsonProperty("isBDC") boolean isBDC
) {
    public InitializeRequest(String csvPath,
                             String errorDirectory,
                             Boolean includeDefaultFacets,
                             Boolean clearDatabase) {
        this(csvPath, errorDirectory, includeDefaultFacets, clearDatabase, null, false);
    }
}
