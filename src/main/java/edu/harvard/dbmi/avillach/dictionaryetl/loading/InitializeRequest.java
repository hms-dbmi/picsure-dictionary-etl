package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record InitializeRequest(
        @JsonProperty("csvPath") String csvPath,
        @JsonProperty("errorDirectory") String errorDirectory,
        @JsonProperty("includeDefaultFacets") Boolean includeDefaultFacets,
        @JsonProperty("clearDatabase") Boolean clearDatabase,
        // Optional list of studies (dataset refs) to restrict processing; null or empty means process all
        @JsonProperty("studies") List<String> studies
) {
    // Backward-compatible convenience constructor (pre-existing 4-arg usage)
    public InitializeRequest(String csvPath,
                             String errorDirectory,
                             Boolean includeDefaultFacets,
                             Boolean clearDatabase) {
        this(csvPath, errorDirectory, includeDefaultFacets, clearDatabase, null);
    }
}
