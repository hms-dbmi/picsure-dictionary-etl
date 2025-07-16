package edu.harvard.dbmi.avillach.dictionaryetl.metadata.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

public class JsonConceptMetadata {

    private String description;

    @JsonProperty("drs_uri")
    private String drsUri;

    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<>();

    public JsonConceptMetadata() {
        // Default constructor for Jackson
    }

    public JsonConceptMetadata(String description, String drsUri) {
        this.description = description;
        this.drsUri = drsUri;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getDrsUri() {
        return drsUri;
    }

    public void setDrsUri(String drsUri) {
        this.drsUri = drsUri;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        additionalProperties.put(name, value);
    }

    public Object getAdditionalProperty(String name) {
        return additionalProperties.get(name);
    }
}
