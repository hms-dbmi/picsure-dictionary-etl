package edu.harvard.dbmi.avillach.dictionaryetl.metadata.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JsonConcept {

    @JsonProperty("dataset_ref")
    private String datasetRef;

    private String name;

    private String display;

    @JsonProperty("concept_path")
    private String conceptPath;

    @JsonProperty("metadata")
    private JsonConceptMetadata metadata;

    public JsonConcept() {
        // Default constructor for Jackson
    }

    public JsonConcept(String datasetRef, String name, String display, String conceptPath, JsonConceptMetadata metadata) {
        this.datasetRef = datasetRef;
        this.name = name;
        this.display = display;
        this.conceptPath = conceptPath;
        this.metadata = metadata;
    }

    public String getDatasetRef() {
        return datasetRef;
    }

    public void setDatasetRef(String datasetRef) {
        this.datasetRef = datasetRef;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDisplay() {
        return display;
    }

    public void setDisplay(String display) {
        this.display = display;
    }

    public String getConceptPath() {
        return conceptPath;
    }

    public void setConceptPath(String conceptPath) {
        this.conceptPath = conceptPath;
    }

    public JsonConceptMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(JsonConceptMetadata metadata) {
        this.metadata = metadata;
    }
}
