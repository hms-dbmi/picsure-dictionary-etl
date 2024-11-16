package edu.harvard.dbmi.avillach.dictionaryetl.fhir.model;

public class Entry {
    private String fullUrl;
    private ResearchStudy resource;

    public String getFullUrl() {
        return fullUrl;
    }

    public void setFullUrl(String fullUrl) {
        this.fullUrl = fullUrl;
    }

    public ResearchStudy getResource() {
        return resource;
    }

    public void setResource(ResearchStudy resource) {
        this.resource = resource;
    }
}
