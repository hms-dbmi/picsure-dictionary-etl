package edu.harvard.dbmi.avillach.dictionaryetl.fhir.model;

public class Link {
    private String relation;
    private String url;

    public String getRelation() {
        return relation;
    }

    public void setRelation(String relation) {
        this.relation = relation;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
