package edu.harvard.dbmi.avillach.dictionaryetl.concept;

import jakarta.persistence.*;

@Entity
public class ConceptStigvarIdentificationModel {
    private String id;
    private String display;
    private String description;
    @Id
    private String concept_path;
    private String value;
    private String phs;

    public ConceptStigvarIdentificationModel() {

    }

    public ConceptStigvarIdentificationModel(String id, String display, String description, String concept_path,
            String value, String phs) {
        this.id = id;
        this.display = display;
        this.description = description;
        this.concept_path = concept_path;
        this.value = value;
        this.phs = phs;
    }

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDisplay() {
        return this.display;
    }

    public void setDisplay(String display) {
        this.display = display;
    }

    public String getDescription() {
        return this.description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getConcept_path() {
        return this.concept_path;
    }

    public void setConcept_path(String concept_path) {
        this.concept_path = concept_path;
    }

    public String getValue() {
        return this.value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getPhs() {
        return this.phs;
    }

    public void setPhs(String phs) {
        this.phs = phs;
    }

    @Override
    public String toString() {
        return "\"" + getId() + "\"\t" +
                "\"" + getDisplay() + "\"\t" +
                "\"" + getDescription() + "\"\t" +
                "\"" + getConcept_path() + "\"\t" +
                "\"" + getValue() + "\"\t" +
                "\"" + getPhs() + "\"" +
                "\n";
    }
}
