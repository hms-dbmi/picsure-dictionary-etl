package edu.harvard.dbmi.avillach.dictionaryetl.concept;

public enum ConceptTypes {
    CATEGORICAL("categorical"),
    CONTINUOUS("continuous");
    private String conceptType;

    ConceptTypes(String conceptType) {
        this.conceptType = conceptType;
    }

    public String getConceptType() {
        return conceptType;
    }

    public void setConceptType(String conceptType) {
        this.conceptType = conceptType;
    }

    public String conceptType(boolean isCategorical) {
        return isCategorical ? CATEGORICAL.conceptType : CONTINUOUS.conceptType;
    }
}
