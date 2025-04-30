package edu.harvard.dbmi.avillach.dictionaryetl.concept;

import edu.harvard.dbmi.avillach.dictionaryetl.loading.ColumnMeta;

public enum ConceptTypes {
    CATEGORICAL("categorical"),
    CONTINUOUS("continuous"),
    INTERIOR("Interior");
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

    public static String conceptType(boolean isCategorical) {
        return isCategorical ? CATEGORICAL.conceptType : CONTINUOUS.conceptType;
    }

    public static String conceptTypeFromColumnMeta(ColumnMeta columnMeta) {
        if (columnMeta == null) {
            return INTERIOR.conceptType;
        }

        return conceptType(columnMeta.categorical());
    }
}
