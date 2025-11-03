package edu.harvard.dbmi.avillach.dictionaryetl.facet.dto;

public interface ConceptToFacetDTO {

    Long getFacetId();
    Long getConceptNodeId();
    String getFacetName();
    String getConceptPath();
}
