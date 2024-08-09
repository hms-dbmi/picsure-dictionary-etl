package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import jakarta.persistence.*;

public class FacetConceptModel {
    @Id
    @SequenceGenerator(name = "facetConceptMetaSeq", sequenceName = "facet__concept_node_facet__concept_node_id_seq", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "facetConceptMetaSeq")
    @Column(name = "facet__concept_node_id")
    private Long facetConceptId;

    @Column(name = "facet_id")
    private Long facetId;

    @Column(name = "concept_node_id")
    private Long conceptNodeId;

    public FacetConceptModel() {

    }

    public FacetConceptModel(Long facetId, Long conceptNodeId) {
        this.facetId = facetId;
        this.conceptNodeId = conceptNodeId;
    }

    public Long getFacetConceptId() {
        return this.facetConceptId;
    }

    public void setFacetConceptId(Long facetConceptId) {
        this.facetConceptId = facetConceptId;
    }

    public Long getFacetId() {
        return this.facetId;
    }

    public void setFacetId(Long facetId) {
        this.facetId = facetId;
    }

    public Long getConceptNodeId() {
        return this.conceptNodeId;
    }

    public void setConceptNodeId(Long conceptNodeId) {
        this.conceptNodeId = conceptNodeId;
    }

    @Override
    public String toString() {
        return "{" +
                " facetConceptId='" + getFacetConceptId() + "'" +
                ", facetId='" + getFacetId() + "'" +
                ", conceptNodeId='" + getConceptNodeId() + "'" +
                "}";
    }

}
