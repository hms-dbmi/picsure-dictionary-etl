package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import jakarta.persistence.*;

@Entity
@Table(name = "facet_meta")
public class FacetMetadataModel {
    @Id
    @SequenceGenerator(name = "facetMetaSeq", sequenceName = "facet_meta_facet_meta_id_seq", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "facetMetaSeq")
    @Column(name = "facet_meta_id")
    private Long facetMetaId;

    @Column(name = "facet_id")
    private Long facetId;

    @Column(name = "key")
    private String key;

    @Column(name = "value")
    private String value;

    public FacetMetadataModel() {

    }

    public FacetMetadataModel(Long facetId, String key, String value) {
        this.facetId = facetId;
        this.key = key;
        this.value = value;
    }

    public Long getFacetMetaId() {
        return this.facetMetaId;
    }

    public Long getFacetId() {
        return this.facetId;
    }

    public void setFacetId(Long facetId) {
        this.facetId = facetId;
    }

    public String getKey() {
        return this.key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return this.value;
    }

    public void setValue(String value) {
        this.value = value;
    }

}
