package edu.harvard.dbmi.avillach.dictionaryetl.facetcategory;

import jakarta.persistence.*;

@Entity
@Table(name = "facet_category_meta")
public class FacetCategoryMeta {

    @Id
    @SequenceGenerator(name = "facetCategoryMetaSeq", sequenceName = "facet_category_meta_facet_category_meta_id_seq", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "facetCategoryMetaSeq")
    @Column(name = "facet_category_meta_id")
    private Long facetCategoryMetaId;

    @Column(name = "facet_category_id")
    private Long facetCategoryId;

    @Column(name = "key")
    private String key;

    @Column(name = "value")
    private String value;

    public FacetCategoryMeta() {
    }

    public FacetCategoryMeta(Long facetCategoryMetaId, Long facetCategoryId, String key, String value) {
        this.facetCategoryMetaId = facetCategoryMetaId;
        this.facetCategoryId = facetCategoryId;
        this.key = key;
        this.value = value;
    }

    public Long getFacetCategoryMetaId() {
        return facetCategoryMetaId;
    }

    public void setFacetCategoryMetaId(Long facetCategoryMetaId) {
        this.facetCategoryMetaId = facetCategoryMetaId;
    }

    public Long getFacetCategoryId() {
        return facetCategoryId;
    }

    public void setFacetCategoryId(Long facetCategoryId) {
        this.facetCategoryId = facetCategoryId;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
