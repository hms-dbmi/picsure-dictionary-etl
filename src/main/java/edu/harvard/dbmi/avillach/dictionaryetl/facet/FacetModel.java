package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import jakarta.persistence.*;

@Entity
@Table(name = "facet")
public class FacetModel {
    @Id
    @SequenceGenerator(name = "facetSeq", sequenceName = "facet_facet_id_seq", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "facetSeq")
    @Column(name = "facet_id")
    private Long facetId;

    @Column(name = "facet_category_id")
    private Long facetCategoryId;

    @Column(name = "name")
    private String name;

    @Column(name = "display")
    private String display;

    @Column(name = "description")
    private String description;

    @Column(name = "parent_id")
    private Long parentId;

    public FacetModel(Long facetCategoryId, String name, String display, String description, Long parentId) {
        this.facetCategoryId = facetCategoryId;
        this.name = name;
        this.display = display;
        this.description = description;
        this.parentId = parentId;
    }

    public FacetModel() {

    }

    public Long getFacetId() {
        return this.facetId;
    }

    public void setFacetId(Long facet_id) {
        this.facetId = facet_id;
    }

    public Long getFacetCategoryId() {
        return this.facetCategoryId;
    }

    public void setFacetCategoryId(Long ref) {
        this.facetCategoryId = ref;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String fullName) {
        this.name = fullName;
    }

    public String getDisplay() {
        return this.display;
    }

    public void setDisplay(String abbreviation) {
        this.display = abbreviation;
    }

    public String getDescription() {
        return this.description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Long getParentId() {
        return this.parentId;
    }

    public void setParentId(Long parent_id) {
        this.parentId = parent_id;
    }

    @Override
    public String toString() {
        return "{" +
                " facet_id='" + getFacetId() + "'" +
                ", ref='" + getFacetCategoryId() + "'" +
                ", full_name='" + getName() + "'" +
                ", abbreviation='" + getDisplay() + "'" +
                ", description='" + getDescription() + "'" +
                ", parent_id='" + getParentId() + "'" +
                "}";
    }

}
