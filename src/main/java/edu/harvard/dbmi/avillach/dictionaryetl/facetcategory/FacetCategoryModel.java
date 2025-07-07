package edu.harvard.dbmi.avillach.dictionaryetl.facetcategory;

import jakarta.persistence.*;

import java.util.Objects;

@Entity
@Table(name = "facet_category")
public class FacetCategoryModel {
    @Id
    @SequenceGenerator(name = "facetCategorySeq", sequenceName = "facet_category_facet_category_id_seq", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "facetCategorySeq")
    @Column(name = "facet_category_id")
    private Long facetCategoryId;

    @Column(name = "name")
    private String name;

    @Column(name = "display")
    private String display;

    @Column(name = "description")
    private String description;

    public FacetCategoryModel() {

    }

    public FacetCategoryModel(String name, String display, String description) {
        this.name = name;
        this.display = display;
        this.description = description;
    }

    public Long getFacetCategoryId() {
        return this.facetCategoryId;
    }

    public void setFacetCategoryId(Long facetCategory_id) {
        this.facetCategoryId = facetCategory_id;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String ref) {
        this.name = ref;
    }

    public String getDisplay() {
        return this.display;
    }

    public void setDisplay(String fullName) {
        this.display = fullName;
    }

    public String getDescription() {
        return this.description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return "{" +
                " facetCategoryId='" + getFacetCategoryId() + "'" +
                ", name='" + getName() + "'" +
                ", display='" + getDisplay() + "'" +
                ", description='" + getDescription() + "'" +
                "}";
    }

}
