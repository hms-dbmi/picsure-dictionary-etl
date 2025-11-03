package edu.harvard.dbmi.avillach.dictionaryetl.facet.dto;

import java.util.ArrayList;
import java.util.List;

public class FacetNameNested {
    public String name;
    public List<FacetNameNested> facets;
    public FacetNameNested() {
        this.facets = new ArrayList<>();
    }
    public FacetNameNested(String name) {
        this.name = name;
        this.facets = new ArrayList<>();
    }
}
