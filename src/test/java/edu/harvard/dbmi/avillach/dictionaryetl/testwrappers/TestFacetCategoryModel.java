package edu.harvard.dbmi.avillach.dictionaryetl.testwrappers;

import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryModel;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public class TestFacetCategoryModel extends FacetCategoryModel implements Comparable<TestFacetCategoryModel> {

    public TestFacetCategoryModel(FacetCategoryModel from) {
        setFacetCategoryId(from.getFacetCategoryId());
        setName(from.getName());
        setDescription(from.getDescription());
        setDisplay(from.getDisplay());
    }

    @Override
    public int compareTo(@NotNull TestFacetCategoryModel o) {
        return (o.getName() + "::" + o.getDisplay() + "::" + o.getDescription())
            .compareTo(getName() + "::" + getDisplay() + "::" + getDescription());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        FacetCategoryModel that = (FacetCategoryModel) o;
        return Objects.equals(getName(), that.getName()) && Objects.equals(getDisplay(), that.getDisplay()) && Objects.equals(getDescription(), that.getDescription());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getDisplay(), getDescription());
    }

}
