package edu.harvard.dbmi.avillach.dictionaryetl.testwrappers;

import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetModel;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public class TestFacetModel extends FacetModel implements Comparable<TestFacetModel> {

    public TestFacetModel(FacetModel from) {
        setFacetId(from.getFacetId());
        setFacetCategoryId(from.getFacetCategoryId());
        setDescription(from.getDescription());
        setName(from.getName());
        setDisplay(from.getDisplay());
        setParentId(from.getParentId());
    }

    @Override
    public int compareTo(@NotNull TestFacetModel o) {
        return (o.getName() + "::" + o.getDisplay() + "::" + o.getDescription())
            .compareTo(getName() + "::" + getDisplay() + "::" + getDescription());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        FacetModel that = (FacetModel) o;
        return Objects.equals(getName(), that.getName()) && Objects.equals(getDisplay(), that.getDisplay()) && Objects.equals(getDescription(), that.getDescription());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getDisplay(), getDescription());
    }
}
