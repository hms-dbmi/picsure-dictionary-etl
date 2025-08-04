package edu.harvard.dbmi.avillach.dictionaryetl.testwrappers;

import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetConceptModel;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public class TestFacetConceptModel extends FacetConceptModel implements Comparable<TestFacetConceptModel> {

    public TestFacetConceptModel(FacetConceptModel from) {
        setFacetConceptId(from.getFacetConceptId());
        setFacetId(from.getFacetId());
        setConceptNodeId(from.getConceptNodeId());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        FacetConceptModel that = (FacetConceptModel) o;
        return Objects.equals(getFacetId(), that.getFacetId()) && Objects.equals(getConceptNodeId(), that.getConceptNodeId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getFacetId(), getConceptNodeId());
    }

    @Override
    public int compareTo(@NotNull TestFacetConceptModel o) {
        return (getFacetId() + "::" + getConceptNodeId()).compareTo(o.getFacetId() + "::" + o.getConceptNodeId());
    }
}
