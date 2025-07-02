package edu.harvard.dbmi.avillach.dictionaryetl.testwrappers;

import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;

import java.util.Objects;

public class TestConceptModel extends ConceptModel {

    public TestConceptModel(ConceptModel model) {
        setConceptNodeId(model.getConceptNodeId());
        setDatasetId(model.getDatasetId());
        setName(model.getName());
        setDisplay(model.getDisplay());
        setConceptType(model.getConceptType());
        setConceptPath(model.getConceptPath());
        setParentId(model.getParentId());
    }
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ConceptModel that = (ConceptModel) o;
        return Objects.equals(getDatasetId(), that.getDatasetId()) && Objects.equals(getName(), that.getName()) && Objects.equals(getDisplay(), that.getDisplay()) && Objects.equals(getConceptType(), that.getConceptType()) && Objects.equals(getConceptPath(), that.getConceptPath());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getDatasetId(), getName(), getDisplay(), getConceptType(), getConceptPath());
    }
}
