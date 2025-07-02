package edu.harvard.dbmi.avillach.dictionaryetl.testwrappers;

import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataModel;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;
import java.util.Objects;

public class TestConceptMetadataModel extends ConceptMetadataModel implements Comparable<TestConceptMetadataModel
    > {

    public TestConceptMetadataModel(ConceptMetadataModel from) {
        setConceptNodeId(from.getConceptNodeId());
        setKey(from.getValue());
        setValue(from.getValue());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ConceptMetadataModel model = (ConceptMetadataModel) o;
        return Objects.equals(getConceptNodeId(), model.getConceptNodeId()) && Objects.equals(getKey(), model.getKey()) && Objects.equals(getValue(), model.getValue());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getConceptNodeId(), getKey(), getValue());
    }


    @Override
    public int compareTo(@NotNull TestConceptMetadataModel o) {
        String other = o.getKey() + "::" + o.getValue() + "::" + o.getConceptNodeId();
        String me = getKey() + "::" + getValue() + "::" + getConceptNodeId();
        return me.compareTo(other);
    }
}
