package edu.harvard.dbmi.avillach.dictionaryetl.hydration;

public class ConceptNode {

    private String conceptPath;
    private String name;
    private ConceptNode parent;
    private ConceptNode child;

    public ConceptNode(String conceptPath, String name) {
        this.conceptPath = conceptPath;
        this.name = name;
    }

    public ConceptNode getChild() {
        return this.child;
    }

    public void setChild(ConceptNode child) {
        this.child = child;
    }

    public ConceptNode getParent() {
        return parent;
    }

    public void setParent(ConceptNode parent) {
        this.parent = parent;
    }

    public String getConceptPath() {
        return conceptPath;
    }

    public void setConceptPath(String conceptPath) {
        this.conceptPath = conceptPath;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
