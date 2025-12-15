package edu.harvard.dbmi.avillach.dictionaryetl.loading.model;

import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.loading.ColumnMeta;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ConceptNode {

    private String conceptPath;
    private ConceptModel conceptModel;
    private ColumnMeta columnMeta;

    private ConceptNode parent;
    private final ConcurrentMap<String, ConceptNode> children = new ConcurrentHashMap<>();

    public ConceptNode(String conceptPath) {
        this.conceptPath = conceptPath;
    }

    public ConceptNode(String conceptPath, ConceptModel conceptModel) {
        this.conceptPath = conceptPath;
        this.conceptModel = conceptModel;
    }

    public ConceptNode(String conceptPath, ConceptModel conceptModel, ConceptNode parent) {
        this.conceptPath = conceptPath;
        this.conceptModel = conceptModel;
        this.parent = parent;
    }

    public void addChild(ConceptNode conceptNode) {
        this.children.putIfAbsent(conceptNode.getConceptPath(), conceptNode);
        conceptNode.setParent(this); // sets reference to the parent incase we need to traverse up the tree
    }

    public String getConceptPath() {
        return conceptPath;
    }

    public void setConceptPath(String conceptPath) {
        this.conceptPath = conceptPath;
    }

    public ConceptModel getConceptModel() {
        return conceptModel;
    }

    public void setConceptModel(ConceptModel conceptModel) {
        this.conceptModel = conceptModel;
    }

    public ConceptNode getParent() {
        return parent;
    }

    public void setParent(ConceptNode parent) {
        this.parent = parent;
    }

    public ConcurrentMap<String, ConceptNode> getChildren() {
        return children;
    }

    public ColumnMeta getColumnMeta() {
        return columnMeta;
    }

    public void setColumnMeta(ColumnMeta columnMeta) {
        this.columnMeta = columnMeta;
    }

    @Override
    public String toString() {
        return "ConceptNode{" +
               "conceptPath='" + conceptPath + '\'' +
               ", conceptModel=" + conceptModel +
               ", parent=" + parent +
               ", children=" + children +
               '}';
    }
}