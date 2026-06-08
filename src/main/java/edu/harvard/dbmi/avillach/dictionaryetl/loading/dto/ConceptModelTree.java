package edu.harvard.dbmi.avillach.dictionaryetl.loading.dto;

import edu.harvard.dbmi.avillach.dictionaryetl.loading.model.ConceptNode;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ConceptModelTree {

    // Registry maps the Full Path (String) to the Node object
    private final ConcurrentMap<String, ConceptNode> registry = new ConcurrentHashMap<>();

    // A placeholder root to hold the top-level paths (e.g., "\laboratory\")
    private final ConceptNode root;
    private static final String ROOT_ID = "ROOT";

    public ConceptModelTree() {
        this.root = new ConceptNode(ROOT_ID);
        this.registry.put(ROOT_ID, this.root);
    }

    public ConceptNode getRoot() {
        return root;
    }

    public ConcurrentMap<String, ConceptNode> getRegistry() {
        return registry;
    }

}

