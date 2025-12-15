package edu.harvard.dbmi.avillach.dictionaryetl.loading.model;

import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptTypes;
import edu.harvard.dbmi.avillach.dictionaryetl.loading.ColumnMeta;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class ConcurrentFullPathTree {

    // Registry maps the Full Path (String) to the Node object
    public final ConcurrentMap<String, ConceptNode> registry = new ConcurrentHashMap<>();

    // A virtual root to hold the top-level paths (e.g., "\laboratory\")
    private final ConceptNode root;
    private static final String ROOT_ID = "ROOT";

    public ConcurrentFullPathTree() {
        this.root = new ConceptNode(ROOT_ID);
        this.registry.put(ROOT_ID, this.root);
    }

    public void ingestColumnMeta(ColumnMeta columnMeta) {
        ConceptNode parent = this.root;
        String[] node = columnMeta.name().split("\\\\");
        StringBuilder currentPath = new StringBuilder();

        for (int i = 0; i < node.length; i++) {
            String conceptSegment = node[i];
            if (conceptSegment.isEmpty()) {
                continue;
            }

            if (currentPath.isEmpty()) {
                currentPath.append("\\");
            }

            currentPath.append(conceptSegment).append("\\");
            String conceptPath = currentPath.toString();

            ConceptNode currentNode = registry.get(conceptPath);
            if (currentNode == null) {
                ConceptModel conceptModel = new ConceptModel(
                            null,
                            conceptSegment,
                            conceptSegment,
                            ConceptTypes.conceptTypeFromColumnMeta(null), // All intermediate nodes are "Categorical" concepts
                            conceptPath,
                            null
                    );

                if (StringUtils.isBlank(node[1])) {
                    throw new IllegalStateException("Unable to derive dataset " + columnMeta.name());
                }

                currentNode = new ConceptNode(conceptPath, conceptModel, node[1]); // The second node is the dataset. Position 0 = ''
                ConceptNode existingNode = registry.putIfAbsent(conceptPath, currentNode);
                if (existingNode == null) {
                    parent.addChild(currentNode);
                } else {
                    currentNode = existingNode;
                }
            }

            if (i == node.length - 1) {
                currentNode.getConceptModel().setConceptType(ConceptTypes.conceptTypeFromColumnMeta(columnMeta));
                currentNode.setColumnMeta(columnMeta);
            }

            parent = currentNode;
        }
    }

    public ConceptNode getRoot() {
        return root;
    }

}

