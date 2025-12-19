package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import com.fasterxml.jackson.core.JsonProcessingException;
import edu.harvard.dbmi.avillach.dictionaryetl.Utility.ColumnMetaUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptTypes;
import edu.harvard.dbmi.avillach.dictionaryetl.loading.model.ColumnMeta;
import edu.harvard.dbmi.avillach.dictionaryetl.loading.model.ConceptNode;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

@Component
public class ColumnMetaTreeBuilder {

    private final ConceptModelTree conceptModelTree;
    private final ColumnMetaUtility columnMetaUtility;
    private final ColumnMetaFlattener columnMetaFlattener;
    private final LoadingErrorRegistry loadingErrorRegistry;

    public ColumnMetaTreeBuilder(ConceptModelTree conceptModelTree, ColumnMetaUtility columnMetaUtility, ColumnMetaFlattener columnMetaFlattener, LoadingErrorRegistry loadingErrorRegistry) {
        this.conceptModelTree = conceptModelTree;
        this.columnMetaUtility = columnMetaUtility;
        this.columnMetaFlattener = columnMetaFlattener;
        this.loadingErrorRegistry = loadingErrorRegistry;
    }

    public void process(List<ColumnMeta> columnMetas) {
        try {
            addToTree(columnMetaFlattener.flatten(columnMetas));
        } catch (IllegalArgumentException e) {
            this.loadingErrorRegistry.addError(e.getMessage());
        }
    }

    private void addToTree(ColumnMeta columnMeta) {
        ConceptNode parent = this.conceptModelTree.getRoot();
        ConcurrentMap<String, ConceptNode> registry = this.conceptModelTree.getRegistry();
        String[] node = columnMeta.name().split("\\\\");
        StringBuilder currentPath = new StringBuilder();

        for (int i = 0; i < node.length; i++) {
            String conceptSegment = node[i];
            if (StringUtils.isBlank(conceptSegment)) {
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
                currentNode.setConceptMetadataModel(processMetadata(columnMeta));
            }

            parent = currentNode;
        }
    }

    private ConceptMetadataModel processMetadata(ColumnMeta columnMeta) {
        try {
            List<String> values = columnMeta.categoryValues();
            if (!columnMeta.categorical()) {
                values = List.of(String.valueOf(columnMeta.min()), String.valueOf(columnMeta.max()));
            }

            String valuesJson = this.columnMetaUtility.listToJson(values);
            return new ConceptMetadataModel("values", valuesJson);

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
