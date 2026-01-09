package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptTypes;
import edu.harvard.dbmi.avillach.dictionaryetl.loading.dto.LoadingContext;
import edu.harvard.dbmi.avillach.dictionaryetl.loading.model.ColumnMeta;
import edu.harvard.dbmi.avillach.dictionaryetl.loading.model.ConceptNode;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

@Component
public class ColumnMetaTreeBuilder {

    private final ColumnMetaFlattener columnMetaFlattener;
    private final ConceptMetadataModelMapper conceptMetadataModelMapper;

    public ColumnMetaTreeBuilder(ColumnMetaFlattener columnMetaFlattener, ConceptMetadataModelMapper conceptMetadataModelMapper) {
        this.columnMetaFlattener = columnMetaFlattener;
        this.conceptMetadataModelMapper = conceptMetadataModelMapper;
    }

    public void process(List<ColumnMeta> columnMetas, LoadingContext context) {
        try {
            addToTree(columnMetaFlattener.flatten(columnMetas), context);
        } catch (IllegalArgumentException e) {
            context.loadingErrorRegistry().addError(e.getMessage());
        }
    }

    private void addToTree(ColumnMeta columnMeta, LoadingContext context) {
        ConceptNode parent = context.conceptModelTree().getRoot();
        ConcurrentMap<String, ConceptNode> registry = context.conceptModelTree().getRegistry();
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
                currentNode.setConceptMetadataModel(conceptMetadataModelMapper.fromColumnMeta(columnMeta));
            }

            parent = currentNode;
        }
    }

}
