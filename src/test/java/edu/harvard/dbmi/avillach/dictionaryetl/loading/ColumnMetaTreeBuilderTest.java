package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import edu.harvard.dbmi.avillach.dictionaryetl.Utility.ColumnMetaUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptTypes;
import edu.harvard.dbmi.avillach.dictionaryetl.loading.dto.ConceptModelTree;
import edu.harvard.dbmi.avillach.dictionaryetl.loading.dto.LoadingErrorRegistry;
import edu.harvard.dbmi.avillach.dictionaryetl.loading.model.ConceptNode;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = {ColumnMetaTreeBuilder.class, ConceptModelTree.class, ColumnMetaFlattener.class, ConceptMetadataModelMapper.class, LoadingErrorRegistry.class, ColumnMetaUtility.class})
class ColumnMetaTreeBuilderTest {

    @Autowired
    ColumnMetaTreeBuilder columnMetaTreeBuilder;

    @Test
    void shouldHavePhvNameAndVariableDisplay_whenCompliantConceptPath() {
        String parentConceptPath = "\\phs\\pht\\phv123\\";
        String parentSegment = "phv123";
        ConceptModel parentConceptModel = new ConceptModel(
                null,
                parentSegment,
                parentSegment,
                ConceptTypes.conceptTypeFromColumnMeta(null),
                parentConceptPath,
                null
        );
        ConceptNode parentConceptNode = new ConceptNode(
                parentConceptPath,
                parentConceptModel,
                parentSegment
        );
        String childConceptPath = "\\phs\\pht\\phv123\\varName\\";
        String childSegment = "varName";
        ConceptModel tailConceptModal = new ConceptModel(
                null,
                childSegment,
                childSegment,
                ConceptTypes.conceptTypeFromColumnMeta(null),
                childConceptPath,
                null
        );

        ConceptNode childConceptNode = new ConceptNode(
                childConceptPath,
                tailConceptModal,
                childSegment
        );

        parentConceptNode.addChild(childConceptNode);
        this.columnMetaTreeBuilder.handleCompliantConceptSpecialCase(childConceptNode);
        assertEquals(parentSegment, childConceptNode.getConceptModel().getName());
    }

}