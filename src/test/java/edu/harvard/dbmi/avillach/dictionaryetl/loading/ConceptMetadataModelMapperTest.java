package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import edu.harvard.dbmi.avillach.dictionaryetl.Utility.ColumnMetaUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.loading.model.ColumnMeta;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = {ConceptMetadataModelMapper.class, ColumnMetaUtility.class})
class ConceptMetadataModelMapperTest {

    @Autowired
    ConceptMetadataModelMapper conceptMetadataModelMapper;

    @Test
    void shouldHaveMinMax_whenColumnMetaIsContinuous() {
        ColumnMeta columnMeta = new ColumnMeta(
                "test",
                "0",
                "1",
                false, // is continuous
                List.of(),
                10.0,
                20.0,
               null,
                null,
               null,
                null
        );
        ConceptMetadataModel conceptMetadataModel = this.conceptMetadataModelMapper.fromColumnMeta(columnMeta);
        assertNotNull(conceptMetadataModel);
        assertEquals("[\"10.0\",\"20.0\"]", conceptMetadataModel.getValue());
    }

    @Test
    void shouldHaveListOfValues_whenColumnMetaIsCategorical() {
        ColumnMeta columnMeta = new ColumnMeta(
                "test",
                "0",
                "1",
                true, // is categorical
                List.of("up", "down", "left", "right"),
                10.0,
                20.0,
                null,
                null,
                null,
                null
        );
        ConceptMetadataModel conceptMetadataModel = this.conceptMetadataModelMapper.fromColumnMeta(columnMeta);
        assertNotNull(conceptMetadataModel);
        assertEquals("[\"up\",\"down\",\"left\",\"right\"]", conceptMetadataModel.getValue());
    }
}