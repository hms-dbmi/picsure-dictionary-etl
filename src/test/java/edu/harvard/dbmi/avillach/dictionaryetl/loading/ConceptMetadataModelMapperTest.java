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
                null,
                false
        );
        List<ConceptMetadataModel> result = this.conceptMetadataModelMapper.fromColumnMeta(columnMeta);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("[\"10.0\",\"20.0\"]", result.getFirst().getValue());
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
                null,
                false
        );
        List<ConceptMetadataModel> result = this.conceptMetadataModelMapper.fromColumnMeta(columnMeta);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("[\"up\",\"down\",\"left\",\"right\"]", result.getFirst().getValue());
    }

    @Test
    void shouldIncludeIsTimestamp_whenTimestampIsTrue() {
        ColumnMeta columnMeta = new ColumnMeta(
                "test",
                "0",
                "1",
                true,
                List.of("2024-01-01", "2024-01-02"),
                null,
                null,
                null,
                null,
                null,
                null,
                true
        );
        List<ConceptMetadataModel> result = this.conceptMetadataModelMapper.fromColumnMeta(columnMeta);
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("values", result.get(0).getKey());
        assertEquals("is_timestamp", result.get(1).getKey());
        assertEquals("true", result.get(1).getValue());
    }

    @Test
    void shouldNotIncludeIsTimestamp_whenTimestampIsFalse() {
        ColumnMeta columnMeta = new ColumnMeta(
                "test",
                "0",
                "1",
                true,
                List.of("a", "b"),
                null,
                null,
                null,
                null,
                null,
                null,
                false
        );
        List<ConceptMetadataModel> result = this.conceptMetadataModelMapper.fromColumnMeta(columnMeta);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.stream().noneMatch(m -> "is_timestamp".equals(m.getKey())));
    }
}