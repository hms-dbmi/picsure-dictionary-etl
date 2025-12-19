package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import com.fasterxml.jackson.core.JsonProcessingException;
import edu.harvard.dbmi.avillach.dictionaryetl.Utility.ColumnMetaUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.loading.model.ColumnMeta;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ConceptMetadataModelMapper {

    private final ColumnMetaUtility columnMetaUtility;

    public ConceptMetadataModelMapper(ColumnMetaUtility columnMetaUtility) {
        this.columnMetaUtility = columnMetaUtility;
    }

    public ConceptMetadataModel fromColumnMeta(ColumnMeta columnMeta) {
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
