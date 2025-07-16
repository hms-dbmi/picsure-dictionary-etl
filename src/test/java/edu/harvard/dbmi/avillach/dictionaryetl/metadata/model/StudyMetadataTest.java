package edu.harvard.dbmi.avillach.dictionaryetl.metadata.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class StudyMetadataTest {

    @Test
    void testDeserializeStudyMetadata() throws JsonProcessingException {
        String json = "[\n" +
                "  {\n" +
                "    \"dataset_ref\": \"phs002694\",\n" +
                "    \"name\": \"BID\",\n" +
                "    \"display\": \"BID\",\n" +
                "    \"concept_path\": \"\\\\phs002694\\\\activ4a\\\\mechanistic\\\\1-Knight-PROTHROMBOTIC-ANTIPHOSPHOLIPID-ANTIBODIES-IN-COVID-19\\\\ACTIV4a_Biorepository_Antiphospholipid_Antibody_data_Data_dictionary\\\\ACTIV4a Biorepository Antiphospholipid Antibody data_upload\\\\BID\\\\\",\n" +
                "    \"metadata\": {\n" +
                "      \"description\": \"Biospecimen ID – provided by the CCC\",\n" +
                "      \"drs_uri\": \"\",\n" +
                "      \"dictionary_file\": \"ACTIV4a_Biorepository_Antiphospholipid_Antibody_data_Data_dictionary\"\n" +
                "    }\n" +
                "  },\n" +
                "  {\n" +
                "    \"dataset_ref\": \"phs002694\",\n" +
                "    \"name\": \"SAMPLE_TYPE\",\n" +
                "    \"display\": \"SAMPLE_TYPE\",\n" +
                "    \"concept_path\": \"\\\\phs002694\\\\activ4a\\\\mechanistic\\\\1-Knight-PROTHROMBOTIC-ANTIPHOSPHOLIPID-ANTIBODIES-IN-COVID-19\\\\ACTIV4a_Biorepository_Antiphospholipid_Antibody_data_Data_dictionary\\\\ACTIV4a Biorepository Antiphospholipid Antibody data_upload\\\\SAMPLE_TYPE\\\\\",\n" +
                "    \"metadata\": {\n" +
                "      \"description\": \"Sample type\",\n" +
                "      \"drs_uri\": \"\"\n" +
                "    }\n" +
                "  }\n" +
                "]";

        JsonStudyMetadata studyMetadata = JsonStudyMetadata.fromJson(json);

        assertNotNull(studyMetadata);
        assertEquals(2, studyMetadata.getConcepts().size());
        
        JsonConcept concept1 = studyMetadata.getConcepts().get(0);
        assertEquals("phs002694", concept1.getDatasetRef());
        assertEquals("BID", concept1.getName());
        assertEquals("Biospecimen ID – provided by the CCC", concept1.getMetadata().getDescription());
        assertEquals("ACTIV4a_Biorepository_Antiphospholipid_Antibody_data_Data_dictionary", 
                     concept1.getMetadata().getAdditionalProperty("dictionary_file"));
        
        JsonConcept concept2 = studyMetadata.getConcepts().get(1);
        assertEquals("phs002694", concept2.getDatasetRef());
        assertEquals("SAMPLE_TYPE", concept2.getName());
        assertEquals("Sample type", concept2.getMetadata().getDescription());
    }

    @Test
    void testSerializeStudyMetadata() throws JsonProcessingException {
        // Create study metadata with two concepts
        JsonConceptMetadata metadata1 = new JsonConceptMetadata("Description 1", "drs-uri-1");
        metadata1.setAdditionalProperty("dictionary_file", "dictionary_file_1");
        
        JsonConcept concept1 = new JsonConcept("dataset1", "name1", "Display 1", "\\path1\\", metadata1);
        
        JsonConceptMetadata metadata2 = new JsonConceptMetadata("Description 2", "drs-uri-2");
        metadata2.setAdditionalProperty("custom_field", "custom_value");
        
        JsonConcept concept2 = new JsonConcept("dataset2", "name2", "Display 2", "\\path2\\", metadata2);
        
        List<JsonConcept> concepts = new ArrayList<>();
        concepts.add(concept1);
        concepts.add(concept2);
        
        JsonStudyMetadata studyMetadata = new JsonStudyMetadata(concepts);
        
        // Serialize to JSON
        String json = studyMetadata.toJson();
        
        // Deserialize to verify
        JsonStudyMetadata deserializedStudyMetadata = JsonStudyMetadata.fromJson(json);
        
        assertEquals(2, deserializedStudyMetadata.getConcepts().size());
        
        JsonConcept deserializedConcept1 = deserializedStudyMetadata.getConcepts().get(0);
        assertEquals("dataset1", deserializedConcept1.getDatasetRef());
        assertEquals("name1", deserializedConcept1.getName());
        assertEquals("Description 1", deserializedConcept1.getMetadata().getDescription());
        assertEquals("dictionary_file_1", deserializedConcept1.getMetadata().getAdditionalProperty("dictionary_file"));
        
        JsonConcept deserializedConcept2 = deserializedStudyMetadata.getConcepts().get(1);
        assertEquals("dataset2", deserializedConcept2.getDatasetRef());
        assertEquals("name2", deserializedConcept2.getName());
        assertEquals("Description 2", deserializedConcept2.getMetadata().getDescription());
        assertEquals("custom_value", deserializedConcept2.getMetadata().getAdditionalProperty("custom_field"));
    }

    @Test
    void testValidateJson() {
        // Valid JSON
        String validJson = "[\n" +
                "  {\n" +
                "    \"dataset_ref\": \"phs002694\",\n" +
                "    \"name\": \"BID\",\n" +
                "    \"display\": \"BID\",\n" +
                "    \"concept_path\": \"\\\\path\\\\\",\n" +
                "    \"metadata\": {\n" +
                "      \"description\": \"Description\",\n" +
                "      \"drs_uri\": \"\"\n" +
                "    }\n" +
                "  }\n" +
                "]";
        
        assertTrue(JsonStudyMetadata.validateJson(validJson));
        
        // Invalid JSON - missing required field
        String invalidJson1 = "[\n" +
                "  {\n" +
                "    \"dataset_ref\": \"phs002694\",\n" +
                "    \"name\": \"BID\",\n" +
                "    \"display\": \"BID\",\n" +
                "    \"metadata\": {\n" +
                "      \"description\": \"Description\",\n" +
                "      \"drs_uri\": \"\"\n" +
                "    }\n" +
                "  }\n" +
                "]";
        
        assertFalse(JsonStudyMetadata.validateJson(invalidJson1));
        
        // Invalid JSON - missing required metadata field
        String invalidJson2 = "[\n" +
                "  {\n" +
                "    \"dataset_ref\": \"phs002694\",\n" +
                "    \"name\": \"BID\",\n" +
                "    \"display\": \"BID\",\n" +
                "    \"concept_path\": \"\\\\path\\\\\",\n" +
                "    \"metadata\": {\n" +
                "      \"description\": \"Description\"\n" +
                "    }\n" +
                "  }\n" +
                "]";
        
        assertFalse(JsonStudyMetadata.validateJson(invalidJson2));
        
        // Invalid JSON - not an array
        String invalidJson3 = "{\n" +
                "  \"dataset_ref\": \"phs002694\",\n" +
                "  \"name\": \"BID\",\n" +
                "  \"display\": \"BID\",\n" +
                "  \"concept_path\": \"\\\\path\\\\\",\n" +
                "  \"metadata\": {\n" +
                "    \"description\": \"Description\",\n" +
                "    \"drs_uri\": \"\"\n" +
                "  }\n" +
                "}";
        
        assertFalse(JsonStudyMetadata.validateJson(invalidJson3));
    }

    @Test
    void testIsValid() {
        // Valid study metadata
        JsonConceptMetadata validMetadata = new JsonConceptMetadata("Description", "drs-uri");
        JsonConcept validConcept = new JsonConcept("dataset", "name", "display", "\\path\\", validMetadata);
        JsonStudyMetadata validStudyMetadata = new JsonStudyMetadata(List.of(validConcept));
        
        assertTrue(validStudyMetadata.isValid());
        
        // Invalid - empty concepts
        JsonStudyMetadata emptyStudyMetadata = new JsonStudyMetadata(new ArrayList<>());
        assertFalse(emptyStudyMetadata.isValid());
        
        // Invalid - null metadata
        JsonConcept invalidConcept1 = new JsonConcept("dataset", "name", "display", "\\path\\", null);
        JsonStudyMetadata invalidStudyMetadata1 = new JsonStudyMetadata(List.of(invalidConcept1));
        assertFalse(invalidStudyMetadata1.isValid());
        
        // Invalid - null required field in concept
        JsonConcept invalidConcept2 = new JsonConcept(null, "name", "display", "\\path\\", validMetadata);
        JsonStudyMetadata invalidStudyMetadata2 = new JsonStudyMetadata(List.of(invalidConcept2));
        assertFalse(invalidStudyMetadata2.isValid());
        
        // Invalid - null required field in metadata
        JsonConceptMetadata invalidMetadata = new JsonConceptMetadata(null, "drs-uri");
        JsonConcept invalidConcept3 = new JsonConcept("dataset", "name", "display", "\\path\\", invalidMetadata);
        JsonStudyMetadata invalidStudyMetadata3 = new JsonStudyMetadata(List.of(invalidConcept3));
        assertFalse(invalidStudyMetadata3.isValid());
    }
}