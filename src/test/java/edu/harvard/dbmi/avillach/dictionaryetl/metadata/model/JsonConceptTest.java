package edu.harvard.dbmi.avillach.dictionaryetl.metadata.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JsonConceptTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void testDeserializeJsonConcept() throws JsonProcessingException {
        String json = "{\n" +
                "    \"dataset_ref\": \"phs002694\",\n" +
                "    \"name\": \"BID\",\n" +
                "    \"display\": \"BID\",\n" +
                "    \"concept_path\": \"\\\\phs002694\\\\activ4a\\\\mechanistic\\\\1-Knight-PROTHROMBOTIC-ANTIPHOSPHOLIPID-ANTIBODIES-IN-COVID-19\\\\ACTIV4a_Biorepository_Antiphospholipid_Antibody_data_Data_dictionary\\\\ACTIV4a Biorepository Antiphospholipid Antibody data_upload\\\\BID\\\\\",\n" +
                "    \"metadata\": {\n" +
                "      \"description\": \"Biospecimen ID – provided by the CCC\",\n" +
                "      \"drs_uri\": \"\"\n" +
                "    }\n" +
                "  }";

        JsonConcept concept = mapper.readValue(json, JsonConcept.class);

        assertEquals("phs002694", concept.getDatasetRef());
        assertEquals("BID", concept.getName());
        assertEquals("BID", concept.getDisplay());
        assertEquals("\\phs002694\\activ4a\\mechanistic\\1-Knight-PROTHROMBOTIC-ANTIPHOSPHOLIPID-ANTIBODIES-IN-COVID-19\\ACTIV4a_Biorepository_Antiphospholipid_Antibody_data_Data_dictionary\\ACTIV4a Biorepository Antiphospholipid Antibody data_upload\\BID\\", concept.getConceptPath());
        assertNotNull(concept.getMetadata());
        assertEquals("Biospecimen ID – provided by the CCC", concept.getMetadata().getDescription());
        assertEquals("", concept.getMetadata().getDrsUri());
    }

    @Test
    void testDeserializeJsonConceptWithAdditionalMetadata() throws JsonProcessingException {
        String json = "{\n" +
                "    \"dataset_ref\": \"phs002694\",\n" +
                "    \"name\": \"BID\",\n" +
                "    \"display\": \"BID\",\n" +
                "    \"concept_path\": \"\\\\phs002694\\\\activ4a\\\\mechanistic\\\\1-Knight-PROTHROMBOTIC-ANTIPHOSPHOLIPID-ANTIBODIES-IN-COVID-19\\\\ACTIV4a_Biorepository_Antiphospholipid_Antibody_data_Data_dictionary\\\\ACTIV4a Biorepository Antiphospholipid Antibody data_upload\\\\BID\\\\\",\n" +
                "    \"metadata\": {\n" +
                "      \"description\": \"Biospecimen ID – provided by the CCC\",\n" +
                "      \"drs_uri\": \"\",\n" +
                "      \"dictionary_file\": \"ACTIV4a_Biorepository_Antiphospholipid_Antibody_data_Data_dictionary\",\n" +
                "      \"custom_field\": \"custom value\"\n" +
                "    }\n" +
                "  }";

        JsonConcept concept = mapper.readValue(json, JsonConcept.class);

        assertEquals("phs002694", concept.getDatasetRef());
        assertEquals("BID", concept.getName());
        assertEquals("BID", concept.getDisplay());
        assertEquals("\\phs002694\\activ4a\\mechanistic\\1-Knight-PROTHROMBOTIC-ANTIPHOSPHOLIPID-ANTIBODIES-IN-COVID-19\\ACTIV4a_Biorepository_Antiphospholipid_Antibody_data_Data_dictionary\\ACTIV4a Biorepository Antiphospholipid Antibody data_upload\\BID\\", concept.getConceptPath());
        assertNotNull(concept.getMetadata());
        assertEquals("Biospecimen ID – provided by the CCC", concept.getMetadata().getDescription());
        assertEquals("", concept.getMetadata().getDrsUri());
        assertEquals("ACTIV4a_Biorepository_Antiphospholipid_Antibody_data_Data_dictionary", concept.getMetadata().getAdditionalProperty("dictionary_file"));
        assertEquals("custom value", concept.getMetadata().getAdditionalProperty("custom_field"));
    }

    @Test
    void testSerializeJsonConcept() throws JsonProcessingException {
        JsonConceptMetadata metadata = new JsonConceptMetadata("Test Description", "test-drs-uri");
        metadata.setAdditionalProperty("dictionary_file", "test_dictionary_file");
        
        JsonConcept concept = new JsonConcept("test-dataset", "test-name", "Test Display", "\\test\\path\\", metadata);
        
        String json = mapper.writeValueAsString(concept);
        
        // Deserialize to verify the structure
        JsonConcept deserializedConcept = mapper.readValue(json, JsonConcept.class);
        
        assertEquals(concept.getDatasetRef(), deserializedConcept.getDatasetRef());
        assertEquals(concept.getName(), deserializedConcept.getName());
        assertEquals(concept.getDisplay(), deserializedConcept.getDisplay());
        assertEquals(concept.getConceptPath(), deserializedConcept.getConceptPath());
        assertEquals(concept.getMetadata().getDescription(), deserializedConcept.getMetadata().getDescription());
        assertEquals(concept.getMetadata().getDrsUri(), deserializedConcept.getMetadata().getDrsUri());
        assertEquals(concept.getMetadata().getAdditionalProperty("dictionary_file"), 
                     deserializedConcept.getMetadata().getAdditionalProperty("dictionary_file"));
    }
}