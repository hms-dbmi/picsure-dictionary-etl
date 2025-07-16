package edu.harvard.dbmi.avillach.dictionaryetl.metadata.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * Class representing a study's metadata, containing a list of JsonConcepts.
 */
public class StudyMetadata {

    private List<JsonConcept> concepts;

    public StudyMetadata() {
        // Default constructor for Jackson
        this.concepts = new ArrayList<>();
    }

    public StudyMetadata(List<JsonConcept> concepts) {
        this.concepts = concepts;
    }

    public List<JsonConcept> getConcepts() {
        return concepts;
    }

    public void setConcepts(List<JsonConcept> concepts) {
        this.concepts = concepts;
    }

    public void addConcept(JsonConcept concept) {
        if (this.concepts == null) {
            this.concepts = new ArrayList<>();
        }
        this.concepts.add(concept);
    }

    /**
     * Validates that the StudyMetadata object can be properly serialized to JSON
     * and that all required fields are present.
     *
     * @return true if the object is valid, false otherwise
     */
    public boolean isValid() {
        if (concepts == null || concepts.isEmpty()) {
            return false;
        }

        for (JsonConcept concept : concepts) {
            if (concept.getDatasetRef() == null || concept.getName() == null ||
                concept.getDisplay() == null || concept.getConceptPath() == null ||
                concept.getMetadata() == null) {
                return false;
            }

            JsonConceptMetadata metadata = concept.getMetadata();
            if (metadata.getDescription() == null || metadata.getDrsUri() == null) {
                return false;
            }
        }

        return true;
    }

    /**
     * Validates that the provided JSON string represents a valid StudyMetadata object.
     *
     * @param json the JSON string to validate
     * @return true if the JSON is valid, false otherwise
     */
    public static boolean validateJson(String json) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(json);

            if (!rootNode.isArray()) {
                return false;
            }

            for (JsonNode conceptNode : rootNode) {
                if (!conceptNode.has("dataset_ref") || !conceptNode.has("name") ||
                    !conceptNode.has("display") || !conceptNode.has("concept_path") ||
                    !conceptNode.has("metadata")) {
                    return false;
                }

                JsonNode metadataNode = conceptNode.get("metadata");
                if (!metadataNode.has("description") || !metadataNode.has("drs_uri")) {
                    return false;
                }
            }

            // Try to deserialize to make sure the structure is correct
            JsonConcept[] concepts = mapper.readValue(json, JsonConcept[].class);

            return true;
        } catch (JsonProcessingException e) {
            return false;
        }
    }

    /**
     * Serializes this object to a JSON string.
     *
     * @return the JSON string representation of this object
     * @throws JsonProcessingException if serialization fails
     */
    public String toJson() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(concepts);
    }

    /**
     * Deserializes a JSON string to a StudyMetadata object.
     *
     * @param json the JSON string to deserialize
     * @return the StudyMetadata object
     * @throws JsonProcessingException if deserialization fails
     */
    public static StudyMetadata fromJson(String json) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        JsonConcept[] concepts = mapper.readValue(json, JsonConcept[].class);
        return new StudyMetadata(List.of(concepts));
    }
}
