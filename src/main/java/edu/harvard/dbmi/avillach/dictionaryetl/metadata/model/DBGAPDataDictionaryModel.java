package edu.harvard.dbmi.avillach.dictionaryetl.metadata.model;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Model class for parsing DBGap data dictionary XML files.
 */
public class DBGAPDataDictionaryModel {
    private String id;
    private String studyId;
    private String participantSet;
    private String dateCreated;
    private String description;
    private List<Variable> variables;

    public DBGAPDataDictionaryModel() {
        this.variables = new ArrayList<>();
    }

    public DBGAPDataDictionaryModel(String id, String studyId, String participantSet, String dateCreated,
                                    String description, List<Variable> variables) {
        this.id = id;
        this.studyId = studyId;
        this.participantSet = participantSet;
        this.dateCreated = dateCreated;
        this.description = description;
        this.variables = variables;
    }

    /**
     * Parse XML string into a DBGAPDictionaryModel
     * 
     * @param xmlString the XML string to parse
     * @return a new DBGAPDictionaryModel instance
     * @throws ParserConfigurationException if a DocumentBuilder cannot be created
     * @throws SAXException if any parse errors occur
     * @throws IOException if any I/O errors occur
     */
    public static DBGAPDataDictionaryModel fromXml(String xmlString)
            throws ParserConfigurationException, SAXException, IOException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(new InputSource(new StringReader(xmlString)));
        document.getDocumentElement().normalize();

        Element dataTableElement = document.getDocumentElement();

        // Parse data_table attributes
        String id = dataTableElement.getAttribute("id");
        String studyId = dataTableElement.getAttribute("study_id");
        String participantSet = dataTableElement.getAttribute("participant_set");
        String dateCreated = dataTableElement.getAttribute("date_created");

        // Parse description
        String description = "";
        NodeList descriptionNodes = dataTableElement.getElementsByTagName("description");
        if (descriptionNodes.getLength() > 0) {
            description = descriptionNodes.item(0).getTextContent();
        }

        // Parse variables
        List<Variable> variables = new ArrayList<>();
        NodeList variableNodes = dataTableElement.getElementsByTagName("variable");

        for (int i = 0; i < variableNodes.getLength(); i++) {
            Node variableNode = variableNodes.item(i);

            if (variableNode.getNodeType() == Node.ELEMENT_NODE) {
                Element variableElement = (Element) variableNode;

                String variableId = variableElement.getAttribute("id");

                NodeList nameNodes = variableElement.getElementsByTagName("name");
                String name = nameNodes.getLength() > 0 ? nameNodes.item(0).getTextContent() : "";

                NodeList varDescNodes = variableElement.getElementsByTagName("description");
                String varDescription = varDescNodes.getLength() > 0 ? varDescNodes.item(0).getTextContent() : "";

                NodeList typeNodes = variableElement.getElementsByTagName("type");
                String type = typeNodes.getLength() > 0 ? typeNodes.item(0).getTextContent() : "";

                // Parse values if present
                Map<String, String> values = new HashMap<>();
                NodeList valueNodes = variableElement.getElementsByTagName("value");

                for (int j = 0; j < valueNodes.getLength(); j++) {
                    Node valueNode = valueNodes.item(j);

                    if (valueNode.getNodeType() == Node.ELEMENT_NODE) {
                        Element valueElement = (Element) valueNode;
                        String code = valueElement.getAttribute("code");
                        String valueText = valueElement.getTextContent();
                        values.put(code, valueText);
                    }
                }

                Variable variable = new Variable(variableId, name, varDescription, type, values);
                variables.add(variable);
            }
        }

        return new DBGAPDataDictionaryModel(id, studyId, participantSet, dateCreated, description, variables);
    }

    /**
     * Convert this DBGAPDictionaryModel to a JsonStudyMetadata
     * 
     * @return a JsonStudyMetadata instance
     */
    public JsonStudyMetadata toJsonStudyMetadata() {
        JsonStudyMetadata metadata = new JsonStudyMetadata();

        for (Variable variable : variables) {
            // Create a concept path based on the study and variable information
            String conceptPath = "\\" + studyId + "\\" + id + "\\" + variable.getName() + "\\";

            // Create metadata with description
            JsonConceptMetadata conceptMetadata = new JsonConceptMetadata(variable.getDescription(), "");

            // Add variable type as an additional property
            conceptMetadata.setAdditionalProperty("type", variable.getType());

            // Add values as additional properties if present
            if (!variable.getValues().isEmpty()) {
                conceptMetadata.setAdditionalProperty("values", variable.getValues());
            }

            JsonConcept concept = new JsonConcept(
                id,                  // dataset_ref
                variable.getName(),  // name
                variable.getName(),  // display (using name as display)
                conceptPath,         // concept_path
                conceptMetadata      // metadata
            );

            metadata.addConcept(concept);
        }

        return metadata;
    }

    // Getters and setters

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getStudyId() {
        return studyId;
    }

    public void setStudyId(String studyId) {
        this.studyId = studyId;
    }

    public String getParticipantSet() {
        return participantSet;
    }

    public void setParticipantSet(String participantSet) {
        this.participantSet = participantSet;
    }

    public String getDateCreated() {
        return dateCreated;
    }

    public void setDateCreated(String dateCreated) {
        this.dateCreated = dateCreated;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<Variable> getVariables() {
        return variables;
    }

    public void setVariables(List<Variable> variables) {
        this.variables = variables;
    }

    /**
     * Inner class representing a variable in the data dictionary
     */
    public static class Variable {
        private String id;
        private String name;
        private String description;
        private String type;
        private Map<String, String> values;

        public Variable() {
            this.values = new HashMap<>();
        }

        public Variable(String id, String name, String description, String type, Map<String, String> values) {
            this.id = id;
            this.name = name;
            this.description = description;
            this.type = type;
            this.values = values;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Map<String, String> getValues() {
            return values;
        }

        public void setValues(Map<String, String> values) {
            this.values = values;
        }
    }
}
