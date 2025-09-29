package edu.harvard.dbmi.avillach.dictionaryetl.metadata.model;

import org.junit.jupiter.api.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DBGapDictionaryModelTest {

    private static final String EXAMPLE_XML_1 = "<?xml-stylesheet type=\"text/xsl\" href=\"./datadict_v6.xsl\"?><data_table id=\"subjects.v6\" study_id=\"phs000571.v6\" participant_set=\"2\" date_created=\"Mon Dec 11 14:19:43 2023\"><description>The subject consent data table includes subject IDs, consent group information, and affection status for congenital heart defects (CHD).</description><variable id=\"subject_id\"><name>SUBJID</name><description>Subject ID</description><type>string</type></variable><variable id=\"consent\"><name>CONSENT</name><description>Consent group as determined by DAC</description><type>encoded value</type><value code=\"1\">Health/Medical/Biomedical (HMB)</value><value code=\"2\">Disease-Specific (Congenital Heart Disease) (DS-CHD)</value></variable></data_table>";
    private static final String EXAMPLE_XML_2 = "<?xml-stylesheet type=\"text/xsl\" href=\"./datadict_v2.xsl\"?><data_table id=\"pht003193.v5\" study_id=\"phs000571.v6\" participant_set=\"2\" date_created=\"Mon Sep 23 23:02:25 2019\"><description>This sample attributes data table includes body site where sample was collected, tumor status, analyte type, and DNA QC status.</description><variable id=\"phv00181091.v5\"><name>SAMPID</name><description>De-identified Sample ID</description><type>string</type></variable><variable id=\"phv00181092.v5\"><name>BODY_SITE</name><description>Body site where sample was collected</description><type>encoded values</type><value code=\"SAL\">Saliva</value><value code=\"WB\">Whole Blood</value></variable><variable id=\"phv00181093.v5\"><name>IS_TUMOR</name><description>Tumor status</description><type>encoded values</type><value code=\"N\">Is not a tumor</value><value code=\"Y\">Is Tumor</value></variable><variable id=\"phv00181094.v5\"><name>ANALYTE_TYPE</name><description>Analyte Type</description><type>string</type></variable><variable id=\"phv00181095.v5\"><name>DNA_QC_STATUS</name><description>DNA QC status</description><type>string</type></variable></data_table>";
    private static final String EXAMPLE_XML_3 = "<?xml-stylesheet type=\"text/xsl\" href=\"./datadict_v2.xsl\"?><data_table id=\"pht005679.v1\" study_id=\"phs000972.v4\" participant_set=\"1\" date_created=\"Fri Sep 27 08:05:23 2019\"><description>The subject consent data table contains subject IDs and consent group information.</description><variable id=\"phv00265837.v1\"><name>SUBJECT_ID</name><description>Subject ID</description><type>string</type></variable><variable id=\"phv00265838.v1\"><name>CONSENT</name><description>Consent group as determined by DAC</description><type>encoded value</type><value code=\"1\">General Research Use (IRB, PUB, COL, NPU, GSO) (GRU-IRB-PUB-COL-NPU-GSO)</value></variable></data_table>";

    @Test
    void testParseExample1() throws ParserConfigurationException, IOException, SAXException {
        DBGAPDataDictionaryModel model = DBGAPDataDictionaryModel.fromXml(EXAMPLE_XML_1);
        
        // Verify data_table attributes
        assertEquals("subjects.v6", model.getId());
        assertEquals("phs000571.v6", model.getStudyId());
        assertEquals("2", model.getParticipantSet());
        assertEquals("Mon Dec 11 14:19:43 2023", model.getDateCreated());
        
        // Verify description
        assertEquals("The subject consent data table includes subject IDs, consent group information, and affection status for congenital heart defects (CHD).", 
                    model.getDescription());
        
        // Verify variables
        List<DBGAPDataDictionaryModel.Variable> variables = model.getVariables();
        assertEquals(2, variables.size());
        
        // Verify first variable
        DBGAPDataDictionaryModel.Variable var1 = variables.get(0);
        assertEquals("subject_id", var1.getId());
        assertEquals("SUBJID", var1.getName());
        assertEquals("Subject ID", var1.getDescription());
        assertEquals("string", var1.getType());
        assertTrue(var1.getValues().isEmpty());
        
        // Verify second variable
        DBGAPDataDictionaryModel.Variable var2 = variables.get(1);
        assertEquals("consent", var2.getId());
        assertEquals("CONSENT", var2.getName());
        assertEquals("Consent group as determined by DAC", var2.getDescription());
        assertEquals("encoded value", var2.getType());
        
        Map<String, String> values = var2.getValues();
        assertEquals(2, values.size());
        assertEquals("Health/Medical/Biomedical (HMB)", values.get("1"));
        assertEquals("Disease-Specific (Congenital Heart Disease) (DS-CHD)", values.get("2"));
    }

    @Test
    void testParseExample2() throws ParserConfigurationException, IOException, SAXException {
        DBGAPDataDictionaryModel model = DBGAPDataDictionaryModel.fromXml(EXAMPLE_XML_2);
        
        // Verify data_table attributes
        assertEquals("pht003193.v5", model.getId());
        assertEquals("phs000571.v6", model.getStudyId());
        assertEquals("2", model.getParticipantSet());
        assertEquals("Mon Sep 23 23:02:25 2019", model.getDateCreated());
        
        // Verify description
        assertEquals("This sample attributes data table includes body site where sample was collected, tumor status, analyte type, and DNA QC status.", 
                    model.getDescription());
        
        // Verify variables
        List<DBGAPDataDictionaryModel.Variable> variables = model.getVariables();
        assertEquals(5, variables.size());
        
        // Verify BODY_SITE variable (index 1)
        DBGAPDataDictionaryModel.Variable bodySiteVar = variables.get(1);
        assertEquals("phv00181092.v5", bodySiteVar.getId());
        assertEquals("BODY_SITE", bodySiteVar.getName());
        assertEquals("Body site where sample was collected", bodySiteVar.getDescription());
        assertEquals("encoded values", bodySiteVar.getType());
        
        Map<String, String> values = bodySiteVar.getValues();
        assertEquals(2, values.size());
        assertEquals("Saliva", values.get("SAL"));
        assertEquals("Whole Blood", values.get("WB"));
    }

    @Test
    void testParseExample3() throws ParserConfigurationException, IOException, SAXException {
        DBGAPDataDictionaryModel model = DBGAPDataDictionaryModel.fromXml(EXAMPLE_XML_3);
        
        // Verify data_table attributes
        assertEquals("pht005679.v1", model.getId());
        assertEquals("phs000972.v4", model.getStudyId());
        assertEquals("1", model.getParticipantSet());
        assertEquals("Fri Sep 27 08:05:23 2019", model.getDateCreated());
        
        // Verify description
        assertEquals("The subject consent data table contains subject IDs and consent group information.", 
                    model.getDescription());
        
        // Verify variables
        List<DBGAPDataDictionaryModel.Variable> variables = model.getVariables();
        assertEquals(2, variables.size());
        
        // Verify CONSENT variable (index 1)
        DBGAPDataDictionaryModel.Variable consentVar = variables.get(1);
        assertEquals("phv00265838.v1", consentVar.getId());
        assertEquals("CONSENT", consentVar.getName());
        assertEquals("Consent group as determined by DAC", consentVar.getDescription());
        assertEquals("encoded value", consentVar.getType());
        
        Map<String, String> values = consentVar.getValues();
        assertEquals(1, values.size());
        assertEquals("General Research Use (IRB, PUB, COL, NPU, GSO) (GRU-IRB-PUB-COL-NPU-GSO)", values.get("1"));
    }

    @Test
    void testConvertToJsonStudyMetadata() throws ParserConfigurationException, IOException, SAXException {
        DBGAPDataDictionaryModel model = DBGAPDataDictionaryModel.fromXml(EXAMPLE_XML_1);
        JsonStudyMetadata metadata = model.toJsonStudyMetadata();
        
        // Verify concepts
        List<JsonConcept> concepts = metadata.getConcepts();
        assertEquals(2, concepts.size());
        
        // Verify first concept
        JsonConcept concept1 = concepts.get(0);
        assertEquals("subjects.v6", concept1.getDatasetRef());
        assertEquals("SUBJID", concept1.getName());
        assertEquals("SUBJID", concept1.getDisplay());
        assertEquals("\\phs000571.v6\\subjects.v6\\SUBJID\\", concept1.getConceptPath());
        
        JsonConceptMetadata conceptMetadata1 = concept1.getMetadata();
        assertEquals("Subject ID", conceptMetadata1.getDescription());
        assertEquals("", conceptMetadata1.getDrsUri());
        assertEquals("string", conceptMetadata1.getAdditionalProperty("type"));
        
        // Verify second concept
        JsonConcept concept2 = concepts.get(1);
        assertEquals("subjects.v6", concept2.getDatasetRef());
        assertEquals("CONSENT", concept2.getName());
        assertEquals("CONSENT", concept2.getDisplay());
        assertEquals("\\phs000571.v6\\subjects.v6\\CONSENT\\", concept2.getConceptPath());
        
        JsonConceptMetadata conceptMetadata2 = concept2.getMetadata();
        assertEquals("Consent group as determined by DAC", conceptMetadata2.getDescription());
        assertEquals("", conceptMetadata2.getDrsUri());
        assertEquals("encoded value", conceptMetadata2.getAdditionalProperty("type"));
        
        @SuppressWarnings("unchecked")
        Map<String, String> values = (Map<String, String>) conceptMetadata2.getAdditionalProperty("values");
        assertNotNull(values);
        assertEquals(2, values.size());
        assertEquals("Health/Medical/Biomedical (HMB)", values.get("1"));
        assertEquals("Disease-Specific (Congenital Heart Disease) (DS-CHD)", values.get("2"));
    }
}