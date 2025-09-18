package edu.harvard.dbmi.avillach.dictionaryetl.concept;

import com.opencsv.exceptions.CsvException;
import edu.harvard.dbmi.avillach.dictionaryetl.Utility.DatabaseCleanupUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import org.json.JSONArray;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

@Testcontainers
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@SpringBootTest
class CSVConceptLoaderTest {

    @Autowired
    ConceptService conceptService;

    @Autowired
    ConceptController conceptController;

    @Autowired
    private DatabaseCleanupUtility databaseCleanupUtility;

    @Autowired
    ConceptRepository conceptRepository;

    @Autowired
    private ConceptMetadataRepository conceptMetadataRepository;

    @Autowired
    DatasetRepository datasetRepository;


    @Container
    static final PostgreSQLContainer<?> databaseContainer = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass")
            .withUrlParam("currentSchema", "dict")
            .withCopyFileToContainer(
                    MountableFile.forClasspathResource("schema.sql"),
                    "/docker-entrypoint-initdb.d/schema.sql"
            );

    @DynamicPropertySource
    static void mySQLProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", databaseContainer::getJdbcUrl);
        registry.add("spring.datasource.username", databaseContainer::getUsername);
        registry.add("spring.datasource.password", databaseContainer::getPassword);
    }

    @BeforeEach
    void cleanDatabase() {
        this.databaseCleanupUtility.truncateTablesAllTables();
    }

    @Test
    void csvBatchConceptLoadTest() throws IOException, CsvException {
        //testing ingesting with hierarchy of ref1 -> ref2 (with categorical values) -> ref3 (with continuous values) with batch size of 2 (two batches)
        DatasetModel dataset = new DatasetModel("ref1", "REF1", "abv", "");
        datasetRepository.save(dataset);
        String csv = """
                dataset_ref,name,display,concept_type,concept_path,parent_concept_path,values,desc
                ref1,ref1,ref1,Categorical,\\\\ref1\\\\,,,
                ref1,concept1,display1,Categorical,\\\\ref1\\\\concept1\\\\,\\\\ref1\\\\,"[\\"vals1\\", 0.2, \\"vals3\\"]",ipsum
                ref1,concept2,display2,Continuous,\\\\ref1\\\\concept1\\\\concept2\\\\,\\\\ref1\\\\concept1\\\\,"[0,0]",ipsum2
                """;

        ResponseEntity<Object> updateResponse = conceptController.updateConceptsFromCSV("ref1", csv);
        Assertions.assertSame(HttpStatus.OK, updateResponse.getStatusCode(), "Response Entity not as expected for CSV Loader call");

        List<ConceptModel> conceptList = conceptService.findAll();
        //verify that there are three concepts in the db
        Assertions.assertEquals(3, conceptList.size(), "Concept list unexpected size");
        //verify that the ref1 concept exists, has no parent, and has no metadata fields present
        Optional<List<ConceptModel>> optRef1 = conceptRepository.findByName("ref1");
        Assertions.assertTrue(optRef1.isPresent(), "Top level concept not present after load");
        ConceptModel ref1 = optRef1.get().getFirst();
        Assertions.assertNull(ref1.getParentId(), "Parent id for top level concept not null");
        Assertions.assertTrue(conceptMetadataRepository.findByConceptNodeId(ref1.getConceptNodeId()).isEmpty(), "Metadata exists for concept without meta entries");

        //verify that concept1 exists, is categorical, has the parent ref1, that the values and desc meta exist, and that the values are a parseable array
        Optional<List<ConceptModel>> optConcept1 = conceptRepository.findByName("concept1");
        Assertions.assertTrue(optConcept1.isPresent(), "Concept1 (mid-level concept) does not exist after load");
        ConceptModel concept1 = optConcept1.get().getFirst();
        Assertions.assertSame(concept1.getParentId(), ref1.getConceptNodeId(), "Concept1 (mid-level concept) parent id does not match top level concept");
        Assertions.assertEquals("Categorical", concept1.getConceptType(), "Concept1 (mid-level concept) not marked as Categorical");
        List<ConceptMetadataModel> concept1Meta = conceptMetadataRepository.findByConceptNodeId(concept1.getConceptNodeId());
        Assertions.assertEquals(2, concept1Meta.size());
        Optional<ConceptMetadataModel> optConcept1Vals = conceptMetadataRepository.findByConceptNodeIdAndKey(concept1.getConceptNodeId(), "values");
        Assertions.assertTrue(optConcept1Vals.isPresent(), "Values key meta not present for concept1 (mid level concept)");
        Assertions.assertDoesNotThrow(() ->
                {
                    JSONArray concept1Vals = new JSONArray(optConcept1Vals.get().getValue());
                    Assertions.assertEquals("vals1", concept1Vals.getString(0), "Val1 for categorical concept concept1 is not as expected");
                    Assertions.assertEquals("0.2", concept1Vals.getString(1), "Val2 for categorical concept concept1 is not as expected");
                    Assertions.assertEquals("vals3", concept1Vals.getString(2), "Val3 for categorical concept concept1 is not as expected");
                }, "JSON Parsing issue for values for concept1(categorical)"
        );
        Assertions.assertDoesNotThrow(
                () -> Assertions.assertEquals("ipsum", conceptMetadataRepository.findByConceptNodeIdAndKey(concept1.getConceptNodeId(), "desc")
                    .get().getValue(), "Desc value not as expected for mid-level concept concept1"), "Desc meta key for mid-level concept concept1 does not exist");

        //verify that concept2 exists, is continuous, has the parent concept1, values and desc meta exist, and min/max values are parseable
        Assertions.assertTrue(conceptRepository.findByName("concept2").isPresent(), "Lowest level concept concept2 not present in db after load");
        ConceptModel concept2 = conceptRepository.findByName("concept2").get().getFirst();
        Assertions.assertSame(concept2.getParentId(), concept1.getConceptNodeId(), "Concept2 (low-level concept) parent id does not match mid level concept");
        Assertions.assertEquals("Continuous",  concept2.getConceptType(), "Concept2 (low-level concept) not marked as Continuous");
        List<ConceptMetadataModel> concept2Meta = conceptMetadataRepository.findByConceptNodeId(concept2.getConceptNodeId());
        Assertions.assertEquals(2, concept2Meta.size());
        Optional<ConceptMetadataModel> optConcept2Vals = conceptMetadataRepository.findByConceptNodeIdAndKey(concept2.getConceptNodeId(), "values");
        Assertions.assertTrue(optConcept2Vals.isPresent(), "Values key for concept 2(low level concept) is not present");
        Assertions.assertDoesNotThrow(() ->
                        {
                            JSONArray concept2Vals = new JSONArray(optConcept2Vals.get().getValue());
                            Assertions.assertEquals(0, concept2Vals.getDouble(0), "Min for continuous concept concept2 is not as expected");
                            Assertions.assertEquals(0, concept2Vals.getDouble(1), "Max for continuous concept concept2 is not as expected");
                        }, "JSON Parsing issue for values for concept2(continuous)"
                );
        Assertions.assertDoesNotThrow(
                        () -> Assertions.assertEquals("ipsum2", conceptMetadataRepository.findByConceptNodeIdAndKey(concept2.getConceptNodeId(), "desc")
                            .get().getValue(), "Desc value not as expected for low-level concept concept2"), "Desc meta key for low-level concept concept2 does not exist");
    }
}
