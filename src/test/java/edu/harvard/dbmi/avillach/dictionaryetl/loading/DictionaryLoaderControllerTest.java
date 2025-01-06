package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import edu.harvard.dbmi.avillach.dictionaryetl.Utility.DatabaseCleanupUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptService;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetService;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetConceptService;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class DictionaryLoaderControllerTest {

    private static String filePath;
    private static String resourcePath;
    @Autowired
    private DatabaseCleanupUtility databaseCleanupUtility;

    @Autowired
    private DictionaryLoaderController dictionaryLoaderController;

    @Autowired
    private FacetConceptService facetConceptService;

    @Autowired
    private FacetService facetService;

    @Autowired
    private ConceptService conceptService;

    @Autowired
    private DatasetService datasetService;

    @Container
    static final PostgreSQLContainer<?> databaseContainer = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass")
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

    @BeforeAll
    public static void init() throws IOException {
        ClassPathResource columnMetaResource = new ClassPathResource("columnMeta.csv");
        assertNotNull(columnMetaResource);

        filePath = columnMetaResource.getFile().toPath().toString();
        Path testResourcePath = Paths.get(System.getProperty("user.dir"), "src", "test", "resources");
        resourcePath = testResourcePath.toString();
    }

    @BeforeEach
    void cleanDatabase() {
        this.databaseCleanupUtility.truncateTables();
    }

    @Test
    void initialDatabaseHydration_onlyDatasetNhanes_shouldMapFacets() {
        this.dictionaryLoaderController.initialDatabaseHydration(new InitializeRequest(
                "NHANES",
                        filePath,
                        resourcePath + "/columnMetaErrors.csv",
                        true,
                        true));


        List<DatasetModel> all = this.datasetService.findAll();
        assertFalse(all.isEmpty());
        assertEquals(1, all.size());
        assertEquals("NHANES", all.getFirst().getRef());

        Optional<FacetModel> categorical = this.facetService.findByName("categorical");
        Optional<FacetModel> continuous = this.facetService.findByName("continuous");
        assertTrue(categorical.isPresent());
        assertTrue(continuous.isPresent());

        List<ConceptModel> allConcepts = this.conceptService.findAll();
        assertFalse(allConcepts.isEmpty());

        // Verify all concepts have been appropriately mapped
        allConcepts.forEach(concept -> {
            Optional<FacetConceptModel> byFacetAndConcept;
            if (concept.getConceptType().equals("continuous")) {
                byFacetAndConcept = this.facetConceptService.findByFacetAndConcept(continuous.get().getFacetId(),
                        concept.getConceptNodeId());
            } else {
                byFacetAndConcept = this.facetConceptService.findByFacetAndConcept(categorical.get().getFacetId(),
                        concept.getConceptNodeId());
            }

            assertTrue(byFacetAndConcept.isPresent());
        });
    }
}