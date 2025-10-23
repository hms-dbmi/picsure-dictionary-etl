package edu.harvard.dbmi.avillach.dictionaryetl.facetloader.recover;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.harvard.dbmi.avillach.dictionaryetl.Utility.DatabaseCleanupUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptService;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class RecoverMonthsFacetControllerTest {

    @Autowired
    private DatabaseCleanupUtility databaseCleanupUtility;

    @Autowired
    private RecoverMonthsFacetController controller;

    @Autowired
    private DatasetRepository datasetRepository;

    @Autowired
    private ConceptService conceptService;

    @Autowired
    private FacetCategoryRepository facetCategoryRepository;

    @Autowired
    private ObjectMapper objectMapper;

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

    @BeforeEach
    void cleanDatabase() {
        this.databaseCleanupUtility.truncateTablesAllTables();
    }

    @Test
    void postGenerate_shouldReturn200_andCreateCategory() {
        // Seed minimal data
        DatasetModel dsAdult = datasetRepository.save(new DatasetModel("phs003436", "RECOVER Adult", "", ""));
        ConceptModel c1 = new ConceptModel(dsAdult.getDatasetId(), "phs003436", "phs003436", "",
                "\\phs003436\\RECOVER_Adult\\biospecimens\\Inventory of Samples Collected\\ac_cptcoll\\Noninf\\9\\", null);
        conceptService.save(c1);

        RecoverMonthsFacetGeneratorService.GenerateRecoverMonthsRequest req = new RecoverMonthsFacetGeneratorService.GenerateRecoverMonthsRequest();
        req.pathPrefixRegex = "(?i)\\\\RECOVER_Adult\\\\";
        req.dryRun = false;

        ResponseEntity<RecoverMonthsFacetGeneratorService.GenerateRecoverMonthsResponse> resp = controller.generate(req);
        assertEquals(200, resp.getStatusCode().value());
        assertNotNull(resp.getBody());
        assertEquals("Generation complete.", resp.getBody().message);
        assertTrue(facetCategoryRepository.findByName("Consortium_Curated_Facets").isPresent());
    }
}
