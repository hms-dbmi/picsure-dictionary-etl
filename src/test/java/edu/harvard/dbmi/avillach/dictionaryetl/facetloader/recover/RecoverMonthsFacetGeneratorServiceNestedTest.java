package edu.harvard.dbmi.avillach.dictionaryetl.facetloader.recover;

import edu.harvard.dbmi.avillach.dictionaryetl.Utility.DatabaseCleanupUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptService;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facetloader.recover.RecoverMonthsFacetGeneratorService.GenerateRecoverMonthsRequest;
import edu.harvard.dbmi.avillach.dictionaryetl.facetloader.recover.RecoverMonthsFacetGeneratorService.GenerateRecoverMonthsResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class RecoverMonthsFacetGeneratorServiceNestedTest {

    @Autowired private DatabaseCleanupUtility cleanup;
    @Autowired private RecoverMonthsFacetGeneratorService service;
    @Autowired private DatasetRepository datasetRepository;
    @Autowired private ConceptService conceptService;
    @Autowired private FacetRepository facetRepository;
    @Autowired private FacetCategoryRepository categoryRepository;
    @Autowired private FacetConceptRepository facetConceptRepository;

    @Container
    static final PostgreSQLContainer<?> db = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass")
            .withCopyFileToContainer(MountableFile.forClasspathResource("schema.sql"),
                    "/docker-entrypoint-initdb.d/schema.sql");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", db::getJdbcUrl);
        r.add("spring.datasource.username", db::getUsername);
        r.add("spring.datasource.password", db::getPassword);
    }

    @BeforeEach
    void clean() { cleanup.truncateTablesAllTables(); }

    @Test
    void generate_shouldNestUnderParent_andScopeMappingsToRecoverAdult() {
        // Seed datasets
        DatasetModel dsAdult = datasetRepository.save(new DatasetModel("phs003436", "RECOVER Adult", "", ""));
        DatasetModel dsPeds = datasetRepository.save(new DatasetModel("phs003431", "RECOVER Pediatrics", "", ""));

        // Seed concepts: two RECOVER Adult with months; one Pediatrics with similar tail (should NOT match)
        ConceptModel a9 = new ConceptModel(dsAdult.getDatasetId(), "phs003436", "phs003436", "",
                "\\phs003436\\RECOVER_Adult\\biospecimens\\Inventory of Samples Collected\\ac_cptcoll\\Inf\\9\\", null);
        ConceptModel a12 = new ConceptModel(dsAdult.getDatasetId(), "phs003436", "phs003436", "",
                "\\phs003436\\RECOVER_Adult_v5\\visits\\Noninf\\12\\", null);
        ConceptModel p12 = new ConceptModel(dsPeds.getDatasetId(), "phs003431", "phs003431", "",
                "\\phs003431\\RECOVER_Pediatrics\\visits\\Inf\\12\\", null);
        conceptService.save(a9); conceptService.save(a12); conceptService.save(p12);

        // Generate (clear category first to ensure clean state)
        GenerateRecoverMonthsRequest req = new GenerateRecoverMonthsRequest();
        req.categoryName = "Consortium_Curated_Facets";
        req.clearCategoryFirst = true;
        GenerateRecoverMonthsResponse resp = service.generate(req);
        assertEquals("Generation complete.", resp.message);

        // Parent facet exists in category
        Optional<FacetModel> parentOpt = facetRepository.findByName("Recover Adult");
        assertTrue(parentOpt.isPresent());
        Long parentId = parentOpt.get().getFacetId();
        assertTrue(categoryRepository.findByName("Consortium_Curated_Facets").isPresent());

        // Children month facets exist and are nested
        Optional<FacetModel> m09 = facetRepository.findByName("09m-post index");
        Optional<FacetModel> m12 = facetRepository.findByName("12m-post index");
        assertTrue(m09.isPresent());
        assertTrue(m12.isPresent());
        assertEquals(parentId, m09.get().getParentId());
        assertEquals(parentId, m12.get().getParentId());

        // Mapping scoped: 09m contains a9; 12m contains a12; pediatric p12 excluded
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(m09.get().getFacetId(), a9.getConceptNodeId()).isPresent());
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(m12.get().getFacetId(), a12.getConceptNodeId()).isPresent());
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(m12.get().getFacetId(), p12.getConceptNodeId()).isEmpty());
    }
}
