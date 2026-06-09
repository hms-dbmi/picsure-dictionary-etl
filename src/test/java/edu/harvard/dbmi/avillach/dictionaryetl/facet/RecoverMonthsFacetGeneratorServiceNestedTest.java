package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import edu.harvard.dbmi.avillach.dictionaryetl.Utility.DatabaseCleanupUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptService;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.model.FacetConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.model.FacetMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.model.FacetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.GenerateRecoverMonthsRequest;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.GenerateRecoverMonthsResponse;
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

    @Autowired
    private DatabaseCleanupUtility cleanup;
    @Autowired
    private RecoverMonthsFacetGeneratorService service;
    @Autowired
    private DatasetRepository datasetRepository;
    @Autowired
    private ConceptService conceptService;
    @Autowired
    private FacetRepository facetRepository;
    @Autowired
    private FacetCategoryRepository categoryRepository;
    @Autowired
    private FacetConceptRepository facetConceptRepository;
    @Autowired
    private FacetMetadataRepository facetMetadataRepository;

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
    void generate_shouldNestUnderInfectedOrNonInfected_basedOnDbFacetMembership() {
        // Seed datasets
        DatasetModel dsAdult = datasetRepository.save(new DatasetModel("phs003463", "RECOVER Adult", "", ""));
        DatasetModel dsPeds = datasetRepository.save(new DatasetModel("phs003431", "RECOVER Pediatrics", "", ""));

        // Seed required facet hierarchy
        FacetCategoryModel consortiumFacetCat = categoryRepository.save(
                new FacetCategoryModel("Consortium_Curated_Facets", "Consortium_Curated_Facets", null));
        FacetModel facetRecoverAdult = facetRepository.save(new FacetModel(
                consortiumFacetCat.getFacetCategoryId(), "RECOVER Adult Curated", "RECOVER Adult Curated",
                "RECOVER Adult Curated Description", null));
        facetMetadataRepository.save(new FacetMetadataModel(
                facetRecoverAdult.getFacetId(), FacetLoaderService.KEY_EFFECTIVE_EXPRESSION_GROUPS,
                "[[{ \"exactly\": \"phs003463\", \"node\": 0 },{ \"regex\": \"(?i)RECOVER_Adult$\", \"node\": 1 }]]"));

        FacetModel infectedFacet = facetRepository.save(new FacetModel(
                consortiumFacetCat.getFacetCategoryId(), "Infected", "Infected", "", facetRecoverAdult.getFacetId()));
        FacetModel nonInfectedFacet = facetRepository.save(new FacetModel(
                consortiumFacetCat.getFacetCategoryId(), "Non-infected", "Non-infected", "", facetRecoverAdult.getFacetId()));

        // Seed concepts
        ConceptModel a9 = new ConceptModel(dsAdult.getDatasetId(), "phs003463", "phs003463", "",
                "\\phs003463\\RECOVER_Adult\\biospecimens\\Inventory of Samples Collected\\ac_cptcoll\\Inf\\9\\", null);
        ConceptModel a12 = new ConceptModel(dsAdult.getDatasetId(), "phs003463", "phs003463", "",
                "\\phs003463\\RECOVER_Adult\\visits\\Noninf\\12\\", null);
        // Pediatric concept with a similar path tail — must be excluded
        ConceptModel p12 = new ConceptModel(dsPeds.getDatasetId(), "phs003431", "phs003431", "",
                "\\phs003431\\RECOVER_Pediatrics\\visits\\Inf\\12\\", null);
        conceptService.save(a9);
        conceptService.save(a12);
        conceptService.save(p12);

        // Map concepts to infection-group facets (source of truth for group assignment)
        facetConceptRepository.save(new FacetConceptModel(infectedFacet.getFacetId(), a9.getConceptNodeId()));
        facetConceptRepository.save(new FacetConceptModel(nonInfectedFacet.getFacetId(), a12.getConceptNodeId()));
        // p12 is intentionally NOT mapped to any infection-group facet

        // Generate
        GenerateRecoverMonthsRequest req = new GenerateRecoverMonthsRequest();
        GenerateRecoverMonthsResponse resp = service.generate(req);
        assertTrue(resp instanceof edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.GenerateRecoverMonthsSuccessResponse);
        edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.GenerateRecoverMonthsSuccessResponse ok =
                (edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.GenerateRecoverMonthsSuccessResponse) resp;
        assertEquals("Generation complete.", ok.message());

        // Generated facets exist with prefixed names
        Optional<FacetModel> inf09Opt = facetRepository.findByName("Infected 09m-post index");
        Optional<FacetModel> noninf12Opt = facetRepository.findByName("Non-infected 12m-post index");
        assertTrue(inf09Opt.isPresent(), "Infected 09m-post index should be created");
        assertTrue(noninf12Opt.isPresent(), "Non-infected 12m-post index should be created");

        // Parent IDs are set to the correct infection-group facet
        assertEquals(infectedFacet.getFacetId(), inf09Opt.get().getParentId(),
                "Infected 09m should be nested under Infected");
        assertEquals(nonInfectedFacet.getFacetId(), noninf12Opt.get().getParentId(),
                "Non-infected 12m should be nested under Non-infected");

        // Concept mappings are scoped correctly
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(
                inf09Opt.get().getFacetId(), a9.getConceptNodeId()).isPresent(),
                "a9 must be mapped to Infected 09m-post index");
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(
                noninf12Opt.get().getFacetId(), a12.getConceptNodeId()).isPresent(),
                "a12 must be mapped to Non-infected 12m-post index");

        // Pediatric concept p12 must not appear in any generated month facet
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(
                inf09Opt.get().getFacetId(), p12.getConceptNodeId()).isEmpty(),
                "p12 must not be in Infected 09m");
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(
                noninf12Opt.get().getFacetId(), p12.getConceptNodeId()).isEmpty(),
                "p12 must not be in Non-infected 12m");

        // RECOVER Adult Curated category still exists
        assertTrue(categoryRepository.findByName("Consortium_Curated_Facets").isPresent());
    }
}
