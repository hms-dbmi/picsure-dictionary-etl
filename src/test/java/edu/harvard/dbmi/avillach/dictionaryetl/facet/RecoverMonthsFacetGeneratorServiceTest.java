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

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class RecoverMonthsFacetGeneratorServiceTest {

    @Autowired
    private DatabaseCleanupUtility databaseCleanupUtility;

    @Autowired
    private RecoverMonthsFacetGeneratorService generatorService;

    @Autowired
    private DatasetRepository datasetRepository;

    @Autowired
    private ConceptService conceptService;

    @Autowired
    private FacetRepository facetRepository;

    @Autowired
    private FacetCategoryRepository facetCategoryRepository;

    @Autowired
    private FacetConceptRepository facetConceptRepository;

    @Container
    static final PostgreSQLContainer<?> databaseContainer = new PostgreSQLContainer<>("postgres:16")
        .withDatabaseName("testdb")
        .withUsername("testuser")
        .withPassword("testpass")
        .withCopyFileToContainer(
            MountableFile.forClasspathResource("schema.sql"),
            "/docker-entrypoint-initdb.d/schema.sql"
        );
    @Autowired
    private FacetMetadataRepository facetMetadataRepository;

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
    void generate_shouldDiscoverMonths_andLoadFacetsAndMappings() {
        // Seed dataset and concept nodes with paths containing (Inf|Noninf) followed by months
        DatasetModel dsAdult = datasetRepository.save(new DatasetModel("phs003436", "RECOVER Adult", "", ""));
        DatasetModel dsOther = datasetRepository.save(new DatasetModel("phs000000", "OTHER", "", ""));
        FacetCategoryModel consortiumFacetCat = facetCategoryRepository.save(new FacetCategoryModel("Consortium_Curated_Facets", "Consortium_Curated_Facets", null));
        FacetModel facetRecoverAdult = facetRepository.save(new FacetModel(consortiumFacetCat.getFacetCategoryId(), "RECOVER Adult Curated", "RECOVER Adult Curated", "RECOVER Adult Curated Description", null));
        facetMetadataRepository.save(new FacetMetadataModel(facetRecoverAdult.getFacetId(), FacetLoaderService.KEY_EFFECTIVE_EXPRESSIONS, "[{ \"exactly\": \"phs003463\", \"node\": 0 },{ \"regex\": \"(?i)RECOVER_Adult$\", \"node\": 1 }]"));

        // Matching RECOVER adult concepts
        ConceptModel c1 = new ConceptModel(dsAdult.getDatasetId(), "phs003436", "phs003436", "",
                "\\phs003436\\RECOVER_Adult\\biospecimens\\Inventory of Samples Collected\\ac_cptcoll\\Noninf\\9\\", null);
        ConceptModel c2 = new ConceptModel(dsAdult.getDatasetId(), "phs003436", "phs003436", "",
                "\\phs003436\\RECOVER_Adult\\flder_tier2\\chest_ct\\Qualitative Read\\chestct_reticular\\Inf\\12\\", null);
        ConceptModel c3 = new ConceptModel(dsAdult.getDatasetId(), "phs003436", "phs003436", "",
                "\\phs003436\\RECOVER_Adult\\flder_tier2\\echocardiogram_with_strain\\Echocardiogram\\rttestrain_aregurg\\Inf\\9\\", null);

        // Non-matching concept (no Inf/Noninf before last)
        ConceptModel cNo = new ConceptModel(dsOther.getDatasetId(), "phs000000", "phs000000", "",
                "\\phs000000\\SomeStudy\\something\\42\\", null);

        conceptService.save(c1);
        conceptService.save(c2);
        conceptService.save(c3);
        conceptService.save(cNo);

        // 1) Dry run to discover months
        GenerateRecoverMonthsRequest req = new GenerateRecoverMonthsRequest();
        req.pathPrefixRegex = "(?i)\\\\phs003436\\\\"; // scope to RECOVER Adult
        req.adultNodeRegex = "(?i)RECOVER_Adult$";
        req.studyId = "phs003436";
        req.dryRun = true;

        GenerateRecoverMonthsResponse dry = generatorService.generate(req);
        assertEquals("Consortium_Curated_Facets", dry.categoryName);
        assertTrue(dry.discoveredMonths.contains("9"));
        assertTrue(dry.discoveredMonths.contains("12"));
        assertEquals(2, dry.discoveredMonths.size());
        assertNull(dry.load);

        // 2) Actual generation (clear none), then verify facets and mappings
        req.dryRun = false;
        req.studyId = "phs003436";
        GenerateRecoverMonthsResponse out = generatorService.generate(req);
        assertNotNull(out.load);
        assertEquals("Generation complete.", out.message);

        // Facets should exist
        assertTrue(facetRepository.findByName("09m-post index").isPresent());
        assertTrue(facetRepository.findByName("12m-post index").isPresent());

        // Verify mappings for 09m and 12m facets
        Long nineFacetId = facetRepository.findByName("09m-post index").map(FacetModel::getFacetId).orElseThrow();
        Long twelveFacetId = facetRepository.findByName("12m-post index").map(FacetModel::getFacetId).orElseThrow();

        Optional<ConceptModel> c1Opt = conceptService.findByConcept(c1.getConceptPath());
        Optional<ConceptModel> c2Opt = conceptService.findByConcept(c2.getConceptPath());
        Optional<ConceptModel> c3Opt = conceptService.findByConcept(c3.getConceptPath());
        assertTrue(c1Opt.isPresent());
        assertTrue(c2Opt.isPresent());
        assertTrue(c3Opt.isPresent());

        List<FacetConceptModel> all = facetConceptRepository.findAll();
        assertFalse(all.isEmpty());

        // 09m facet should map c1 and c3
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(nineFacetId, c1Opt.get().getConceptNodeId()).isPresent());
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(nineFacetId, c3Opt.get().getConceptNodeId()).isPresent());
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(nineFacetId, c2Opt.get().getConceptNodeId()).isEmpty());

        // 12m facet should map c2 only
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(twelveFacetId, c2Opt.get().getConceptNodeId()).isPresent());
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(twelveFacetId, c1Opt.get().getConceptNodeId()).isEmpty());
    }
}
