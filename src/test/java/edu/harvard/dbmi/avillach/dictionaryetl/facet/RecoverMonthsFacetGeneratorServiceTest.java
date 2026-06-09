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
import edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.GenerateRecoverMonthsSuccessResponse;
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
        // Seed datasets
        DatasetModel dsAdult = datasetRepository.save(new DatasetModel("phs003463", "RECOVER Adult", "", ""));
        DatasetModel dsOther = datasetRepository.save(new DatasetModel("phs000000", "OTHER", "", ""));

        // Seed required facet category and prerequisite facets
        FacetCategoryModel consortiumFacetCat = facetCategoryRepository.save(
                new FacetCategoryModel("Consortium_Curated_Facets", "Consortium_Curated_Facets", null));
        FacetModel facetRecoverAdult = facetRepository.save(new FacetModel(
                consortiumFacetCat.getFacetCategoryId(), "RECOVER Adult Curated", "RECOVER Adult Curated",
                "RECOVER Adult Curated Description", null));
        facetMetadataRepository.save(new FacetMetadataModel(
                facetRecoverAdult.getFacetId(), FacetLoaderService.KEY_EFFECTIVE_EXPRESSION_GROUPS,
                "[[{ \"exactly\": \"phs003463\", \"node\": 0 },{ \"regex\": \"(?i)RECOVER_Adult$\", \"node\": 1 }]]"));

        FacetModel infectedFacet = facetRepository.save(new FacetModel(
                consortiumFacetCat.getFacetCategoryId(), "Infected", "Infected", "", null));
        FacetModel nonInfectedFacet = facetRepository.save(new FacetModel(
                consortiumFacetCat.getFacetCategoryId(), "Non-infected", "Non-infected", "", null));

        // Seed concepts
        // c1: Non-infected month 9
        ConceptModel c1 = new ConceptModel(dsAdult.getDatasetId(), "phs003463", "phs003463", "",
                "\\phs003463\\RECOVER_Adult\\biospecimens\\Inventory of Samples Collected\\ac_cptcoll\\Noninf\\9\\", null);
        // c2: Infected month 12
        ConceptModel c2 = new ConceptModel(dsAdult.getDatasetId(), "phs003463", "phs003463", "",
                "\\phs003463\\RECOVER_Adult\\flder_tier2\\chest_ct\\Qualitative Read\\chestct_reticular\\Inf\\12\\", null);
        // c3: Infected month 9
        ConceptModel c3 = new ConceptModel(dsAdult.getDatasetId(), "phs003463", "phs003463", "",
                "\\phs003463\\RECOVER_Adult\\flder_tier2\\echocardiogram_with_strain\\Echocardiogram\\rttestrain_aregurg\\Inf\\9\\", null);
        // cNo: different dataset — must not appear in any month facet
        ConceptModel cNo = new ConceptModel(dsOther.getDatasetId(), "phs000000", "phs000000", "",
                "\\phs000000\\SomeStudy\\something\\42\\", null);

        conceptService.save(c1);
        conceptService.save(c2);
        conceptService.save(c3);
        conceptService.save(cNo);

        // Map concepts to infection-group facets (this is the source of truth for group assignment)
        facetConceptRepository.save(new FacetConceptModel(nonInfectedFacet.getFacetId(), c1.getConceptNodeId()));
        facetConceptRepository.save(new FacetConceptModel(infectedFacet.getFacetId(), c2.getConceptNodeId()));
        facetConceptRepository.save(new FacetConceptModel(infectedFacet.getFacetId(), c3.getConceptNodeId()));

        // 1) Dry run — discover months without persisting
        GenerateRecoverMonthsRequest req = new GenerateRecoverMonthsRequest();
        req.dryRun = true;

        GenerateRecoverMonthsResponse dry = generatorService.generate(req);
        assertTrue(dry instanceof GenerateRecoverMonthsSuccessResponse);
        GenerateRecoverMonthsSuccessResponse dryOk = (GenerateRecoverMonthsSuccessResponse) dry;
        assertEquals("Consortium_Curated_Facets", dryOk.categoryName());
        assertTrue(dryOk.discoveredMonths().contains("9"));
        assertTrue(dryOk.discoveredMonths().contains("12"));
        assertEquals(2, dryOk.discoveredMonths().size());
        assertNull(dryOk.load());

        // 2) Actual generation — verify facets and mappings
        req.dryRun = false;
        GenerateRecoverMonthsResponse out = generatorService.generate(req);
        assertTrue(out instanceof GenerateRecoverMonthsSuccessResponse);
        GenerateRecoverMonthsSuccessResponse outOk = (GenerateRecoverMonthsSuccessResponse) out;
        assertNotNull(outOk.load());
        assertEquals("Generation complete.", outOk.message());

        // Generated facets should use prefixed names and group-specific display
        Optional<FacetModel> inf09Opt = facetRepository.findByName("Infected 09m-post index");
        Optional<FacetModel> noninf09Opt = facetRepository.findByName("Non-infected 09m-post index");
        Optional<FacetModel> inf12Opt = facetRepository.findByName("Infected 12m-post index");
        assertTrue(inf09Opt.isPresent(), "Infected 09m-post index should exist");
        assertTrue(noninf09Opt.isPresent(), "Non-infected 09m-post index should exist");
        assertTrue(inf12Opt.isPresent(), "Infected 12m-post index should exist");

        // Old un-prefixed names should not exist
        assertTrue(facetRepository.findByName("09m-post index").isEmpty());
        assertTrue(facetRepository.findByName("12m-post index").isEmpty());

        // Display should be un-prefixed (e.g. "09m-post index")
        assertEquals("09m-post index", inf09Opt.get().getDisplay());
        assertEquals("09m-post index", noninf09Opt.get().getDisplay());
        assertEquals("12m-post index", inf12Opt.get().getDisplay());

        // Parent IDs must point to the correct infection-group facet
        assertEquals(infectedFacet.getFacetId(), inf09Opt.get().getParentId());
        assertEquals(nonInfectedFacet.getFacetId(), noninf09Opt.get().getParentId());
        assertEquals(infectedFacet.getFacetId(), inf12Opt.get().getParentId());

        // Fetch saved concepts for concept-mapping assertions
        Optional<ConceptModel> c1Opt = conceptService.findByConcept(c1.getConceptPath());
        Optional<ConceptModel> c2Opt = conceptService.findByConcept(c2.getConceptPath());
        Optional<ConceptModel> c3Opt = conceptService.findByConcept(c3.getConceptPath());
        assertTrue(c1Opt.isPresent());
        assertTrue(c2Opt.isPresent());
        assertTrue(c3Opt.isPresent());

        Long inf09Id = inf09Opt.get().getFacetId();
        Long noninf09Id = noninf09Opt.get().getFacetId();
        Long inf12Id = inf12Opt.get().getFacetId();

        // c1 (Noninf/9) → Non-infected 09m-post index only
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(noninf09Id, c1Opt.get().getConceptNodeId()).isPresent());
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(inf09Id, c1Opt.get().getConceptNodeId()).isEmpty());

        // c2 (Inf/12) → Infected 12m-post index only
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(inf12Id, c2Opt.get().getConceptNodeId()).isPresent());
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(noninf09Id, c2Opt.get().getConceptNodeId()).isEmpty());

        // c3 (Inf/9) → Infected 09m-post index only
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(inf09Id, c3Opt.get().getConceptNodeId()).isPresent());
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(noninf09Id, c3Opt.get().getConceptNodeId()).isEmpty());

        // cNo (other dataset) must not appear in any generated month facet
        List<FacetConceptModel> all = facetConceptRepository.findAll();
        assertFalse(all.isEmpty());
        boolean noMapping = all.stream()
                .filter(fc -> fc.getConceptNodeId().equals(cNo.getConceptNodeId()))
                .allMatch(fc -> !fc.getFacetId().equals(inf09Id)
                        && !fc.getFacetId().equals(noninf09Id)
                        && !fc.getFacetId().equals(inf12Id));
        assertTrue(noMapping, "cNo should not be mapped to any generated month facet");
    }
}
