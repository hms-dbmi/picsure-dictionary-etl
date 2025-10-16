package edu.harvard.dbmi.avillach.dictionaryetl.facetloader;

import edu.harvard.dbmi.avillach.dictionaryetl.Utility.DatabaseCleanupUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptService;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetRepository;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class FacetLoaderMappingIntegrationTest {

    @Autowired
    private DatabaseCleanupUtility databaseCleanupUtility;

    @Autowired
    private FacetLoaderService service;

    @Autowired
    private DatasetRepository datasetRepository;

    @Autowired
    private ConceptService conceptService;

    @Autowired
    private FacetRepository facetRepository;

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
    void expressions_shouldMapFacetsToMatchingConcepts() {
        // Seed dataset and concept nodes
        DatasetModel dsAdult = datasetRepository.save(new DatasetModel("phs003436", "RECOVER Adult", "", ""));
        DatasetModel dsPeds = datasetRepository.save(new DatasetModel("phs003461", "RECOVER Peds", "", ""));

        ConceptModel c1 = new ConceptModel(dsAdult.getDatasetId(), "phs003436", "phs003436", "", "\\phs003436\\Recover_Adult\\biostats_derived\\visits\\inf\\12\\pasc_cc_2024\\", null);
        ConceptModel c2 = new ConceptModel(dsPeds.getDatasetId(), "phs003461", "phs003461", "", "\\phs003461\\RECOVER_Caregiver\\biospecimens\\pc_tassoreplacewhy\\", null);
        ConceptModel c3 = new ConceptModel(dsPeds.getDatasetId(), "phs003461", "phs003461", "", "\\phs003461\\RECOVER_Pediatrics\\pdclassmts2\\asq\\asqsec_persoc_14\\asq14_persoc_score\\", null);
        conceptService.save(c1);
        conceptService.save(c2);
        conceptService.save(c3);

        // Build payload with expressions
        FacetDTO infected = new FacetDTO();
        infected.name = "Infected";
        infected.display = "Infected";
        infected.description = "Infected Facet Description";
        infected.expressions = new ArrayList<>();
        FacetExpressionDTO expInf = new FacetExpressionDTO();
        expInf.regex = "(?i)\\binf(ected)?\\b";
        expInf.node = -3; // third from the end
        infected.expressions.add(expInf);

        FacetDTO parent = new FacetDTO();
        parent.name = "Recover Adult";
        parent.display = "RECOVER Adult";
        parent.description = "Recover adult parent facet.";
        parent.expressions = new ArrayList<>();
        FacetExpressionDTO expParent = new FacetExpressionDTO();
        expParent.exactly = "Recover_Adult";
        expParent.node = 1; // 2nd node
        parent.expressions.add(expParent);
        parent.facets = List.of(infected);

        FacetCategoryDTO catDto = new FacetCategoryDTO();
        catDto.name = "Consortium_Curated_Facets";
        catDto.display = "Consortium Curated Facets";
        catDto.description = "Consortium Curated Facets Description";
        catDto.facets = List.of(parent);

        FacetCategoryWrapper wrapper = new FacetCategoryWrapper();
        wrapper.facetCategory = catDto;

        service.load(List.of(wrapper));

        // Assert facets created
        Optional<FacetModel> parentFacetOpt = facetRepository.findByName("Recover Adult");
        Optional<FacetModel> infFacetOpt = facetRepository.findByName("Infected");
        assertTrue(parentFacetOpt.isPresent());
        assertTrue(infFacetOpt.isPresent());

        Long parentFacetId = parentFacetOpt.get().getFacetId();
        Long infFacetId = infFacetOpt.get().getFacetId();

        // Fetch concepts again for ids
        Optional<ConceptModel> c1Opt = conceptService.findByConcept("\\phs003436\\Recover_Adult\\biostats_derived\\visits\\inf\\12\\pasc_cc_2024\\");
        Optional<ConceptModel> c2Opt = conceptService.findByConcept("\\phs003461\\RECOVER_Caregiver\\biospecimens\\pc_tassoreplacewhy\\");
        Optional<ConceptModel> c3Opt = conceptService.findByConcept("\\phs003461\\RECOVER_Pediatrics\\pdclassmts2\\asq\\asqsec_persoc_14\\asq14_persoc_score\\");
        assertTrue(c1Opt.isPresent());
        assertTrue(c2Opt.isPresent());
        assertTrue(c3Opt.isPresent());

        // Parent facet should map only to c1
        Optional<FacetConceptModel> m1 = facetConceptRepository.findByFacetIdAndConceptNodeId(parentFacetId, c1Opt.get().getConceptNodeId());
        assertTrue(m1.isPresent());
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(parentFacetId, c2Opt.get().getConceptNodeId()).isEmpty());
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(parentFacetId, c3Opt.get().getConceptNodeId()).isEmpty());

        // Infected facet should map to c1 only
        Optional<FacetConceptModel> mInf = facetConceptRepository.findByFacetIdAndConceptNodeId(infFacetId, c1Opt.get().getConceptNodeId());
        assertTrue(mInf.isPresent());
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(infFacetId, c2Opt.get().getConceptNodeId()).isEmpty());
        assertTrue(facetConceptRepository.findByFacetIdAndConceptNodeId(infFacetId, c3Opt.get().getConceptNodeId()).isEmpty());
    }
}
