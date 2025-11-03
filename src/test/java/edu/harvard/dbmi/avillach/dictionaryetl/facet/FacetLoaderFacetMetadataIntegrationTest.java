package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import edu.harvard.dbmi.avillach.dictionaryetl.Utility.DatabaseCleanupUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptService;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.model.FacetMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.model.FacetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.FacetCategoryDTO;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.FacetCategoryWrapper;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.FacetDTO;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.FacetExpressionDTO;
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
class FacetLoaderFacetMetadataIntegrationTest {

    private static final String KEY_EFFECTIVE_EXPRESSIONS = "facet_loader.effective_expressions";
    private static final String KEY_EFFECTIVE_EXPRESSIONS_HASH = "facet_loader.effective_expressions_sha256hex";
    private static final String KEY_OWN_EXPRESSIONS = "facet_loader.expressions";
    private static final String KEY_OWN_EXPRESSIONS_HASH = "facet_loader.expressions_sha256hex";

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

    @Autowired
    private FacetMetadataRepository facetMetadataRepository;

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
    void effectiveExpressions_metadata_shouldBePersisted_andChildShouldInheritParent() {
        // Seed dataset and concept nodes
        DatasetModel ds = datasetRepository.save(new DatasetModel("d1", "Dataset 1", "", ""));
        ConceptModel c = new ConceptModel(ds.getDatasetId(), "d1", "d1", "", "\\A\\B\\C\\", null);
        conceptService.save(c);

        // Build payload: parent requires B at node1; child requires C at node2
        FacetDTO child = new FacetDTO();
        child.name = "Child";
        child.display = "Child";
        child.description = "Child facet";
        child.expressions = new ArrayList<>();
        FacetExpressionDTO expC2 = new FacetExpressionDTO();
        expC2.exactly = "C";
        expC2.node = 2;
        child.expressions.add(expC2);

        FacetDTO parent = new FacetDTO();
        parent.name = "Parent";
        parent.display = "Parent";
        parent.description = "Parent facet";
        parent.expressions = new ArrayList<>();
        FacetExpressionDTO expB1 = new FacetExpressionDTO();
        expB1.exactly = "B";
        expB1.node = 1;
        parent.expressions.add(expB1);
        parent.facets = List.of(child);

        FacetCategoryDTO cat = new FacetCategoryDTO();
        cat.name = "Cat";
        cat.display = "Cat";
        cat.description = "Cat desc";
        cat.facets = List.of(parent);

        FacetCategoryWrapper wrapper = new FacetCategoryWrapper();
        wrapper.facetCategory = cat;

        // Load
        service.load(List.of(wrapper));

        // Resolve facets
        Optional<FacetModel> parentOpt = facetRepository.findByName("Parent");
        Optional<FacetModel> childOpt = facetRepository.findByName("Child");
        assertTrue(parentOpt.isPresent());
        assertTrue(childOpt.isPresent());

        long parentId = parentOpt.get().getFacetId();
        long childId = childOpt.get().getFacetId();

        // Metadata: both own and effective keys should exist
        List<FacetMetadataModel> parentMeta = facetMetadataRepository.findByFacetId(parentId);
        List<FacetMetadataModel> childMeta = facetMetadataRepository.findByFacetId(childId);

        assertTrue(parentMeta.stream().anyMatch(m -> KEY_OWN_EXPRESSIONS.equals(m.getKey())));
        assertTrue(parentMeta.stream().anyMatch(m -> KEY_OWN_EXPRESSIONS_HASH.equals(m.getKey())));
        assertTrue(parentMeta.stream().anyMatch(m -> KEY_EFFECTIVE_EXPRESSIONS.equals(m.getKey())));
        assertTrue(parentMeta.stream().anyMatch(m -> KEY_EFFECTIVE_EXPRESSIONS_HASH.equals(m.getKey())));

        assertTrue(childMeta.stream().anyMatch(m -> KEY_OWN_EXPRESSIONS.equals(m.getKey())));
        assertTrue(childMeta.stream().anyMatch(m -> KEY_OWN_EXPRESSIONS_HASH.equals(m.getKey())));
        assertTrue(childMeta.stream().anyMatch(m -> KEY_EFFECTIVE_EXPRESSIONS.equals(m.getKey())));
        assertTrue(childMeta.stream().anyMatch(m -> KEY_EFFECTIVE_EXPRESSIONS_HASH.equals(m.getKey())));

        // Child should map because it inherits Parent(B@1) and has C@2
        assertTrue(facetConceptRepository.countForFacet(childId) >= 1);

        // Now change only the parent to require non-matching value at node1; child should de-map
        FacetDTO parentV2 = new FacetDTO();
        parentV2.name = "Parent";
        parentV2.display = "Parent";
        parentV2.description = "Parent facet";
        parentV2.expressions = new ArrayList<>();
        FacetExpressionDTO expZ1 = new FacetExpressionDTO();
        expZ1.exactly = "Z";
        expZ1.node = 1;
        parentV2.expressions.add(expZ1);
        parentV2.facets = List.of(child); // child unchanged

        FacetCategoryDTO catV2 = new FacetCategoryDTO();
        catV2.name = "Cat";
        catV2.display = "Cat";
        catV2.description = "Cat desc";
        catV2.facets = List.of(parentV2);

        FacetCategoryWrapper wrapV2 = new FacetCategoryWrapper();
        wrapV2.facetCategory = catV2;

        service.load(List.of(wrapV2));

        // Child should have zero mappings now due to parent's constraint
        assertEquals(0L, facetConceptRepository.countForFacet(childId));

        // Child effective metadata should have updated hash/value reflecting new parent
        List<FacetMetadataModel> childMetaAfter = facetMetadataRepository.findByFacetId(childId);
        String effectiveJsonAfter = childMetaAfter.stream()
                .filter(m -> KEY_EFFECTIVE_EXPRESSIONS.equals(m.getKey()))
                .map(FacetMetadataModel::getValue)
                .findFirst()
                .orElse(null);
        assertNotNull(effectiveJsonAfter);
        assertTrue(effectiveJsonAfter.contains("\"exactly\":\"Z\""));
        assertTrue(effectiveJsonAfter.contains("\"exactly\":\"C\""));
    }
}
