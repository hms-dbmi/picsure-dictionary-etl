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
        FacetExpressionDTO expC2 = new FacetExpressionDTO("C", null, null, 2);
        FacetDTO child = new FacetDTO(
                "Child",
                "Child",
                "Child facet",
                new ArrayList<>(List.of(List.of(expC2))),
                null
        );

        FacetExpressionDTO expB1 = new FacetExpressionDTO("B", null, null, 1);
        FacetDTO parent = new FacetDTO(
                "Parent",
                "Parent",
                "Parent facet",
                new ArrayList<>(List.of(List.of(expB1))),
                List.of(child)
        );

        FacetCategoryDTO cat = new FacetCategoryDTO(
                "Cat",
                "Cat",
                "Cat desc",
                List.of(parent)
        );

        FacetCategoryWrapper wrapper = new FacetCategoryWrapper(cat);
        service.load(List.of(wrapper));

        Optional<FacetModel> parentOpt = facetRepository.findByName("Parent");
        Optional<FacetModel> childOpt = facetRepository.findByName("Child");
        assertTrue(parentOpt.isPresent());
        assertTrue(childOpt.isPresent());

        long parentId = parentOpt.get().getFacetId();
        long childId = childOpt.get().getFacetId();

        // Metadata: both own and effective keys should exist
        List<FacetMetadataModel> parentMeta = facetMetadataRepository.findByFacetId(parentId);
        List<FacetMetadataModel> childMeta = facetMetadataRepository.findByFacetId(childId);

        assertTrue(parentMeta.stream().anyMatch(m -> FacetLoaderService.KEY_FACET_EXPRESSION_GROUPS.equals(m.getKey())));
        assertTrue(parentMeta.stream().anyMatch(m -> FacetLoaderService.KEY_FACET_EXPRESSION_GROUPS_HASH.equals(m.getKey())));
        assertTrue(parentMeta.stream().anyMatch(m -> FacetLoaderService.KEY_FACET_EXPRESSION_GROUPS.equals(m.getKey())));
        assertTrue(parentMeta.stream().anyMatch(m -> FacetLoaderService.KEY_FACET_EXPRESSION_GROUPS_HASH.equals(m.getKey())));

        assertTrue(childMeta.stream().anyMatch(m -> FacetLoaderService.KEY_FACET_EXPRESSION_GROUPS.equals(m.getKey())));
        assertTrue(childMeta.stream().anyMatch(m -> FacetLoaderService.KEY_EFFECTIVE_EXPRESSION_GROUPS_HASH.equals(m.getKey())));
        assertTrue(childMeta.stream().anyMatch(m -> FacetLoaderService.KEY_FACET_EXPRESSION_GROUPS.equals(m.getKey())));
        assertTrue(childMeta.stream().anyMatch(m -> FacetLoaderService.KEY_FACET_EXPRESSION_GROUPS_HASH.equals(m.getKey())));

        // Child should map because it inherits Parent(B@1) and has C@2
        assertTrue(facetConceptRepository.countForFacet(childId) >= 1);

        // Now change only the parent to require non-matching value at node1; child should de-map
        FacetExpressionDTO expZ1 = new FacetExpressionDTO("Z", null, null, 1);
        FacetDTO parentV2 = new FacetDTO(
                "Parent",
                "Parent",
                "Parent facet",
                new ArrayList<>(List.of(List.of(expZ1))),
                List.of(child)
        );

        FacetCategoryDTO catV2 = new FacetCategoryDTO(
                "Cat",
                "Cat",
                "Cat desc",
                List.of(parentV2)
        );

        FacetCategoryWrapper wrapV2 = new FacetCategoryWrapper(catV2);
        service.load(List.of(wrapV2));

        // Child should have zero mappings now due to parent's constraint
        assertEquals(0L, facetConceptRepository.countForFacet(childId));

        // Child effective metadata should have updated hash/value reflecting new parent
        List<FacetMetadataModel> childMetaAfter = facetMetadataRepository.findByFacetId(childId);
        String effectiveJsonAfter = childMetaAfter.stream()
                .filter(m -> FacetLoaderService.KEY_EFFECTIVE_EXPRESSION_GROUPS.equals(m.getKey()))
                .map(FacetMetadataModel::getValue)
                .findFirst()
                .orElse(null);
        assertNotNull(effectiveJsonAfter);
        assertTrue(effectiveJsonAfter.contains("\"exactly\":\"Z\""));
        assertTrue(effectiveJsonAfter.contains("\"exactly\":\"C\""));
    }
}
