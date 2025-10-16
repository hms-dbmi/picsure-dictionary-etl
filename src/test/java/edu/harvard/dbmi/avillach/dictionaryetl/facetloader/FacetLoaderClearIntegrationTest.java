package edu.harvard.dbmi.avillach.dictionaryetl.facetloader;

import edu.harvard.dbmi.avillach.dictionaryetl.Utility.DatabaseCleanupUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptService;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryRepository;
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
class FacetLoaderClearIntegrationTest {

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
    private FacetCategoryRepository facetCategoryRepository;

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
    void clear_shouldRemoveByCategoryAndFacetNames() {
        // Seed a dataset and concept that both facets can map to
        DatasetModel ds = datasetRepository.save(new DatasetModel("d1", "Dataset 1", "", ""));
        ConceptModel concept = new ConceptModel(ds.getDatasetId(), "d1", "d1", "", "\\A\\B\\C\\", null);
        conceptService.save(concept);

        // Category 1: ToClear, with Parent and Child facets, both map to the concept
        FacetExpressionDTO expA0 = new FacetExpressionDTO();
        expA0.exactly = "A";
        expA0.node = 0;

        FacetDTO child = new FacetDTO();
        child.name = "Child";
        child.display = "Child";
        child.expressions = new ArrayList<>();
        child.expressions.add(expA0);

        FacetDTO parent = new FacetDTO();
        parent.name = "Parent";
        parent.display = "Parent";
        parent.expressions = new ArrayList<>();
        FacetExpressionDTO expB1 = new FacetExpressionDTO();
        expB1.exactly = "B";
        expB1.node = 1;
        parent.expressions.add(expB1);
        parent.facets = List.of(child);

        FacetCategoryDTO cat1 = new FacetCategoryDTO();
        cat1.name = "ToClear";
        cat1.display = "ToClear";
        cat1.description = "Category to clear";
        cat1.facets = List.of(parent);

        FacetCategoryWrapper wrap1 = new FacetCategoryWrapper();
        wrap1.facetCategory = cat1;

        // Category 2: KeepCat, with Root->Leaf; will clear by facet name (Root)
        FacetExpressionDTO expA0b = new FacetExpressionDTO();
        expA0b.exactly = "A";
        expA0b.node = 0;

        FacetDTO leaf = new FacetDTO();
        leaf.name = "Leaf";
        leaf.display = "Leaf";
        leaf.expressions = new ArrayList<>();
        leaf.expressions.add(expA0b);

        FacetDTO root = new FacetDTO();
        root.name = "Root";
        root.display = "Root";
        root.expressions = new ArrayList<>();
        FacetExpressionDTO expC2 = new FacetExpressionDTO();
        expC2.exactly = "C";
        expC2.node = 2;
        root.expressions.add(expC2);
        root.facets = List.of(leaf);

        FacetCategoryDTO cat2 = new FacetCategoryDTO();
        cat2.name = "KeepCat";
        cat2.display = "KeepCat";
        cat2.description = "Category to keep";
        cat2.facets = List.of(root);

        FacetCategoryWrapper wrap2 = new FacetCategoryWrapper();
        wrap2.facetCategory = cat2;

        service.load(List.of(wrap1, wrap2));

        // verify loaded
        Optional<FacetCategoryModel> toClearCat = facetCategoryRepository.findByName("ToClear");
        assertTrue(toClearCat.isPresent());
        assertTrue(facetRepository.findByName("Parent").isPresent());
        assertTrue(facetRepository.findByName("Child").isPresent());
        Optional<FacetModel> rootFacet = facetRepository.findByName("Root");
        Optional<FacetModel> leafFacet = facetRepository.findByName("Leaf");
        assertTrue(rootFacet.isPresent());
        assertTrue(leafFacet.isPresent());

        // perform clear by category name
        FacetClearRequest clearReq = new FacetClearRequest();
        clearReq.facetCategories = List.of("ToClear");
        FacetLoaderService.ClearResult clearRes = service.clear(clearReq);
        assertEquals(1, clearRes.categoriesDeleted());
        assertEquals(2, clearRes.facetsDeleted()); // Parent + Child
        assertTrue(clearRes.mappingsDeleted() >= 2); // both parent and child mapped

        // assert removed
        assertTrue(facetCategoryRepository.findByName("ToClear").isEmpty());
        assertTrue(facetRepository.findByName("Parent").isEmpty());
        assertTrue(facetRepository.findByName("Child").isEmpty());

        // ensure KeepCat remains
        assertTrue(facetCategoryRepository.findByName("KeepCat").isPresent());
        assertTrue(facetRepository.findByName("Root").isPresent());
        assertTrue(facetRepository.findByName("Leaf").isPresent());

        // perform clear by facet name (should remove root and leaf under KeepCat, but keep category)
        FacetClearRequest clearReq2 = new FacetClearRequest();
        clearReq2.facets = List.of("Root");
        FacetLoaderService.ClearResult clearRes2 = service.clear(clearReq2);
        assertEquals(0, clearRes2.categoriesDeleted());
        assertEquals(2, clearRes2.facetsDeleted()); // Root + Leaf
        assertTrue(clearRes2.mappingsDeleted() >= 2);

        assertTrue(facetCategoryRepository.findByName("KeepCat").isPresent());
        assertTrue(facetRepository.findByName("Root").isEmpty());
        assertTrue(facetRepository.findByName("Leaf").isEmpty());
    }
}
