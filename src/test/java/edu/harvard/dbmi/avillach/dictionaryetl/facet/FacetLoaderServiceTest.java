package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import edu.harvard.dbmi.avillach.dictionaryetl.Utility.DatabaseCleanupUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.*;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.model.FacetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryMeta;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryMetaRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryRepository;
import org.junit.jupiter.api.Assertions;
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

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class FacetLoaderServiceTest {

    @Autowired
    private DatabaseCleanupUtility databaseCleanupUtility;

    @Autowired
    private FacetLoaderService service;

    @Autowired
    private FacetRepository facetRepository;

    @Autowired
    private FacetCategoryRepository facetCategoryRepository;

    @Autowired
    private FacetCategoryMetaRepository facetCategoryMetaRepository;

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
    void load_shouldCreateCategoryAndFacets_andBeIdempotent() {
        // Build payload programmatically (records)
        FacetDTO infected = new FacetDTO(
                "Infected",
                "Infected",
                "Infected Facet Description",
                null,
                null
        );

        FacetDTO parent = new FacetDTO(
                "Recover Adult",
                "RECOVER Adult",
                "Recover adult parent facet.",
                null,
                List.of(infected)
        );

        FacetCategoryDTO catDto = new FacetCategoryDTO(
                "Consortium_Curated_Facets",
                "Consortium Curated Facets",
                "Consortium Curated Facets Description",
                List.of(parent)
        );

        FacetCategoryWrapper wrapper = new FacetCategoryWrapper(catDto);

        // First load
        Result result1 = service.load(List.of(wrapper));
        Assertions.assertEquals(1, result1.categoriesCreated());
        Assertions.assertEquals(0, result1.categoriesUpdated());
        Assertions.assertEquals(2, result1.facetsCreated());
        Assertions.assertEquals(0, result1.facetsUpdated());

        Optional<FacetCategoryModel> cat = facetCategoryRepository.findByName("Consortium_Curated_Facets");
        Assertions.assertTrue(cat.isPresent());

        Optional<FacetModel> parentFacet = facetRepository.findByName("Recover Adult");
        Optional<FacetModel> infectedFacet = facetRepository.findByName("Infected");
        Assertions.assertTrue(parentFacet.isPresent());
        Assertions.assertTrue(infectedFacet.isPresent());
        Assertions.assertEquals(parentFacet.get().getFacetId(), infectedFacet.get().getParentId());
        Assertions.assertEquals(cat.get().getFacetCategoryId(), parentFacet.get().getFacetCategoryId());
        Assertions.assertEquals(cat.get().getFacetCategoryId(), infectedFacet.get().getFacetCategoryId());

        // Second load (idempotency/update path)
        Result result2 = service.load(List.of(wrapper));
        Assertions.assertEquals(0, result2.categoriesCreated());
        Assertions.assertEquals(1, result2.categoriesUpdated());
        Assertions.assertEquals(0, result2.facetsCreated());
        Assertions.assertEquals(2, result2.facetsUpdated());
    }

    @Test
    void load_shouldCreateCategory_andAddMetadata() {
        String name = "Consortium_Curated_Facets";
        String display = "Consortium Curated Facets";
        String description = "Consortium Curated Facets Description";
        String metaKey = "Some Key";
        String metaValue = "some value";

        FacetCategoryMetaDTO requestMeta = new FacetCategoryMetaDTO(metaKey, metaValue);
        FacetCategoryDTO requestCategory = new FacetCategoryDTO(name, display, description, List.of(), List.of(requestMeta));
        service.load(List.of(new FacetCategoryWrapper(requestCategory)));

        Optional<FacetCategoryModel> category = facetCategoryRepository.findByName(name);
        Assertions.assertTrue(category.isPresent());

        Long categoryId = category.get().getFacetCategoryId();

        Optional<FacetCategoryMeta> metadata = facetCategoryMetaRepository.findFacetCategoryMetaByCategoryId(categoryId, metaKey);
        Assertions.assertTrue(metadata.isPresent());
        Assertions.assertEquals(metaValue, metadata.get().getValue());
    }

    @Test
    void load_shouldCreateCategory_andUpdateMetadata() {
        String name = "Consortium_Curated_Facets";
        String display = "Consortium Curated Facets";
        String description = "Consortium Curated Facets Description";
        String metaKey = "Some Key";

        // Load initial
        String testValue = "some value";
        FacetCategoryMetaDTO requestMeta = new FacetCategoryMetaDTO(metaKey, testValue);
        FacetCategoryDTO requestCategory = new FacetCategoryDTO(name, display, description, List.of(), List.of(requestMeta));
        service.load(List.of(new FacetCategoryWrapper(requestCategory)));

        Optional<FacetCategoryModel> category = facetCategoryRepository.findByName(name);
        Assertions.assertTrue(category.isPresent());

        Long categoryId = category.get().getFacetCategoryId();

        Optional<FacetCategoryMeta> metadata = facetCategoryMetaRepository.findFacetCategoryMetaByCategoryId(categoryId, metaKey);
        Assertions.assertTrue(metadata.isPresent());
        Assertions.assertEquals(testValue, metadata.get().getValue());

        // Load updated
        String updatedTestValue = "some other value";
        FacetCategoryMetaDTO updatedMeta = new FacetCategoryMetaDTO(metaKey, updatedTestValue);
        FacetCategoryDTO updatedCategory = new FacetCategoryDTO(name, display, description, List.of(), List.of(updatedMeta));
        service.load(List.of(new FacetCategoryWrapper(updatedCategory)));

        Optional<FacetCategoryModel> reloadedCategory = facetCategoryRepository.findByName(name);
        Assertions.assertTrue(reloadedCategory.isPresent());

        Optional<FacetCategoryMeta> reloadedMetadata = facetCategoryMetaRepository.findFacetCategoryMetaByCategoryId(categoryId, metaKey);
        Assertions.assertTrue(reloadedMetadata.isPresent());
        Assertions.assertEquals(updatedTestValue, reloadedMetadata.get().getValue());
    }
}
