package edu.harvard.dbmi.avillach.dictionaryetl.facetloader;

import edu.harvard.dbmi.avillach.dictionaryetl.Utility.DatabaseCleanupUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetRepository;
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

import java.util.ArrayList;
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
        // Build payload programmatically
        FacetDTO infected = new FacetDTO();
        infected.name = "Infected";
        infected.display = "Infected";
        infected.description = "Infected Facet Description";

        FacetDTO parent = new FacetDTO();
        parent.name = "Recover Adult";
        parent.display = "RECOVER Adult";
        parent.description = "Recover adult parent facet.";
        parent.facets = new ArrayList<>();
        parent.facets.add(infected);

        FacetCategoryDTO catDto = new FacetCategoryDTO();
        catDto.name = "Consortium_Curated_Facets";
        catDto.display = "Consortium Curated Facets";
        catDto.description = "Consortium Curated Facets Description";
        catDto.facets = List.of(parent);

        FacetCategoryWrapper wrapper = new FacetCategoryWrapper();
        wrapper.facetCategory = catDto;

        // First load
        FacetLoaderService.Result result1 = service.load(List.of(wrapper));
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
        FacetLoaderService.Result result2 = service.load(List.of(wrapper));
        Assertions.assertEquals(0, result2.categoriesCreated());
        Assertions.assertEquals(1, result2.categoriesUpdated());
        Assertions.assertEquals(0, result2.facetsCreated());
        Assertions.assertEquals(2, result2.facetsUpdated());
    }
}
