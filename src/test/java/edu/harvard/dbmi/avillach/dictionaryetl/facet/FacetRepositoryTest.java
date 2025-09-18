package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import edu.harvard.dbmi.avillach.dictionaryetl.Utility.DatabaseCleanupUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
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

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class FacetRepositoryTest {

    @Autowired
    private DatabaseCleanupUtility databaseCleanupUtility;

    @Autowired
    FacetRepository subject;

    @Autowired
    FacetCategoryRepository facetCategoryRepository;

    @Autowired
    DatasetRepository datasetRepository;

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
    void shouldDeleteAllForCategory() {
        FacetCategoryModel cat = new FacetCategoryModel("cat", "Cat", "category");
        facetCategoryRepository.save(cat);
        FacetModel facet = new FacetModel(cat.getFacetCategoryId(), "f", "F", "facet", null);
        subject.save(facet);

        Assertions.assertTrue(subject.findByName("f").isPresent());
        subject.deleteAllForCategory(cat.getFacetCategoryId());
        Assertions.assertTrue(subject.findByName("f").isEmpty());
    }

    @Test
    void shouldNotDeleteAllForCategoryThatDNE() {
        FacetCategoryModel cat = new FacetCategoryModel("cat", "Cat", "category");
        facetCategoryRepository.save(cat);
        FacetModel facet = new FacetModel(cat.getFacetCategoryId(), "f", "F", "facet", null);
        subject.save(facet);

        Assertions.assertTrue(subject.findByName("f").isPresent());
        subject.deleteAllForCategory(0L);
        Assertions.assertTrue(subject.findByName("f").isPresent());
    }

    @Test
    void shouldCreateFacetForEachDatasetForCategory() {
        DatasetModel dA = new DatasetModel("Ref_A", "AAAA", "abv", "desc");
        DatasetModel dB = new DatasetModel("Ref_B", "BBBB", "abv", "desc");
        DatasetModel dC = new DatasetModel("Ref_C", "CCCC", "abv", "desc");
        datasetRepository.save(dA);
        datasetRepository.save(dB);
        datasetRepository.save(dC);
        FacetCategoryModel cat = new FacetCategoryModel("dataset_id", "Datasets", "category");
        facetCategoryRepository.save(cat);

        subject.createFacetForEachDatasetForCategory(cat.getFacetCategoryId());
        List<String> actual = subject.getAllFacetNames();

        List<String> expected = List.of("Ref_A", "Ref_B", "Ref_C");
        Assertions.assertEquals(expected, actual);
    }
}