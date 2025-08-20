package edu.harvard.dbmi.avillach.dictionaryetl.dataset;

import edu.harvard.dbmi.avillach.dictionaryetl.Utility.DatabaseCleanupUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetConceptRepository;
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

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

@Testcontainers
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@SpringBootTest
class DatasetFacetRefreshServiceTest {
    @Autowired
    DatasetFacetRefreshService subject;
    @Autowired
    private DatabaseCleanupUtility databaseCleanupUtility;

    @Autowired
    FacetRepository facetRepository;

    @Autowired
    FacetCategoryRepository facetCategoryRepository;

    @Autowired
    ConceptRepository conceptRepository;

    @Autowired
    DatasetRepository datasetRepository;

    @Autowired
    FacetConceptRepository facetConceptRepository;

    @Autowired
    ConceptMetadataRepository conceptMetadataRepository;

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
    void shouldNotRefreshIfOneDataset() {
        DatasetModel dA = new DatasetModel("Ref_A", "AAAA", "abv", "desc");
        datasetRepository.save(dA);
        ConceptModel cA = new ConceptModel(dA.getDatasetId(), "c_A", "desc", "continuous", "\\Ref_A\\path\\");
        ConceptModel cB = new ConceptModel(dA.getDatasetId(), "c_B", "desc", "continuous", "\\Ref_B\\path\\");
        ConceptModel cC = new ConceptModel(dA.getDatasetId(), "c_C", "desc", "continuous", "\\Ref_C\\path\\");
        conceptRepository.save(cA);
        conceptRepository.save(cB);
        conceptRepository.save(cC);

        subject.refreshDatasetFacet();

        Optional<FacetCategoryModel> category = facetCategoryRepository.findByName("dataset_id");
        Assertions.assertFalse(category.isPresent());
        List<String> actual = facetRepository.getAllFacetNames();
        List<String> expected = List.of();
        Assertions.assertEquals(expected, actual);
    }

    @Test
    void shouldDeleteAllIfCategoryExists() {
        DatasetModel dA = new DatasetModel("Ref_A", "AAAA", "abv", "desc");
        DatasetModel dB = new DatasetModel("Ref_B", "BBBB", "abv", "desc");
        DatasetModel dC = new DatasetModel("Ref_C", "CCCC", "abv", "desc");
        datasetRepository.save(dA);
        datasetRepository.save(dB);
        datasetRepository.save(dC);
        ConceptModel cA = new ConceptModel(dA.getDatasetId(), "c_A", "desc", "continuous", "\\Ref_A\\path\\");
        ConceptModel cB = new ConceptModel(dB.getDatasetId(), "c_B", "desc", "continuous", "\\Ref_B\\path\\");
        ConceptModel cC = new ConceptModel(dC.getDatasetId(), "c_C", "desc", "continuous", "\\Ref_C\\path\\");
        conceptRepository.save(cA);
        conceptRepository.save(cB);
        conceptRepository.save(cC);
        List<ConceptMetadataModel> conceptMetas = Stream.of(cA, cB, cC)
            .map(c -> new ConceptMetadataModel(c.getConceptNodeId(), "values", "[]"))
            .toList();
        conceptMetadataRepository.saveAll(conceptMetas);

        subject.refreshDatasetFacet();

        Optional<FacetCategoryModel> category = facetCategoryRepository.findByName("dataset_id");
        Assertions.assertTrue(category.isPresent());
        // there is a facet for each dataset
        List<String> actual = facetRepository.getAllFacetNames();
        List<String> expected = List.of("Ref_A", "Ref_B", "Ref_C");
        Assertions.assertEquals(expected, actual);
        // every concept is associated with a facet
        List<Long> actualConcepts = facetConceptRepository.findAll().stream().map(FacetConceptModel::getConceptNodeId).toList();
        List<Long> expectedConcepts = Stream.of(cA, cB, cC).map(ConceptModel::getConceptNodeId).toList();
        Assertions.assertEquals(expectedConcepts, actualConcepts);
    }
}