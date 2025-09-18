package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import edu.harvard.dbmi.avillach.dictionaryetl.Utility.DatabaseCleanupUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptRepository;
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
import java.util.Optional;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class FacetConceptRepositoryTest {

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
    ConceptMetadataRepository conceptMetadataRepository;

    @Autowired
    FacetConceptRepository subject;

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
    void shouldDeleteForCategory() {
        DatasetModel dataset = new DatasetModel("ref", "full", "abv", "desc");
        datasetRepository.save(dataset);
        FacetCategoryModel cat = new FacetCategoryModel("cat", "Cat", "category");
        facetCategoryRepository.save(cat);
        FacetModel facet = new FacetModel(cat.getFacetCategoryId(), "f", "F", "facet", null);
        facetRepository.save(facet);
        ConceptModel concept = new ConceptModel(dataset.getDatasetId(), "my_concept", "desc", "continuous", "\\my\\path\\");
        conceptRepository.save(concept);
        FacetConceptModel pair = new FacetConceptModel(facet.getFacetId(), concept.getConceptNodeId());
        subject.save(pair);
        Optional<List<FacetConceptModel>> forFacet = subject.findByFacetId(facet.getFacetId());
        Assertions.assertTrue(forFacet.isPresent());
        Assertions.assertEquals(1, forFacet.get().size());
        Assertions.assertEquals(pair.getFacetConceptId(), forFacet.get().getFirst().getFacetConceptId());

        subject.deleteAllForCategory(cat.getFacetCategoryId());

        forFacet = subject.findByFacetId(facet.getFacetId());
        Assertions.assertTrue(forFacet.isPresent());
        Assertions.assertTrue(forFacet.get().isEmpty());
    }

    @Test
    void shouldNotDeleteForOtherCategory() {
        DatasetModel dataset = new DatasetModel("ref", "full", "abv", "desc");
        datasetRepository.save(dataset);
        FacetCategoryModel cat = new FacetCategoryModel("cat", "Cat", "category");
        facetCategoryRepository.save(cat);
        FacetModel facet = new FacetModel(cat.getFacetCategoryId(), "f", "F", "facet", null);
        facetRepository.save(facet);
        ConceptModel concept = new ConceptModel(dataset.getDatasetId(), "my_concept", "desc", "continuous", "\\my\\path\\");
        conceptRepository.save(concept);
        FacetConceptModel pair = new FacetConceptModel(facet.getFacetId(), concept.getConceptNodeId());
        subject.save(pair);
        Optional<List<FacetConceptModel>> forFacet = subject.findByFacetId(facet.getFacetId());
        Assertions.assertTrue(forFacet.isPresent());
        Assertions.assertEquals(1, forFacet.get().size());
        Assertions.assertEquals(pair.getFacetConceptId(), forFacet.get().getFirst().getFacetConceptId());

        subject.deleteAllForCategory(cat.getFacetCategoryId() + 1);

        forFacet = subject.findByFacetId(facet.getFacetId());
        Assertions.assertTrue(forFacet.isPresent());
        Assertions.assertEquals(1, forFacet.get().size());
    }

    @Test
    void shouldCreateFacetForEachDataset() {
        DatasetModel dA = new DatasetModel("Ref_A", "AAAA", "abv", "desc");
        DatasetModel dB = new DatasetModel("Ref_B", "BBBB", "abv", "desc");
        DatasetModel dC = new DatasetModel("Ref_C", "CCCC", "abv", "desc");
        datasetRepository.save(dA);
        datasetRepository.save(dB);
        datasetRepository.save(dC);
        ConceptModel cA = new ConceptModel(dA.getDatasetId(), "c_A", "desc", "continuous", "\\Ref_A\\path\\");
        ConceptModel cB = new ConceptModel(dA.getDatasetId(), "c_B", "desc", "continuous", "\\Ref_B\\path\\");
        ConceptModel cC = new ConceptModel(dA.getDatasetId(), "c_C", "desc", "continuous", "\\Ref_C\\path\\");
        conceptRepository.save(cA);
        conceptRepository.save(cB);
        conceptRepository.save(cC);
        ConceptMetadataModel metaA = new ConceptMetadataModel(cA.getConceptNodeId(), "min", "0");
        ConceptMetadataModel metaB = new ConceptMetadataModel(cB.getConceptNodeId(), "values", "[\"a\"]");
        // no meta for C, expect it not to be added to facet, as it does not show in search
        conceptMetadataRepository.save(metaA);
        conceptMetadataRepository.save(metaB);

        FacetCategoryModel cat = new FacetCategoryModel("dataset_id", "Datasets", "category");
        facetCategoryRepository.save(cat);
        facetRepository.createFacetForEachDatasetForCategory(cat.getFacetCategoryId());

        subject.createDatasetPairForEachLeafConcept(cat.getFacetCategoryId());

        Assertions.assertEquals(2, subject.findAll().size());
    }
}