package edu.harvard.dbmi.avillach.dictionaryetl.clear;

import edu.harvard.dbmi.avillach.dictionaryetl.concept.*;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.*;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryRepository;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
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

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class DictionaryClearControllerTest {

    @Autowired
    private DictionaryClearController dictionaryClearController;

    @Autowired
    private FacetCategoryRepository facetCategoryRepository;

    @Autowired
    private FacetRepository facetRepository;

    @Autowired
    private FacetConceptRepository facetConceptRepository;

    @Autowired
    private DatasetRepository datasetRepository;

    @Autowired
    private ConceptRepository conceptRepository;

    @Autowired
    private ConceptMetadataRepository conceptMetadataRepository;

    @PersistenceContext
         private EntityManager entityManager;
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
    void refreshDatabase() {
        datasetRepository.deleteAll();
        DatasetModel dataset = new DatasetModel("ref1", "REF1", "abv", "");
        datasetRepository.save(dataset);

        conceptRepository.deleteAll();
        ConceptModel concept = new ConceptModel(dataset.getDatasetId(), "concept1", "concept1", "Categorical", "\\concept1path\\", null);
        conceptRepository.save(concept);

        conceptMetadataRepository.deleteAll();
        ConceptMetadataModel conceptMetadata = new ConceptMetadataModel(concept.getConceptNodeId(), "meta1", "key1");
        conceptMetadataRepository.save(conceptMetadata);

        facetCategoryRepository.deleteAll();
        FacetCategoryModel facetCategory = new FacetCategoryModel("category1", "category1", "");
        facetCategoryRepository.save(facetCategory);

        facetRepository.deleteAll();
        FacetModel facet = new FacetModel(facetCategory.getFacetCategoryId(), "facet", "facet", "facet", null);
        facetRepository.save(facet);

        facetConceptRepository.deleteAll();
        FacetConceptModel facetConcept = new FacetConceptModel(facet.getFacetId(), concept.getConceptNodeId());
        facetConceptRepository.save(facetConcept);
    }

    @Test
    void shouldClearOnlyDatasetsAndConcepts() {
        dictionaryClearController.clearDatasetAndConceptTables();
        Assertions.assertEquals(1, facetCategoryRepository.findAll().size());
        Assertions.assertEquals(1, facetRepository.findAll().size());
        Assertions.assertEquals(0, facetConceptRepository.findAll().size());
        Assertions.assertEquals(0, datasetRepository.findAll().size());
        Assertions.assertEquals(0, conceptRepository.findAll().size());
        Assertions.assertEquals(0, conceptMetadataRepository.findAll().size());
    }

    @Test
    void shouldClearAllTables() {
        dictionaryClearController.clearAllTables();
        Assertions.assertEquals(0, facetCategoryRepository.findAll().size());
        Assertions.assertEquals(0, facetRepository.findAll().size());
        Assertions.assertEquals(0, facetConceptRepository.findAll().size());
        Assertions.assertEquals(0, datasetRepository.findAll().size());
        Assertions.assertEquals(0, conceptRepository.findAll().size());
        Assertions.assertEquals(0, conceptMetadataRepository.findAll().size());
    }
}