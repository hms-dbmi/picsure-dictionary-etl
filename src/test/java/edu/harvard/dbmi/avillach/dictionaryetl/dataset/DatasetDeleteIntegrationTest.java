package edu.harvard.dbmi.avillach.dictionaryetl.dataset;

import edu.harvard.dbmi.avillach.dictionaryetl.consent.ConsentModel;
import edu.harvard.dbmi.avillach.dictionaryetl.consent.ConsentRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.util.Optional;

@Testcontainers
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@SpringBootTest
public class DatasetDeleteIntegrationTest {

    @Autowired
    private DatasetController datasetController;

    @Autowired
    private DatasetRepository datasetRepository;

    @Autowired
    private FacetRepository facetRepository;

    @Autowired
    private FacetCategoryRepository facetCategoryRepository;

    @Autowired
    private ConsentRepository consentRepository;

    @Container
    static final PostgreSQLContainer<?> databaseContainer = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass")
            .withUrlParam("currentSchema", "dict")
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

    private FacetCategoryModel getOrCreateDatasetFacetCategory() {
        return facetCategoryRepository.findByName("dataset_id")
                .orElseGet(() -> facetCategoryRepository.save(new FacetCategoryModel("dataset_id", "Dataset", "Facet per dataset")));
    }

    @Test
    void deleteDataset_whenDatasetAndFacetExist_removesBoth_andReturnsOk() {
        DatasetModel dataset = new DatasetModel("REF_INT_1", "Full Name", "ABV", "Desc");
        datasetRepository.save(dataset);

        FacetCategoryModel category = getOrCreateDatasetFacetCategory();

        FacetModel facet = new FacetModel(category.getFacetCategoryId(), "REF_INT_1", "REF_INT_1", "desc", null);
        facetRepository.save(facet);

        Assertions.assertTrue(datasetRepository.findByRef("REF_INT_1").isPresent());
        Assertions.assertTrue(facetRepository.findByName("REF_INT_1").isPresent());

        ResponseEntity<String> response = datasetController.deleteDataset("REF_INT_1");

        Assertions.assertEquals(HttpStatus.OK, response.getStatusCode());
        Assertions.assertEquals("Dataset deleted", response.getBody());
        Assertions.assertTrue(datasetRepository.findByRef("REF_INT_1").isEmpty());
        Assertions.assertTrue(facetRepository.findByName("REF_INT_1").isEmpty());
    }

    @Test
    void deleteDataset_whenDatasetMissing_butFacetExists_removesFacet_andReturnsNoContent() {
        FacetCategoryModel category = getOrCreateDatasetFacetCategory();
        FacetModel facet = new FacetModel(category.getFacetCategoryId(), "REF_INT_2", "REF_INT_2", "desc", null);
        facetRepository.save(facet);

        Optional<DatasetModel> missing = datasetRepository.findByRef("REF_INT_2");
        Assertions.assertTrue(missing.isEmpty());
        Assertions.assertTrue(facetRepository.findByName("REF_INT_2").isPresent());

        ResponseEntity<String> response = datasetController.deleteDataset("REF_INT_2");

        Assertions.assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());
        Assertions.assertEquals("No dataset found to delete", response.getBody());
        Assertions.assertTrue(facetRepository.findByName("REF_INT_2").isEmpty());
    }

    @Test
    void deleteDataset_removesConsents_andLeavesNoOrphans() {
        // Create dataset
        DatasetModel dataset = new DatasetModel("REF_INT_3", "Full Name 3", "ABV3", "Desc3");
        datasetRepository.save(dataset);

        // Create a consent linked to the dataset (note: consent table has no FK in schema.sql)
        ConsentModel consent = new ConsentModel(
                dataset.getDatasetId(),
                "DUO:0000001",
                "Test consent",
                "AUTHZ",
                10L,
                20L,
                30L
        );
        consentRepository.save(consent);

        // Sanity checks
        Assertions.assertTrue(datasetRepository.findByRef("REF_INT_3").isPresent());
        Assertions.assertFalse(consentRepository.findByDatasetId(dataset.getDatasetId()).isEmpty());

        // Delete the dataset via controller
        ResponseEntity<String> response = datasetController.deleteDataset("REF_INT_3");

        // Verify response and that both dataset and consents are gone
        Assertions.assertEquals(HttpStatus.OK, response.getStatusCode());
        Assertions.assertTrue(datasetRepository.findByRef("REF_INT_3").isEmpty());
        Assertions.assertTrue(consentRepository.findByDatasetId(dataset.getDatasetId()).isEmpty());
    }
}
