package edu.harvard.dbmi.avillach.dictionaryetl.export;

import edu.harvard.dbmi.avillach.dictionaryetl.Utility.CSVUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.Utility.DatabaseCleanupUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptController;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptService;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetController;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.*;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryController;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.loading.DictionaryLoaderService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
class DictionaryCSVServiceTest {

    @Container
    static final PostgreSQLContainer<?> databaseContainer =
            new PostgreSQLContainer<>("postgres:16").withDatabaseName("testdb").withUsername("testuser").withPassword("testpass").withUrlParam("currentSchema", "dict")
                    .withCopyFileToContainer(MountableFile.forClasspathResource("schema.sql"), "/docker-entrypoint-initdb.d/schema.sql");

    private static String resourcePath;
    @Autowired
    private DictionaryCSVService dictionaryCSVService;
    @Autowired
    private DictionaryLoaderService dictionaryLoaderService;

    @Autowired
    DatasetController datasetController;
    @Autowired
    DatasetRepository datasetRepository;

    @Autowired
    ConceptController conceptController;
    @Autowired
    ConceptRepository conceptRepository;
    @Autowired
    ConceptMetadataRepository conceptMetadataRepository;

    @Autowired
    FacetCategoryController facetCategoryController;
    @Autowired
    FacetCategoryRepository facetCategoryRepository;

    @Autowired
    FacetController facetController;
    @Autowired
    FacetService facetService;
    @Autowired
    FacetConceptRepository facetConceptRepository;

    @Autowired
    private CSVUtility csvUtility;
    @Autowired
    private DatabaseCleanupUtility databaseCleanupUtility;
    @Autowired
    private ConceptService conceptService;

    @DynamicPropertySource
    static void mySQLProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", databaseContainer::getJdbcUrl);
        registry.add("spring.datasource.username", databaseContainer::getUsername);
        registry.add("spring.datasource.password", databaseContainer::getPassword);
    }

    @BeforeAll
    static void setUp() {
        Path testResourcePath = Paths.get(System.getProperty("user.dir"), "src", "test", "resources");
        resourcePath = testResourcePath.toString();
    }

    @BeforeEach
    void cleanDatabase() {
        databaseCleanupUtility.truncateTablesAllTables();
    }

    @Test
    void generateFullIngestCSVs() throws IOException {
        ClassPathResource syntheaResource = new ClassPathResource("columnMeta_synthea.csv");
        assertNotNull(syntheaResource);
        String syntheaFilePath = syntheaResource.getFile().toPath().toString();
        dictionaryLoaderService.processColumnMetaCSV(syntheaFilePath, resourcePath + "/columnMetaErrors" + ".csv");
        facetService.createDefaultFacets();
        assertFalse(conceptRepository.findByDatasetId(datasetRepository.findByRef("ACT Diagnosis ICD-10").get().getDatasetId()).isEmpty());
        assertFalse(facetService.findAllFacetsByDatasetIDs(new Long[]{datasetRepository.findByRef("ACT Diagnosis ICD-10").get().getDatasetId()}).isEmpty());
        // make a directory for the generated files
        String generatedFilePath = resourcePath + "/generatedFiles/";
        // Make the directory
        File generatedFile = new File(generatedFilePath);
        if (generatedFile.exists()) {
            boolean delete = csvUtility.removeDirectoryIfExists(generatedFilePath);
            assertTrue(delete, "Directory should be deleted");
        }

        boolean created = generatedFile.mkdir();
        assertTrue(created, "Directory should be created");

        Assertions.assertDoesNotThrow(() -> dictionaryCSVService.generateFullIngestCSVs(generatedFilePath));
    }

    @Test
    void validateGeneratedCSVs() throws IOException {
        Path generatedFilesPath = Paths.get(resourcePath, "generatedFiles");
        File datasetFile = generatedFilesPath.resolve("Datasets.csv").toFile();
        Assertions.assertTrue(datasetFile.exists());

        //check if Datasets.csv can be reloaded
        String datasetReload = Files.readString(datasetFile.toPath());
        Assertions.assertEquals(HttpStatus.CREATED, datasetController.updateDatasetsFromCsv(datasetReload).getStatusCode());
        Assertions.assertEquals(5, datasetRepository.findAll().size());

        File conceptsFile = generatedFilesPath.resolve("Concepts.csv").toFile();
        Assertions.assertTrue(conceptsFile.exists());
        //check if concepts.csv can be reloaded
        String conceptReload = Files.readString(conceptsFile.toPath());
        Assertions.assertEquals(HttpStatus.OK, conceptController.updateConceptsFromCSV(conceptReload).getStatusCode());
        //Check all datasets got merged and updated correctly
        datasetRepository.findAll().forEach(
        dataset ->{
            Assertions.assertFalse(conceptRepository.findByDatasetId(dataset.getDatasetId()).isEmpty());
        }
        );
        Assertions.assertTrue(conceptRepository.findByName("Z00-Z99 Factors influencing health status and contact with health services (Z00-Z99)").isPresent());
        Assertions.assertFalse(conceptMetadataRepository.findByKey("values").isEmpty());

        File facetCategoryFile = generatedFilesPath.resolve("Facet_Categories.csv").toFile();
        Assertions.assertTrue(facetCategoryFile.exists());
        //check if facet_categories.csv can be reloaded
        String categoryReload = Files.readString(facetCategoryFile.toPath());
        Assertions.assertEquals(HttpStatus.OK, facetCategoryController.updateFacetCategoriesFromCSVs(categoryReload).getStatusCode());
        Assertions.assertEquals(1, facetCategoryRepository.findAll().size());

        File facetFile = generatedFilesPath.resolve("Facets.csv").toFile();
        Assertions.assertTrue(facetFile.exists());
        //check if facets.csv can be reloaded
        String facetReload = Files.readString(facetFile.toPath());
        Assertions.assertEquals(HttpStatus.OK, facetController.updateFacetsFromCSVs(facetReload).getStatusCode());
        Assertions.assertEquals(2, facetService.findAll().size());
        FacetModel catFacet = facetService.findByName("categorical").orElseThrow();

        File facetConceptListFile = generatedFilesPath.resolve("Facet_Concept_Lists.csv").toFile();
        Assertions.assertTrue(facetConceptListFile.exists());
        String facetConceptReload = Files.readString(facetConceptListFile.toPath());
        Assertions.assertEquals(HttpStatus.OK, facetController.updateFacetConceptMappingsFromCSVs(facetConceptReload).getStatusCode());
        Assertions.assertEquals(conceptRepository.findByConceptType("categorical").size(), facetConceptRepository.findByFacetId(catFacet.getFacetId()).get().size());



    }
}
