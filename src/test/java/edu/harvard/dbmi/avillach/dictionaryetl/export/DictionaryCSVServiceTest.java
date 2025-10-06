package edu.harvard.dbmi.avillach.dictionaryetl.export;

import edu.harvard.dbmi.avillach.dictionaryetl.Utility.CSVUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptService;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetService;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetService;
import edu.harvard.dbmi.avillach.dictionaryetl.loading.DictionaryLoaderService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
class DictionaryCSVServiceTest {

    @Container
    static final PostgreSQLContainer<?> databaseContainer =
            new PostgreSQLContainer<>("postgres:16").withDatabaseName("testdb").withUsername("testuser").withPassword("testpass")
                    .withCopyFileToContainer(MountableFile.forClasspathResource("schema.sql"), "/docker-entrypoint-initdb.d/schema.sql");
    @Container
    static final PostgreSQLContainer<?> reloadContainer =
                new PostgreSQLContainer<>("postgres:16").withDatabaseName("testdb").withUsername("testuser").withPassword("testpass")
                        .withCopyFileToContainer(MountableFile.forClasspathResource("schema.sql"), "/docker-entrypoint-initdb.d/schema.sql");


    private static String resourcePath;
    @Autowired
    private DictionaryCSVService dictionaryCSVService;
    @Autowired
    private DictionaryLoaderService dictionaryLoaderService;
    @Autowired
    private FacetService facetService;
    @Autowired
    private ConceptService conceptService;
    @Autowired
    private DatasetService datasetService;

    @Autowired
    private CSVUtility csvUtility;

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

    @Test
    void generateFullIngestCSVs_WithSinglePHS() throws IOException {
        ClassPathResource syntheaResource = new ClassPathResource("columnMeta_synthea.csv");
        assertNotNull(syntheaResource);
        String syntheaFilePath = syntheaResource.getFile().toPath().toString();
        this.dictionaryLoaderService.processColumnMetaCSV(syntheaFilePath, resourcePath + "/columnMetaErrors" + ".csv");
        facetService.createDefaultFacets();
        assertFalse(conceptService.findByDatasetID(datasetService.findByRef("ACT Diagnosis ICD-10").get().getDatasetId()).isEmpty());
        assertFalse(facetService.findAllFacetsByDatasetIDs(new Long[]{datasetService.findByRef("ACT Diagnosis ICD-10").get().getDatasetId()}).isEmpty());
        // make a directory for the generated files
        String generatedFilePath = resourcePath + "/generatedFiles/";
        // Make the directory
        File generatedFile = new File(generatedFilePath);
        if (generatedFile.exists()) {
            boolean delete = this.csvUtility.removeDirectoryIfExists(generatedFilePath);
            assertTrue(delete, "Directory should be deleted");
        }

        boolean created = generatedFile.mkdir();
        assertTrue(created, "Directory should be created");

        Assertions.assertDoesNotThrow(() -> this.dictionaryCSVService.generateFullIngestCSVs(generatedFilePath));

        Path generatedFilesPath = Paths.get(resourcePath, "generatedFiles");

        File facetCategoryFile = generatedFilesPath.resolve("Facet_Categories.csv").toFile();
        Assertions.assertTrue(facetCategoryFile.exists());

        File facetFile = generatedFilesPath.resolve("Facets.csv").toFile();
        Assertions.assertTrue(facetFile.exists());

        File facetConceptListFile = generatedFilesPath.resolve("Facet_Concept_Lists.csv").toFile();
        Assertions.assertTrue(facetConceptListFile.exists());

        File datasetFile = generatedFilesPath.resolve("Datasets.csv").toFile();
        Assertions.assertTrue(datasetFile.exists());

        File consentsFile = generatedFilesPath.resolve("Consents.csv").toFile();
        Assertions.assertTrue(consentsFile.exists());

        File conceptsFile = generatedFilesPath.resolve("Concepts.csv").toFile();
        Assertions.assertTrue(conceptsFile.exists());


    }
    @Test
    void verifyGeneratedCSVs(){

    }
}
