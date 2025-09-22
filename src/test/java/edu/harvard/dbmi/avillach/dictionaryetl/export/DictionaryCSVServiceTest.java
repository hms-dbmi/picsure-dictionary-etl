package edu.harvard.dbmi.avillach.dictionaryetl.export;

import edu.harvard.dbmi.avillach.dictionaryetl.Utility.CSVUtility;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
class DictionaryCSVServiceTest {

    private static String resourcePath;

    @Autowired
    private DictionaryCSVService dictionaryCSVService;

    @Autowired
    private DictionaryLoaderService dictionaryLoaderService;

    @Autowired
    private CSVUtility csvUtility;

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
        this.dictionaryLoaderService.processColumnMetaCSV(syntheaFilePath, resourcePath +
                                                                                     "/columnMetaErrors" +
                                                                                     ".csv");
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
        Assertions.assertTrue(generatedFilesPath.resolve("Facet.csv").toFile().exists());
        Assertions.assertTrue(generatedFilesPath.resolve("Facet_Concept_List.csv").toFile().exists());
        Assertions.assertTrue(generatedFilesPath.resolve("Facet_Categories.csv").toFile().exists());
        Assertions.assertTrue(generatedFilesPath.resolve("Concept.csv").toFile().exists());
        Assertions.assertTrue(generatedFilesPath.resolve("Consents.csv").toFile().exists());
        Assertions.assertTrue(generatedFilesPath.resolve("Datasets.csv").toFile().exists());

        boolean deleted = this.csvUtility.removeDirectoryIfExists(generatedFilePath);
        assertTrue(deleted, "Directory should be deleted");
    }

}