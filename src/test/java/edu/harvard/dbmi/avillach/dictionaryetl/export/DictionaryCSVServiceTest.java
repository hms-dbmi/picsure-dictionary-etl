package edu.harvard.dbmi.avillach.dictionaryetl.export;

import edu.harvard.dbmi.avillach.dictionaryetl.Utility.ColumnMetaUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataService;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptService;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetMetadataService;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetService;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetService;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryService;
import edu.harvard.dbmi.avillach.dictionaryetl.loading.ColumnMetaMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.checkerframework.checker.units.qual.A;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
class DictionaryCSVServiceTest {

    private static String filePath;
    private static String resourcePath;

    @Autowired
    private DictionaryCSVService dictionaryCSVService;

    @Container
    static final PostgreSQLContainer<?> databaseContainer = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("testdb")
            .withUsername("username")
            .withPassword("password")
            .withCopyFileToContainer(
                    MountableFile.forClasspathResource("2025_04_04-dump.sql"),
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
    void generateFullIngestCSVs_WithSinglePHS() {
        this.dictionaryCSVService.generateFullIngestCSVs(resourcePath, "phs002205");
    }

    @Test
    void generateFullIngestCSVs_WithMultiplePHS() {
        this.dictionaryCSVService.generateFullIngestCSVs(resourcePath, "phs002205", "phs000007");
    }

    @Test
    void generateFullIngestCSVs_WithNoPHS() {
        this.dictionaryCSVService.generateFullIngestCSVs(resourcePath);
    }


}