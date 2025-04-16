package edu.harvard.dbmi.avillach.dictionaryetl.export;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.nio.file.Paths;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
class DictionaryCSVServiceTest {

    private static String resourcePath;

    @Autowired
    private DictionaryCSVService dictionaryCSVService;

    @Container
    static final PostgreSQLContainer<?> databaseContainer = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("testdb")
            .withUsername("username")
            .withPassword("password")
            .withCopyFileToContainer(
                    MountableFile.forClasspathResource("2025_04_09_15_33_11-dump.sql"),
                    "/docker-entrypoint-initdb.d/2025_04_09_15_33_11-dump.sql"
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
        //this.dictionaryCSVService.generateFullIngestCSVs(resourcePath, "phs002205");
    }

    @Test
    void generateFullIngestCSVs_WithMultiplePHS() {
        //this.dictionaryCSVService.generateFullIngestCSVs(resourcePath, "phs000007", "open_access-1000Genomes");
    }

    @Test
    void generateFullIngestCSVs_WithNoPHS() {
        //this.dictionaryCSVService.generateFullIngestCSVs(resourcePath);
    }

}