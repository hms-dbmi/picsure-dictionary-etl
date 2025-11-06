package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import edu.harvard.dbmi.avillach.dictionaryetl.Utility.DatabaseCleanupUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptService;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.model.FacetConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.model.FacetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.FacetCategoryDTO;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.FacetCategoryWrapper;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.FacetDTO;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.FacetExpressionDTO;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates that ConceptRepository.streamNodeIdAndPath() streams beyond the fetch size (1000)
 * and that FacetLoaderService.mapFacetToConcepts processes all rows.
 */
@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class FacetLoaderStreamingIntegrationTest {

    @Autowired
    private DatabaseCleanupUtility databaseCleanupUtility;

    @Autowired
    private FacetLoaderService service;

    @Autowired
    private DatasetRepository datasetRepository;

    @Autowired
    private ConceptService conceptService;

    @Autowired
    private FacetRepository facetRepository;

    @Autowired
    private FacetConceptRepository facetConceptRepository;

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
    void streaming_shouldProcessMoreThanFetchSize() {
        // Arrange: seed a dataset and > 1000 concept rows that all match the same simple expression
        DatasetModel ds = datasetRepository.save(new DatasetModel("TEST", "TEST Dataset", "", ""));

        int total = 2500; // exceed fetch size so the stream must cross multiple DB fetches
        List<ConceptModel> concepts = new ArrayList<>(total);
        for (int i = 0; i < total; i++) {
            String path = "\\\\TEST\\\\Group\\\\var" + i + "\\\\"; // \\TEST\\Group\\var{i}\\
            concepts.add(new ConceptModel(ds.getDatasetId(), "var" + i, "var" + i, "", path, null));
        }
        concepts.forEach(conceptService::save);

        // Build a facet payload that matches all of the above paths: node 1 (second node) equals "Group"
        FacetExpressionDTO expr = new FacetExpressionDTO();
        expr.exactly = "Group";
        expr.node = 1;

        FacetDTO facet = new FacetDTO();
        facet.name = "All Group";
        facet.display = "All Group";
        facet.description = "All nodes where second segment is Group";
        facet.expressionGroups = List.of(List.of(expr));

        FacetCategoryDTO cat = new FacetCategoryDTO();
        cat.name = "Streaming_Test_Category";
        cat.display = "Streaming Test Category";
        cat.description = "Category for streaming test";
        cat.facets = List.of(facet);

        FacetCategoryWrapper wrapper = new FacetCategoryWrapper();
        wrapper.facetCategory = cat;

        // Act: run the loader which calls mapFacetToConcepts and streams over all concept rows
        service.load(List.of(wrapper));

        // Assert: the facet exists and the number of mappings equals the number of seeded concepts
        Optional<FacetModel> facetOpt = facetRepository.findByName("All Group");
        assertTrue(facetOpt.isPresent(), "Expected the facet to be created");
        Long facetId = facetOpt.get().getFacetId();

        Optional<List<FacetConceptModel>> mappingsOpt = facetConceptRepository.findByFacetId(facetId);
        assertTrue(mappingsOpt.isPresent(), "Expected mappings to be created for the facet");
        assertEquals(total, mappingsOpt.get().size(),
                "Expected all " + total + " concept rows to be processed and mapped");
    }
}
