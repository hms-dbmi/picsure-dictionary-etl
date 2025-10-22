package edu.harvard.dbmi.avillach.dictionaryetl.facetloader;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.harvard.dbmi.avillach.dictionaryetl.Utility.DatabaseCleanupUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptService;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetMetadataRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facetloader.dto.*;
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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class FacetLoaderFacetMetadataIntegrationTest {

    private static final String KEY_FACET_EXPRESSIONS = "facet_loader.expressions";
    private static final String KEY_FACET_EXPRESSIONS_HASH = "facet_loader.expressions_hash";

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

    @Autowired
    private FacetMetadataRepository facetMetadataRepository;

    @Autowired
    private ObjectMapper objectMapper;

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
    void firstLoad_thenSameExpressions_shouldPersistFacetMeta_andRemainUnchanged_andNotIncreaseMappings() throws Exception {
        // Seed 2 datasets and 2 concepts (one intended to match, one not)
        DatasetModel ds1 = datasetRepository.save(new DatasetModel("d1", "Dataset 1", "", ""));
        DatasetModel ds2 = datasetRepository.save(new DatasetModel("d2", "Dataset 2", "", ""));
        ConceptModel cMatch = new ConceptModel(ds1.getDatasetId(), "d1", "d1", "", "\\A\\B\\C\\", null);
        ConceptModel cNoMatch = new ConceptModel(ds2.getDatasetId(), "d2", "d2", "", "\\X\\Y\\Z\\", null);
        conceptService.save(cMatch);
        conceptService.save(cNoMatch);

        // Facet with expression that matches only cMatch (node index 1 == "B")
        FacetExpressionDTO expB1 = new FacetExpressionDTO();
        expB1.exactly = "B";
        expB1.node = 1;
        FacetDTO facet = new FacetDTO();
        facet.name = "FacetOne";
        facet.display = "Facet One";
        facet.expressions = new ArrayList<>();
        facet.expressions.add(expB1);

        FacetCategoryDTO cat = new FacetCategoryDTO();
        cat.name = "Cat";
        cat.display = "Cat";
        cat.facets = List.of(facet);

        FacetCategoryWrapper wrapper = new FacetCategoryWrapper();
        wrapper.facetCategory = cat;

        // First load
        Result res1 = service.load(List.of(wrapper));
        assertEquals(1, res1.categoriesCreated());
        assertEquals(1, res1.facetsCreated());

        Optional<FacetModel> facetModelOpt = facetRepository.findByName("FacetOne");
        assertTrue(facetModelOpt.isPresent());
        long facetId = facetModelOpt.get().getFacetId();

        // Verify metadata entries exist and are consistent
        Optional<FacetMetadataModel> metaExprOpt = facetMetadataRepository.findByFacetIdAndKey(facetId, KEY_FACET_EXPRESSIONS);
        Optional<FacetMetadataModel> metaHashOpt = facetMetadataRepository.findByFacetIdAndKey(facetId, KEY_FACET_EXPRESSIONS_HASH);
        assertTrue(metaExprOpt.isPresent());
        assertTrue(metaHashOpt.isPresent());
        String storedJson = metaExprOpt.get().getValue();
        String storedHash = metaHashOpt.get().getValue();

        // Parse back and assert equivalence
        List<FacetExpressionDTO> parsed = objectMapper.readValue(storedJson, new TypeReference<>() {});
        assertEquals(1, parsed.size());
        assertEquals("B", parsed.get(0).exactly);
        assertEquals(1, parsed.get(0).node);

        // Hash should match SHA-256 of stored JSON
        assertEquals(sha256Hex(storedJson), storedHash);

        // Exactly one mapping should exist
        assertEquals(1L, facetConceptRepository.countForFacet(facetId));

        // Second load with identical expressions should not increase mappings (delta == 0)
        Result res2 = service.load(List.of(wrapper));
        // Find breakdown for our facet
        long delta = res2.facetMappings().stream()
                .filter(b -> b.facetName().equals("FacetOne"))
                .mapToLong(FacetMappingBreakdown::conceptsMapped)
                .sum();
        assertEquals(0L, delta);

        // Metadata should remain unchanged
        Optional<FacetMetadataModel> metaExprOpt2 = facetMetadataRepository.findByFacetIdAndKey(facetId, KEY_FACET_EXPRESSIONS);
        Optional<FacetMetadataModel> metaHashOpt2 = facetMetadataRepository.findByFacetIdAndKey(facetId, KEY_FACET_EXPRESSIONS_HASH);
        assertTrue(metaExprOpt2.isPresent());
        assertTrue(metaHashOpt2.isPresent());
        assertEquals(storedJson, metaExprOpt2.get().getValue());
        assertEquals(storedHash, metaHashOpt2.get().getValue());
        // Total mappings should still be 1
        assertEquals(1L, facetConceptRepository.countForFacet(facetId));
    }

    @Test
    void changeExpressions_thenEmpty_shouldClearAndRemap_andPersistUpdatedMetadata() throws Exception {
        // Seed 2 datasets and 2 concepts
        DatasetModel ds1 = datasetRepository.save(new DatasetModel("d1", "Dataset 1", "", ""));
        DatasetModel ds2 = datasetRepository.save(new DatasetModel("d2", "Dataset 2", "", ""));
        ConceptModel cABC = new ConceptModel(ds1.getDatasetId(), "d1", "d1", "", "\\A\\B\\C\\", null);
        ConceptModel cXYZ = new ConceptModel(ds2.getDatasetId(), "d2", "d2", "", "\\X\\Y\\Z\\", null);
        conceptService.save(cABC);
        conceptService.save(cXYZ);

        // Initial expressions -> match ABC (B at node 1)
        FacetExpressionDTO expB1 = new FacetExpressionDTO();
        expB1.exactly = "B";
        expB1.node = 1;
        FacetDTO facet = new FacetDTO();
        facet.name = "FacetTwo";
        facet.display = "Facet Two";
        facet.expressions = new ArrayList<>();
        facet.expressions.add(expB1);

        FacetCategoryDTO cat = new FacetCategoryDTO();
        cat.name = "Cat2";
        cat.display = "Cat2";
        cat.facets = List.of(facet);
        FacetCategoryWrapper wrapper = new FacetCategoryWrapper();
        wrapper.facetCategory = cat;

        service.load(List.of(wrapper));
        long facetId = facetRepository.findByName("FacetTwo").orElseThrow().getFacetId();
        assertEquals(1L, facetConceptRepository.countForFacet(facetId));

        String json1 = facetMetadataRepository.findByFacetIdAndKey(facetId, KEY_FACET_EXPRESSIONS).orElseThrow().getValue();
        String hash1 = facetMetadataRepository.findByFacetIdAndKey(facetId, KEY_FACET_EXPRESSIONS_HASH).orElseThrow().getValue();
        assertEquals(sha256Hex(json1), hash1);

        // Change expressions -> now match XYZ (X at node 0)
        FacetExpressionDTO expX0 = new FacetExpressionDTO();
        expX0.exactly = "X";
        expX0.node = 0;
        FacetDTO facetChanged = new FacetDTO();
        facetChanged.name = "FacetTwo"; // same facet name
        facetChanged.display = "Facet Two";
        facetChanged.expressions = new ArrayList<>();
        facetChanged.expressions.add(expX0);

        FacetCategoryDTO cat2 = new FacetCategoryDTO();
        cat2.name = "Cat2"; // same category
        cat2.display = "Cat2";
        cat2.facets = List.of(facetChanged);
        FacetCategoryWrapper wrapper2 = new FacetCategoryWrapper();
        wrapper2.facetCategory = cat2;

        Result resChange = service.load(List.of(wrapper2));
        long delta = resChange.facetMappings().stream()
                .filter(b -> b.facetName().equals("FacetTwo"))
                .mapToLong(FacetMappingBreakdown::conceptsMapped)
                .sum();
        // After clear, before=0, mapped to 1 new concept
        assertEquals(1L, delta);
        assertEquals(1L, facetConceptRepository.countForFacet(facetId));

        String json2 = facetMetadataRepository.findByFacetIdAndKey(facetId, KEY_FACET_EXPRESSIONS).orElseThrow().getValue();
        String hash2 = facetMetadataRepository.findByFacetIdAndKey(facetId, KEY_FACET_EXPRESSIONS_HASH).orElseThrow().getValue();
        assertNotEquals(json1, json2);
        assertNotEquals(hash1, hash2);
        assertEquals(sha256Hex(json2), hash2);

        // Now set expressions to empty -> should clear all mappings and store []
        FacetDTO facetEmpty = new FacetDTO();
        facetEmpty.name = "FacetTwo";
        facetEmpty.display = "Facet Two";
        facetEmpty.expressions = new ArrayList<>(); // empty
        FacetCategoryDTO cat3 = new FacetCategoryDTO();
        cat3.name = "Cat2";
        cat3.display = "Cat2";
        cat3.facets = List.of(facetEmpty);
        FacetCategoryWrapper wrapper3 = new FacetCategoryWrapper();
        wrapper3.facetCategory = cat3;

        Result resEmpty = service.load(List.of(wrapper3));
        long deltaEmpty = resEmpty.facetMappings().stream()
                .filter(b -> b.facetName().equals("FacetTwo"))
                .mapToLong(FacetMappingBreakdown::conceptsMapped)
                .sum();
        assertEquals(0L, deltaEmpty); // mapFacetToConcepts returns 0 for empty expressions
        assertEquals(0L, facetConceptRepository.countForFacet(facetId)); // mappings cleared

        String jsonEmpty = facetMetadataRepository.findByFacetIdAndKey(facetId, KEY_FACET_EXPRESSIONS).orElseThrow().getValue();
        String hashEmpty = facetMetadataRepository.findByFacetIdAndKey(facetId, KEY_FACET_EXPRESSIONS_HASH).orElseThrow().getValue();
        assertEquals("[]", jsonEmpty);
        assertEquals(sha256Hex("[]"), hashEmpty);
    }

    private static String sha256Hex(String s) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] digest = md.digest(s.getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder(digest.length * 2);
        for (byte b : digest) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}
