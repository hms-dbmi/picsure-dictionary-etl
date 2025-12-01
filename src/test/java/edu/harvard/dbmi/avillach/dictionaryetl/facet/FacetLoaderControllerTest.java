package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.harvard.dbmi.avillach.dictionaryetl.Utility.DatabaseCleanupUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptService;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.model.FacetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryMeta;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryMetaRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.FacetCategoryWrapper;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.Result;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;
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
class FacetLoaderControllerTest {

    @Autowired
    private DatabaseCleanupUtility databaseCleanupUtility;

    @Autowired
    private FacetController controller;

    @Autowired
    private FacetRepository facetRepository;

    @Autowired
    private FacetCategoryRepository facetCategoryRepository;

    @Autowired
    private FacetCategoryMetaRepository facetCategoryMetaRepository;

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
    void postPayload_shouldCreateRecords() throws Exception {
        String json = "[\n" +
                "  {\n" +
                "    \"Facet_Category\": {\n" +
                "      \"Name\": \"Consortium_Curated_Facets\",\n" +
                "      \"Display\": \"Consortium Curated Facets\",\n" +
                "      \"Description\": \"Consortium Curated Facets Description\",\n" +
                "      \"Facets\": [\n" +
                "        {\n" +
                "          \"Name\": \"Recover Adult\",\n" +
                "          \"Display\": \"RECOVER Adult\",\n" +
                "          \"Description\": \"Recover adult parent facet.\",\n" +
                "          \"Facets\": [\n" +
                "            {\n" +
                "              \"Name\": \"Infected\",\n" +
                "              \"Display\": \"Infected\",\n" +
                "              \"Description\": \"Infected Facet Description\"\n" +
                "            }\n" +
                "          ]\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  }\n" +
                "]";

        List<FacetCategoryWrapper> payload = objectMapper.readValue(
                json, new TypeReference<List<FacetCategoryWrapper>>(){});

        ResponseEntity<Result> response = controller.load(payload);
        Assertions.assertEquals(200, response.getStatusCode().value());
        Result result = response.getBody();
        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.categoriesCreated());
        Assertions.assertEquals(2, result.facetsCreated());

        Optional<FacetCategoryModel> cat = facetCategoryRepository.findByName("Consortium_Curated_Facets");
        Assertions.assertTrue(cat.isPresent());
        Optional<FacetModel> parentFacet = facetRepository.findByName("Recover Adult");
        Optional<FacetModel> infectedFacet = facetRepository.findByName("Infected");
        Assertions.assertTrue(parentFacet.isPresent());
        Assertions.assertTrue(infectedFacet.isPresent());
        Assertions.assertEquals(parentFacet.get().getFacetId(), infectedFacet.get().getParentId());
    }

    @Test
    void postPayload_shouldCreateMetadataRecords() throws Exception {
        String json = "[\n" +
                "  {\n" +
                "    \"Facet_Category\": {\n" +
                "      \"Name\": \"Consortium_Curated_Facets\",\n" +
                "      \"Display\": \"Consortium Curated Facets\",\n" +
                "      \"Description\": \"Consortium Curated Facets Description\",\n" +
                "      \"Metadata\": [\n" +
                "        { \"key\": \"Some Key\", \"value\": \"some value\" }\n" +
                "      ],\n" +
                "      \"Facets\": [\n" +
                "        {\n" +
                "          \"Name\": \"Recover Adult\",\n" +
                "          \"Display\": \"RECOVER Adult\",\n" +
                "          \"Description\": \"Recover adult parent facet.\",\n" +
                "          \"Facets\": [\n" +
                "            {\n" +
                "              \"Name\": \"Infected\",\n" +
                "              \"Display\": \"Infected\",\n" +
                "              \"Description\": \"Infected Facet Description\"\n" +
                "            }\n" +
                "          ]\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  }\n" +
                "]";

        List<FacetCategoryWrapper> payload = objectMapper.readValue(
                json, new TypeReference<>(){});

        ResponseEntity<Result> response = controller.load(payload);
        Assertions.assertEquals(200, response.getStatusCode().value());
        Optional<FacetCategoryModel> cat = facetCategoryRepository.findByName("Consortium_Curated_Facets");
        Assertions.assertTrue(cat.isPresent());
        Optional<FacetCategoryMeta> metadata = facetCategoryMetaRepository.findFacetCategoryMetaByCategoryId(cat.get().getFacetCategoryId(), "Some Key");
        Assertions.assertTrue(metadata.isPresent());
        Assertions.assertEquals("some value", metadata.get().getValue());
    }
}
