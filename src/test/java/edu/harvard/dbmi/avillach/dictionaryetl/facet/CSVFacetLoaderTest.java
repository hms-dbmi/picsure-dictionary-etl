package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import com.opencsv.exceptions.CsvException;
import edu.harvard.dbmi.avillach.dictionaryetl.Utility.DatabaseCleanupUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptService;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetService;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryService;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.apache.tomcat.util.buf.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
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

import java.io.IOException;
import java.util.List;
import java.util.Optional;

@Testcontainers
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@SpringBootTest
public class CSVFacetLoaderTest {
    @Autowired
    FacetCategoryService facetCategoryService;
    @Autowired
    FacetService facetRepository;

    @Autowired
    FacetService facetService;

    @Autowired
    FacetConceptService facetConceptService;

    @Autowired
    FacetCategoryRepository facetCategoryRepository;

    @Autowired
    FacetConceptRepository facetConceptRepository;

    @Autowired
    private DatabaseCleanupUtility databaseCleanupUtility;

    @Autowired
    ConceptRepository conceptRepository;

    @Autowired
    private ConceptMetadataRepository conceptMetadataRepository;

    @Autowired
    DatasetRepository datasetRepository;

    @PersistenceContext
    private EntityManager entityManager;


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


    @Test
    void csvLoadFacetInfoFromCSVs() {
        String facetCategoryCSV = """
                name(unique),display name,description
                category,Category,Description of Category
                """;
        String facetCSV = """
                facet_category,facet_name(unique),display_name,description,parent_name
                category,parent,Parent Facet,description of parent1,
                category,child1,Child Facet 1,description of child1,parent
                category,child2,Child Facet 2,,parent
                """;
        String facetConceptCSV = """
                child1,child2
                \\\\ref\\\\concept1\\\\,\\\\ref\\\\concept2\\\\
                \\\\ref\\\\concept2\\\\,\\\\ref\\\\concept3\\\\
                \\\\ref\\\\concept3\\\\,
                """;

        DatasetModel dataset = new DatasetModel("ref", "", "", "");
        datasetRepository.save(dataset);
        ConceptModel concept1 = new ConceptModel(dataset.getDatasetId(), "concept1", "desc", "continuous", "\\ref\\concept1\\");
        conceptRepository.save(concept1);
        ConceptModel concept2 = new ConceptModel(dataset.getDatasetId(), "concept2", "desc", "continuous", "\\ref\\concept2\\");
        conceptRepository.save(concept2);
        ConceptModel concept3 = new ConceptModel(dataset.getDatasetId(), "concept3", "desc", "continuous", "\\ref\\concept3\\");
        conceptRepository.save(concept3);

        Assertions.assertDoesNotThrow(() ->
                {
                    facetCategoryService.updateFacetCategoriesFromCSVs(facetCategoryCSV);
                    facetService.updateFacetsFromCSVs(facetCSV);
                    facetConceptService.updateFacetConceptMappingsFromCSVs(facetConceptCSV);
                }

        );
        List<FacetCategoryModel> categoryList = facetCategoryRepository.findAll();
        Assertions.assertEquals(1, categoryList.size());
        FacetCategoryModel category = categoryList.getFirst();
        Assertions.assertEquals("category", category.getName());
        Assertions.assertEquals("Category", category.getDisplay());
        Assertions.assertEquals("Description of Category", category.getDescription());
        List<FacetModel> facetList = facetRepository.findAll();
        Assertions.assertEquals(3, facetList.size());
        ;
        //Wrapping in a doesNotThrow to catch if any of the facets are missing
        Assertions.assertDoesNotThrow(() ->
                {
                    FacetModel parentFacet = facetService.findByName("parent").get();
                    FacetModel childFacet1 = facetService.findByName("child1").get();
                    FacetModel childFacet2 = facetService.findByName("child2").get();
                    Assertions.assertEquals(parentFacet.getFacetId(), childFacet1.getParentId());
                    Assertions.assertEquals(childFacet1.getParentId(), childFacet2.getParentId());
                    Assertions.assertEquals(3, facetConceptRepository.findByFacetId(childFacet1.getFacetId()).get().size());
                    Assertions.assertEquals(2, facetConceptRepository.findByFacetId(childFacet2.getFacetId()).get().size());
                    Assertions.assertEquals(3, facetConceptRepository.findByFacetId(parentFacet.getFacetId()).get().size());
                }

        );


    }


}
