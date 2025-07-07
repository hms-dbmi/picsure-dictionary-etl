package edu.harvard.dbmi.avillach.dictionaryetl.concept.csv;

import edu.harvard.dbmi.avillach.dictionaryetl.Utility.DatabaseCleanupUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.testwrappers.TestConceptMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.testwrappers.TestConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.testwrappers.TestFacetCategoryModel;
import edu.harvard.dbmi.avillach.dictionaryetl.testwrappers.TestFacetModel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.*;
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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class ConceptCSVServiceTest {

    @Autowired
    private ConceptRepository conceptRepository;

    @Autowired
    private ConceptMetadataRepository conceptMetadataRepository;

    @Autowired
    private DatabaseCleanupUtility databaseCleanupUtility;

    @Autowired
    private FacetRepository facetRepository;

    @Autowired
    private FacetCategoryRepository facetCategoryRepository;

    @Autowired
    private DatasetRepository datasetRepository;

    @Autowired
    private ConceptCSVService csvService;

    private DatasetModel dataset;

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
    public void createDataSet() {
    }

    @BeforeEach
    void cleanDatabase() {
        this.databaseCleanupUtility.truncateTablesAllTables();
        dataset = datasetRepository.save(new DatasetModel("a", "A", "A", ""));
    }

    @Test
    void shouldProcessBasicCSV() {
        String csv = """
            dataset_ref,name,display,concept_type,concept_path,parent_concept_path,meta1,meta2
            a,n,N,Categorical,\\\\Foo\\\\,,val1,val2
            """;
        ConceptCSVManifest actualManifest = csvService.process(dataset, csv, List.of());
        csvService.linkConceptNodes();
        ConceptCSVManifest expectedManifest = new ConceptCSVManifest(1, 0, 1, 2, 1, true, true, true);
        Assertions.assertEquals(expectedManifest, actualManifest);

        List<ConceptModel> actualConcepts = conceptRepository.findAll();
        List<ConceptModel> expectedConcepts = List.of(
            new ConceptModel(1L, dataset.getDatasetId(), "n", "N", "Categorical", "\\Foo\\", null)
        );
        compareConcepts(expectedConcepts, actualConcepts);

        List<ConceptMetadataModel> actualMetas = conceptMetadataRepository.findAll();
        List<ConceptMetadataModel> expectedMetas = List.of(
            new ConceptMetadataModel(actualConcepts.get(0).getConceptNodeId(), "meta1", "val1"),
            new ConceptMetadataModel(actualConcepts.get(0).getConceptNodeId(), "meta2", "val2")
        );
        compareConceptMetas(expectedMetas, actualMetas);
    }

    @Test
    void shouldConstructHierarchy() {
        String csv = """
            dataset_ref,name,display,concept_type,concept_path,parent_concept_path,meta1,meta2
            a,n,N,Categorical,\\\\Foo\\\\,,val1,val2
            a,n1,N1,Categorical,\\\\Foo\\\\Bar\\\\,\\\\Foo\\\\,val1,val2
            a,n2,N2,Categorical,\\\\Foo\\\\Baz\\\\,\\\\Foo\\\\,val1,val2
            """;
        ConceptCSVManifest actualManifest = csvService.process(dataset, csv, List.of());
        csvService.linkConceptNodes();
        ConceptCSVManifest expectedManifest = new ConceptCSVManifest(3, 0, 3, 6, 1, true, true, true);
        Assertions.assertEquals(expectedManifest, actualManifest);

        List<ConceptModel> actualConcepts = conceptRepository.findAll();
        List<ConceptModel> expectedConcepts = List.of(
            new ConceptModel(1L, dataset.getDatasetId(), "n", "N", "Categorical", "\\Foo\\", null),
            new ConceptModel(2L, dataset.getDatasetId(), "n1", "N1", "Categorical", "\\Foo\\Bar\\", 1L),
            new ConceptModel(3L, dataset.getDatasetId(), "n2", "N2", "Categorical", "\\Foo\\Baz\\", 1L)
        );
        compareConcepts(expectedConcepts, actualConcepts);
    }

    @Test
    void shouldMakeBaseFacetsAndCategories() {
        String csv = """
            dataset_ref,name,display,concept_type,concept_path,parent_concept_path,meta1,meta2
            a,n,N,Categorical,\\\\Foo\\\\,,val1,val2
            a,n1,N1,Categorical,\\\\Foo\\\\Bar\\\\,\\\\Foo\\\\,val1,val2
            a,n2,N2,Categorical,\\\\Foo\\\\Bar\\\\Baz,\\\\Foo\\\\Bar\\\\,val1,val2
            """;
        ConceptCSVManifest actualManifest = csvService.process(dataset, csv, List.of());
        csvService.linkConceptNodes();
        ConceptCSVManifest expectedManifest = new ConceptCSVManifest(3, 0, 3, 6, 2, true, true, true);
        Assertions.assertEquals(expectedManifest, actualManifest);

        List<FacetCategoryModel> actualFacetCategories = facetCategoryRepository.findAll();
        List<FacetCategoryModel> expectedFacetCategories = List.of(
            new FacetCategoryModel("data_type", "Data Type", ""),
            new FacetCategoryModel("category", "Category", "")
        );
        compareFacetCategories(expectedFacetCategories, actualFacetCategories);
        List<FacetModel> actualFacets = facetRepository.findAll();
        List<FacetModel> expectedFacets = List.of(
            new FacetModel(actualFacetCategories.getFirst().getFacetCategoryId(), "data_type_categorical", "Categorical", "", null),
            new FacetModel(actualFacetCategories.getFirst().getFacetCategoryId(), "category_foo", "Foo", "", null)
        );
        compareFacets(expectedFacets, actualFacets);
    }

    @Test
    void shouldMakeBaseFacetsAndCategoriesAndMetaFacets() {
        String csv = """
            dataset_ref,name,display,concept_type,concept_path,parent_concept_path,meta1,meta2
            a,n,N,Categorical,\\\\Foo\\\\,,val1,val2
            a,n1,N1,Categorical,\\\\Foo\\\\Bar\\\\,\\\\Foo\\\\,val1,val2
            a,n2,N2,Categorical,\\\\Foo\\\\Bar\\\\Baz,\\\\Foo\\\\Bar\\\\,val1,val2
            """;
        ConceptCSVManifest actualManifest = csvService.process(dataset, csv, List.of("meta1"));
        csvService.linkConceptNodes();
        ConceptCSVManifest expectedManifest = new ConceptCSVManifest(3, 0, 3, 6, 3, true, true, true);
        Assertions.assertEquals(expectedManifest, actualManifest);

        List<FacetCategoryModel> actualFacetCategories = facetCategoryRepository.findAll();
        List<FacetCategoryModel> expectedFacetCategories = List.of(
            new FacetCategoryModel("meta1", "Meta1", ""),
            new FacetCategoryModel("data_type", "Data Type", ""),
            new FacetCategoryModel("category", "Category", "")
        );
        compareFacetCategories(expectedFacetCategories, actualFacetCategories);
        List<FacetModel> actualFacets = facetRepository.findAll();
        List<FacetModel> expectedFacets = List.of(
            new FacetModel(actualFacetCategories.getFirst().getFacetCategoryId(), "meta1_val1", "val1", "", null),
            new FacetModel(actualFacetCategories.getFirst().getFacetCategoryId(), "data_type_categorical", "Categorical", "", null),
            new FacetModel(actualFacetCategories.getFirst().getFacetCategoryId(), "category_foo", "Foo", "", null)
        );
        compareFacets(expectedFacets, actualFacets);
    }

    @Test
    void shouldHandleNestedFacets() {
        String csv = """
            dataset_ref,name,display,concept_type,concept_path,parent_concept_path,meta1,meta2
            a,n,N,Categorical,\\\\Foo\\\\,,val1,val2
            a,n1,N1,Categorical,\\\\Foo\\\\Bar\\\\,\\\\Foo\\\\,val1,val2
            a,n2,N2,Categorical,\\\\Foo\\\\Bar\\\\Baz\\\\,\\\\Foo\\\\Bar\\\\,val1,val2
            a,n3,N3,Categorical,\\\\Foo\\\\Bar\\\\Baz\\\\Qux\\\\,\\\\Foo\\\\Bar\\\\,val1,val2
            """;
        ConceptCSVManifest actualManifest = csvService.process(dataset, csv, List.of());
        csvService.linkConceptNodes();
        ConceptCSVManifest expectedManifest = new ConceptCSVManifest(4, 0, 4, 8, 3, true, true, true);
        Assertions.assertEquals(expectedManifest, actualManifest);

        List<FacetCategoryModel> actualFacetCategories = facetCategoryRepository.findAll();
        List<FacetCategoryModel> expectedFacetCategories = List.of(
            new FacetCategoryModel("data_type", "Data Type", ""),
            new FacetCategoryModel("category", "Category", "")
        );
        compareFacetCategories(expectedFacetCategories, actualFacetCategories);
        List<FacetModel> actualFacets = facetRepository.findAll();
        List<FacetModel> expectedFacets = List.of(
            new FacetModel(actualFacetCategories.getFirst().getFacetCategoryId(), "data_type_categorical", "Categorical", "", null),
            new FacetModel(actualFacetCategories.getFirst().getFacetCategoryId(), "category_foo", "Foo", "", null),
            new FacetModel(actualFacetCategories.getFirst().getFacetCategoryId(), "category_bar", "Bar", "", null)
        );
        compareFacets(expectedFacets, actualFacets);
    }

    private void compareFacets(List<? extends FacetModel> expected, List<? extends FacetModel> actual) {
        expected = expected.stream()
            .map(TestFacetModel::new)
            .sorted()
            .toList();
        actual = actual.stream()
            .map(TestFacetModel::new)
            .sorted()
            .toList();
        Assertions.assertEquals(expected, actual);
    }

    private void compareConcepts(List<? extends ConceptModel> expected, List<? extends ConceptModel> actual) {
        expected = expected.stream()
            .map(TestConceptModel::new)
            .sorted(Comparator.comparing(ConceptModel::getConceptPath))
            .toList();
        actual = actual.stream()
            .map(TestConceptModel::new)
            .sorted(Comparator.comparing(ConceptModel::getConceptPath))
            .toList();
        Assertions.assertEquals(expected, actual);
        // compare parent IDs via paths
        Map<Long, String> expectedPathIdMap = expected.stream()
            .collect(Collectors.toMap(ConceptModel::getConceptNodeId, ConceptModel::getConceptPath));
        Map<Long, String> actualPathIdMap =
            actual.stream().collect(Collectors.toMap(ConceptModel::getConceptNodeId, ConceptModel::getConceptPath));
        List<String> expectedParents = expected.stream().map(ConceptModel::getParentId).map(expectedPathIdMap::get).toList();
        List<String> actualParents = actual.stream().map(ConceptModel::getParentId).map(actualPathIdMap::get).toList();
        Assertions.assertEquals(expectedParents, actualParents);
    }

    private void compareConceptMetas(
        List<? extends ConceptMetadataModel> expected,
        List<? extends ConceptMetadataModel> actual
    ) {
        expected = expected.stream()
            .map(TestConceptMetadataModel::new)
            .sorted(Comparator.comparing(ConceptMetadataModel::toString))
            .toList();
        actual = actual.stream()
            .map(TestConceptMetadataModel::new)
            .sorted()
            .toList();
        Assertions.assertEquals(expected, actual);
    }

    private void compareFacetCategories(
        List<? extends FacetCategoryModel> expected,
        List<? extends FacetCategoryModel> actual
    ) {
        expected = expected.stream()
            .map(TestFacetCategoryModel::new)
            .sorted()
            .toList();
        actual = actual.stream()
            .map(TestFacetCategoryModel::new)
            .sorted()
            .toList();
        Assertions.assertEquals(expected, actual);
    }
}