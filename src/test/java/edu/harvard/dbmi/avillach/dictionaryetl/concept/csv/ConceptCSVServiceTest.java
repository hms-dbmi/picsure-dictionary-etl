package edu.harvard.dbmi.avillach.dictionaryetl.concept.csv;

import edu.harvard.dbmi.avillach.dictionaryetl.Utility.DatabaseCleanupUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.testwrappers.TestConceptMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.testwrappers.TestConceptModel;
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
import java.util.Optional;
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
        this.databaseCleanupUtility.truncateTables();
        String csv = """
            dataset_ref,name,display,concept_type,concept_path,parent_concept_path,meta1,meta2
            a,n,N,Categorical,\\\\Foo\\\\,,val1,val2
            """;
        ConceptCSVManifest actualManifest = csvService.process(dataset, csv);
        csvService.linkConceptNodes();
        ConceptCSVManifest expectedManifest = new ConceptCSVManifest(1, 0, 1, 2, true, true, true);
        Assertions.assertEquals(expectedManifest, actualManifest);

        List<ConceptModel> actualConcepts = conceptRepository.findAll();
        List<ConceptModel> expectedConcepts = List.of(
            new ConceptModel(dataset.getDatasetId(), "n", "N", "Categorical", "\\Foo\\", null)
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
        this.databaseCleanupUtility.truncateTables();
        String csv = """
            dataset_ref,name,display,concept_type,concept_path,parent_concept_path,meta1,meta2
            a,n,N,Categorical,\\\\Foo\\\\,,val1,val2
            a,n1,N1,Categorical,\\\\Foo\\\\Bar\\\\,\\\\Foo\\\\,val1,val2
            a,n2,N2,Categorical,\\\\Foo\\\\Baz\\\\,\\\\Foo\\\\,val1,val2
            """;
        ConceptCSVManifest actualManifest = csvService.process(dataset, csv);
        csvService.linkConceptNodes();
        ConceptCSVManifest expectedManifest = new ConceptCSVManifest(3, 0, 3, 6, true, true, true);
        Assertions.assertEquals(expectedManifest, actualManifest);

        List<ConceptModel> actualConcepts = conceptRepository.findAll();
        List<ConceptModel> expectedConcepts = List.of(
            new ConceptModel(1L, dataset.getDatasetId(), "n", "N", "Categorical", "\\Foo\\", null),
            new ConceptModel(2L, dataset.getDatasetId(), "n1", "N1", "Categorical", "\\Foo\\Bar\\", 1L),
            new ConceptModel(3L, dataset.getDatasetId(), "n2", "N2", "Categorical", "\\Foo\\Baz\\", 1L)
        );
        compareConcepts(expectedConcepts, actualConcepts);
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
}