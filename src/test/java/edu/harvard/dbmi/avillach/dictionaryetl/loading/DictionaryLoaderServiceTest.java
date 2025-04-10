package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import edu.harvard.dbmi.avillach.dictionaryetl.Utility.ColumnMetaUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.Utility.DatabaseCleanupUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataService;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptService;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@ActiveProfiles("test")
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class DictionaryLoaderServiceTest {

    private static String nhanesFilePath;
    private static String resourcePath;
    private static String thousandGenomesFilePath;
    private static String syntheaFilePath;

    @Autowired
    private DictionaryLoaderService dictionaryLoaderService;

    @Autowired
    private ColumnMetaMapper columnMetaMapper;

    @Autowired
    private DatasetService datasetService;

    @Autowired
    private ConceptService conceptService;

    @Autowired
    private ColumnMetaUtility columnMetaUtility;

    @Autowired
    private ConceptMetadataService conceptMetadataService;

    @Autowired
    private DatabaseCleanupUtility databaseCleanupUtility;


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
    public static void init() throws IOException {
        ClassPathResource columnMetaResource = new ClassPathResource("columnMeta.csv");
        assertNotNull(columnMetaResource);
        nhanesFilePath = columnMetaResource.getFile().toPath().toString();

        ClassPathResource genomesResource = new ClassPathResource("columnMeta_1000_genomes.csv");
        assertNotNull(genomesResource);
        thousandGenomesFilePath = genomesResource.getFile().toPath().toString();

        ClassPathResource syntheaResource = new ClassPathResource("columnMeta_synthea.csv");
        assertNotNull(syntheaResource);
        syntheaFilePath = syntheaResource.getFile().toPath().toString();

        Path testResourcePath = Paths.get(System.getProperty("user.dir"), "src", "test", "resources");
        resourcePath = testResourcePath.toString();

    }

    @BeforeEach
    void cleanDatabase() {
        this.databaseCleanupUtility.truncateTables();
    }

    @Test
    void processColumnMetaCSV() {
        // The error file should be written to your resources directory.
        assertDoesNotThrow(() -> this.dictionaryLoaderService.processColumnMetaCSV(nhanesFilePath, resourcePath +
                                                                                                         "/columnMetaErrors" +
                                                                                                         ".csv"));
    }

    @Test
    void shouldProduceConceptHierarchy() {
        String examinationConceptPath = "\\examination\\physical fitness\\Recovery 2 diastolic BP (mm Hg)\\";
        ConceptNode conceptNode = this.dictionaryLoaderService.buildConceptHierarchy(examinationConceptPath);
        assertEquals("\\examination\\", conceptNode.getConceptPath());
        System.out.println(conceptNode.getConceptPath());
        assertEquals("\\examination\\physical fitness\\", conceptNode.getChild().getConceptPath());
        System.out.println(conceptNode.getChild().getConceptPath());
        assertEquals("\\examination\\physical fitness\\Recovery 2 diastolic BP (mm Hg)\\",
                conceptNode.getChild().getChild().getConceptPath());
        System.out.println(conceptNode.getChild().getChild().getConceptPath());
    }

    @Test
    void shouldFlattenConceptMeta_demographics() {
        List<ColumnMeta> columnMetas = new ArrayList<>();
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta("\\demographics\\area\\1_10\\,4,0,true,1_10,null,null," +
                                                               "4660968,4665205,82,82").get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta("\\demographics\\area\\1_11\\,4,0,true,1_11,null,null," +
                                                               "4665205,4670139,99,99").get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta("\\demographics\\area\\1_12\\,4,0,true,1_12,null,null," +
                                                               "4670139,4675237,103,103").get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta("\\demographics\\area\\1_13\\,4,0,true,1_13,null,null," +
                                                               "4675237,4678695,63,63").get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta("\\demographics\\area\\1_14\\,4,0,true,1_14,null,null," +
                                                               "4678695,4682727,77,77").get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta("\\demographics\\area\\1_15\\,4,0,true,1_15,null,null," +
                                                               "4682727,4688727,125,125").get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta("\\demographics\\area\\1_16\\,4,0,true,1_16,null,null," +
                                                               "4688727,4694276,114,114").get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta("\\demographics\\area\\1_17\\,4,0,true,1_17,null,null," +
                                                               "4694276,4699538,107,107").get());
        ColumnMeta columnMeta = this.dictionaryLoaderService.flattenCategoricalColumnMeta(columnMetas);
        assertNotNull(columnMeta);
        assertEquals("\\demographics\\area\\", columnMeta.name());
        List<String> valuesList = new ArrayList<>(List.of("1_10", "1_11", "1_12", "1_13", "1_14", "1_15", "1_16", "1_17"));
        Collections.reverse(valuesList);
        assertEquals(valuesList,
                columnMeta.categoryValues());
        System.out.println(columnMeta);
    }

    @Test
    void shouldFlattenConceptMeta_Sex() {
        List<ColumnMeta> columnMetas = new ArrayList<>();
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta("\\demographics\\SEX\\female\\,6,0,true,female,null," +
                                                               "null,3664589,3885367,5114,5114").get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta("\\demographics\\SEX\\male\\,4,0,true,male,null,null," +
                                                               "3885367,4086526,4885,4885").get());

        ColumnMeta columnMeta = this.dictionaryLoaderService.flattenCategoricalColumnMeta(columnMetas);
        assertNotNull(columnMeta);
        assertEquals("\\demographics\\SEX\\", columnMeta.name());
        assertEquals(List.of("female", "male"), columnMeta.categoryValues());
        assertTrue(columnMeta.categorical());
    }

    @Test
    void shouldProduceValidValuesMetadata_Categorical() {
        List<ColumnMeta> columnMetas = new ArrayList<>();
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta("\\demographics\\area\\1_10\\,4,0,true,1_10,null,null," +
                                                               "4660968,4665205,82,82").get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta("\\demographics\\area\\1_11\\,4,0,true,1_11,null,null," +
                                                               "4665205,4670139,99,99").get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta("\\demographics\\area\\1_12\\,4,0,true,1_12,null,null," +
                                                               "4670139,4675237,103,103").get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta("\\demographics\\area\\1_13\\,4,0,true,1_13,null,null," +
                                                               "4675237,4678695,63,63").get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta("\\demographics\\area\\1_14\\,4,0,true,1_14,null,null," +
                                                               "4678695,4682727,77,77").get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta("\\demographics\\area\\1_15\\,4,0,true,1_15,null,null," +
                                                               "4682727,4688727,125,125").get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta("\\demographics\\area\\1_16\\,4,0,true,1_16,null,null," +
                                                               "4688727,4694276,114,114").get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta("\\demographics\\area\\1_17\\,4,0,true,1_17,null,null," +
                                                               "4694276,4699538,107,107").get());
        ColumnMeta columnMeta = this.dictionaryLoaderService.flattenCategoricalColumnMeta(columnMetas);

        DatasetModel dataset = new DatasetModel("TEST", "", "", "");
        dataset = this.datasetService.save(dataset);

        ConceptModel concept = new ConceptModel(dataset.getDatasetId(), "area", "", "categorical", columnMeta.name(), null);
        concept = this.conceptService.save(concept);

        this.dictionaryLoaderService.buildValuesMetadata(columnMeta, concept.getConceptNodeId());
        List<ConceptMetadataModel> metadataModel =
                this.conceptMetadataService.findByConceptID(concept.getConceptNodeId());
        assertNotNull(metadataModel);
        assertFalse(metadataModel.isEmpty());
        assertEquals(metadataModel.getFirst().getValue(), columnMeta.categoryValues().toString());

        Optional<ConceptMetadataModel> byID =
                this.conceptMetadataService.findByID(metadataModel.getFirst().getConceptMetaId());
        assertTrue(byID.isPresent());
        ConceptMetadataModel loadedMeta = byID.get();
        List<String> strings = this.columnMetaUtility.parseValues(loadedMeta.getValue());
        List<String> valuesList = new ArrayList<>(List.of("1_10", "1_11", "1_12", "1_13", "1_14", "1_15", "1_16", "1_17"));
        Collections.reverse(valuesList);
        assertEquals(valuesList, strings);
    }

    @Test
    void shouldProduceValidValuesMetadata_Continuous() {
        List<ColumnMeta> columnMetas = new ArrayList<>();
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta("\\examination\\body measures\\Waist " +
                                                                    "Circumference (cm)\\,8,0,false,,32.0,170.7," +
                                                                    "10198148,10514943,8317,8317").get());
        ColumnMeta columnMeta = this.dictionaryLoaderService.flattenCategoricalColumnMeta(columnMetas);

        DatasetModel dataset = new DatasetModel("TEST2", "", "", "");
        dataset = this.datasetService.save(dataset);

        ConceptModel concept = new ConceptModel(dataset.getDatasetId(), "area2", "", "categorical", columnMeta.name()
                , null);
        concept = this.conceptService.save(concept);

        this.dictionaryLoaderService.buildValuesMetadata(columnMeta, concept.getConceptNodeId());
        List<ConceptMetadataModel> metadataModel = this.conceptMetadataService.findByConceptID(concept.getConceptNodeId());
        assertNotNull(metadataModel);
        assertFalse(metadataModel.isEmpty());

        Optional<ConceptMetadataModel> byID =
                this.conceptMetadataService.findByID(metadataModel.getFirst().getConceptMetaId());
        assertTrue(byID.isPresent());
        ConceptMetadataModel loadedMeta = byID.get();
        Float min = this.columnMetaUtility.parseMin(loadedMeta.getValue());
        Float max = this.columnMetaUtility.parseMax(loadedMeta.getValue());
        assertNotNull(min);
        assertNotNull(max);
        assertEquals(32.0f, min);
        assertEquals(170.7f, max);
    }

    @Test
    void shouldProcessColumnMetas_Categorical() {
        List<ColumnMeta> columnMetas = new ArrayList<>();
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta("\\laboratory\\acrylamide\\Acrylamide (pmoL per G" +
                                                                    " Hb)\\100\\,3,0,true,100,null,null,12066159," +
                                                                    "12067144,2,2").get());
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta("\\laboratory\\acrylamide\\Acrylamide (pmoL per G" +
                                                                    " Hb)\\101\\,3,0,true,101,null,null,12067144," +
                                                                    "12068169,3,3").get());
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta("\\laboratory\\acrylamide\\Acrylamide (pmoL per G" +
                                                                    " Hb)\\103\\,3,0,true,103,null,null,12068169," +
                                                                    "12069274,5,5").get());
        this.dictionaryLoaderService.processColumnMetas(columnMetas);
        Optional<ConceptModel> demographics = this.conceptService.findByConcept("\\laboratory\\");
        assertTrue(demographics.isPresent());
        assertEquals("laboratory", demographics.get().getName());

        Optional<ConceptModel> demographicsArea =
                this.conceptService.findByConcept("\\laboratory\\acrylamide\\Acrylamide (pmoL per G Hb)\\");
        assertTrue(demographicsArea.isPresent());
        assertEquals("Acrylamide (pmoL per G Hb)", demographicsArea.get().getName());

        List<ConceptMetadataModel> demographicsAreaMeta =
                this.conceptMetadataService.findByConceptID(demographicsArea.get().getConceptNodeId());
        assertFalse(demographicsAreaMeta.isEmpty());
        assertEquals("values", demographicsAreaMeta.getFirst().getKey());
        assertEquals(List.of("100", "101", "103"),
                this.columnMetaUtility.parseValues(demographicsAreaMeta.getFirst().getValue()));
    }

    @Test
    void shouldProcessColumMetas_Continuous() {
        List<ColumnMeta> columnMetas = new ArrayList<>();
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta("\"\\laboratory\\biochemistry\\Creatinine, urine " +
                                                                    "(umol per L)\\\",8,0,false,,0.0,68422.0," +
                                                                    "28621157,29175570,14570,9068").get());
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta("\"\\laboratory\\biochemistry\\Creatinine, urine (umol per L)\\10078\\\",5,0,true,10078,null,null,29175570,29176735,6,6").get());
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta("\"\\laboratory\\biochemistry\\Creatinine, urine (umol per L)\\10166\\\",5,0,true,10166,null,null,29176735,29178026,9,9").get());
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta("\"\\laboratory\\biochemistry\\Creatinine, urine " +
                                                                    "(umol per L)\\10254\\\",5,0,true,10254,null," +
                                                                    "null,29178026,29179443,12,12").get());

        this.dictionaryLoaderService.processColumnMetas(columnMetas);
        Optional<ConceptModel> byConcept = this.conceptService.findByConcept("\\laboratory\\biochemistry\\Creatinine, urine (umol per L)\\");
        assertTrue(byConcept.isPresent());

        List<ConceptMetadataModel> conceptMetadata =
                this.conceptMetadataService.findByConceptID(byConcept.get().getConceptNodeId());
        assertFalse(conceptMetadata.isEmpty());
        ConceptMetadataModel metadata = conceptMetadata.getFirst();
        Float max = this.columnMetaUtility.parseMax(metadata.getValue());
        Float min = this.columnMetaUtility.parseMin(metadata.getValue());

        assertEquals(0.0f, min);
        assertEquals(68422.0f, max);
    }

    @Test
    void shouldProcessColumMetas_Continuous_IncreasedMax() {
        List<ColumnMeta> columnMetas = new ArrayList<>();
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta("\"\\laboratory\\biochemistry\\Creatinine, urine " +
                                                                    "(umol per L)\\\",8,0,false,,0.0,68422.0," +
                                                                    "28621157,29175570,14570,9068").get());
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta("\"\\laboratory\\biochemistry\\Creatinine, urine (umol per L)\\10078\\\",5,0,true,10078,null,null,29175570,29176735,6,6").get());
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta("\"\\laboratory\\biochemistry\\Creatinine, urine " +
                                                                    "(umol per L)\\10166.0\\\",5,0,true,10166.0,null," +
                                                                    "null,29176735,29178026,9,9").get());
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta("\"\\laboratory\\biochemistry\\Creatinine, urine " +
                                                                    "(umol per L)\\76543\\\",5,0,true,76543,null," +
                                                                    "null,29178026,29179443,12,12").get());

        this.dictionaryLoaderService.processColumnMetas(columnMetas);
        Optional<ConceptModel> byConcept = this.conceptService.findByConcept("\\laboratory\\biochemistry\\Creatinine, urine (umol per L)\\");
        assertTrue(byConcept.isPresent());

        List<ConceptMetadataModel> conceptMetadata =
                this.conceptMetadataService.findByConceptID(byConcept.get().getConceptNodeId());
        assertFalse(conceptMetadata.isEmpty());
        ConceptMetadataModel metadata = conceptMetadata.getFirst();
        Float max = this.columnMetaUtility.parseMax(metadata.getValue());
        Float min = this.columnMetaUtility.parseMin(metadata.getValue());

        assertEquals(0.0f, min);
        assertEquals(76543.0f, max);
    }

    @Test
    void shouldProcessColumMetas_Continuous_DecreasedMin() {
        List<ColumnMeta> columnMetas = new ArrayList<>();
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta("\"\\laboratory\\biochemistry\\Creatinine, urine " +
                                                                    "(umol per L)\\\",8,0,false,,0.0,68422.0," +
                                                                    "28621157,29175570,14570,9068").get());
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta("\"\\laboratory\\biochemistry\\Creatinine, urine (umol per L)\\10078\\\",5,0,true,10078,null,null,29175570,29176735,6,6").get());
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta("\"\\laboratory\\biochemistry\\Creatinine, urine " +
                                                                    "(umol per L)\\10.0\\\",5,0,true,-10.0,null," +
                                                                    "null,29176735,29178026,9,9").get());
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta("\"\\laboratory\\biochemistry\\Creatinine, urine " +
                                                                    "(umol per L)\\76543\\\",5,0,true,76543,null," +
                                                                    "null,29178026,29179443,12,12").get());

        this.dictionaryLoaderService.processColumnMetas(columnMetas);
        Optional<ConceptModel> byConcept = this.conceptService.findByConcept("\\laboratory\\biochemistry\\Creatinine, urine (umol per L)\\");
        assertTrue(byConcept.isPresent());

        List<ConceptMetadataModel> conceptMetadata =
                this.conceptMetadataService.findByConceptID(byConcept.get().getConceptNodeId());
        assertFalse(conceptMetadata.isEmpty());
        ConceptMetadataModel metadata = conceptMetadata.getFirst();
        Float max = this.columnMetaUtility.parseMax(metadata.getValue());
        Float min = this.columnMetaUtility.parseMin(metadata.getValue());

        assertEquals(-10.0f, min);
        assertEquals(76543.0f, max);
    }

    @Test
    void shouldProcessThousandGenomes() {
        assertDoesNotThrow(() -> this.dictionaryLoaderService.processColumnMetaCSV(thousandGenomesFilePath,resourcePath +
                                                                                                                      "/columnMetaErrors" +
                                                                                                                      ".csv"));

        List<ConceptModel> all = this.conceptService.findAll();
        assertFalse(all.isEmpty());
        assertTrue(all.size() >= 25);
    }

    @Test
    void shouldProcessSynthea() {
        assertDoesNotThrow(() -> this.dictionaryLoaderService.processColumnMetaCSV(syntheaFilePath, resourcePath +
                                                                                                              "/columnMetaErrors" +
                                                                                                              ".csv"));

        List<ConceptModel> all = this.conceptService.findAll();
        assertFalse(all.isEmpty());
        assertTrue(all.size() >= 32);
    }

}