package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import edu.harvard.dbmi.avillach.dictionaryetl.Utility.ColumnMetaUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.Utility.DatabaseCleanupUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.*;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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

//    @Test
//    void shouldProduceConceptHierarchy() {
//        String examinationConceptPath = "\\examination\\physical fitness\\Recovery 2 diastolic BP (mm Hg)\\";
//        ConceptNode conceptNode = this.dictionaryLoaderService.buildConceptHierarchy(examinationConceptPath);
//        assertEquals("\\examination\\", conceptNode.getConceptPath());
//        System.out.println(conceptNode.getConceptPath());
//        assertEquals("\\examination\\physical fitness\\", conceptNode.getChild().getConceptPath());
//        System.out.println(conceptNode.getChild().getConceptPath());
//        assertEquals("\\examination\\physical fitness\\Recovery 2 diastolic BP (mm Hg)\\",
//                conceptNode.getChild().getChild().getConceptPath());
//        System.out.println(conceptNode.getChild().getChild().getConceptPath());
//    }

    @Test
    void shouldFlattenConceptMeta_demographics() throws IOException {
        List<ColumnMeta> columnMetas = new ArrayList<>();
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta(
                columnMetaMapper.getParser().parseLine("\\demographics\\area\\1_10\\,4,0,true,1_10,null,null," +
                                                                        "4660968,4665205,82,82")).get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta(
                columnMetaMapper.getParser().parseLine("\\demographics\\area\\1_11\\,4,0,true,1_11,null,null," +
                                                                        "4665205,4670139,99,99")).get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta(
                columnMetaMapper.getParser().parseLine("\\demographics\\area\\1_12\\,4,0,true,1_12,null,null," +
                                                                        "4670139,4675237,103,103")).get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta(
                columnMetaMapper.getParser().parseLine("\\demographics\\area\\1_13\\,4,0,true,1_13,null,null," +
                                                                        "4675237,4678695,63,63")).get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta(
                columnMetaMapper.getParser().parseLine("\\demographics\\area\\1_14\\,4,0,true,1_14,null,null," +
                                                                        "4678695,4682727,77,77")).get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta(
                columnMetaMapper.getParser().parseLine("\\demographics\\area\\1_15\\,4,0,true,1_15,null,null," +
                                                                        "4682727,4688727,125,125")).get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta(
                columnMetaMapper.getParser().parseLine("\\demographics\\area\\1_16\\,4,0,true,1_16,null,null," +
                                                                        "4688727,4694276,114,114")).get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta(
                columnMetaMapper.getParser().parseLine("\\demographics\\area\\1_17\\,4,0,true,1_17,null,null," +
                                                                        "4694276,4699538,107,107")).get());
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
    void shouldFlattenConceptMeta_Sex() throws IOException {
        List<ColumnMeta> columnMetas = new ArrayList<>();
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta(
                columnMetaMapper.getParser().parseLine("\\demographics\\SEX\\female\\,6,0,true,female,null," +
                                                                        "null,3664589,3885367,5114,5114")).get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta(
                columnMetaMapper.getParser().parseLine("\\demographics\\SEX\\male\\,4,0,true,male,null,null," +
                                                                        "3885367,4086526,4885,4885")).get());

        ColumnMeta columnMeta = this.dictionaryLoaderService.flattenCategoricalColumnMeta(columnMetas);
        assertNotNull(columnMeta);
        assertEquals("\\demographics\\SEX\\", columnMeta.name());
        assertEquals(List.of("female", "male"), columnMeta.categoryValues());
        assertTrue(columnMeta.categorical());
    }

    @Test
    void shouldProduceValidValuesMetadata_Categorical() throws IOException {
        List<ColumnMeta> columnMetas = new ArrayList<>();
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta(
                columnMetaMapper.getParser().parseLine("\\demographics\\area\\1_10\\,4,0,true,1_10,null,null," +
                                                                        "4660968,4665205,82,82")).get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta(
                columnMetaMapper.getParser().parseLine("\\demographics\\area\\1_11\\,4,0,true,1_11,null,null," +
                                                                        "4665205,4670139,99,99")).get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta(
                columnMetaMapper.getParser().parseLine("\\demographics\\area\\1_12\\,4,0,true,1_12,null,null," +
                                                                        "4670139,4675237,103,103")).get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta(
                columnMetaMapper.getParser().parseLine("\\demographics\\area\\1_13\\,4,0,true,1_13,null,null," +
                                                                        "4675237,4678695,63,63")).get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta(
                columnMetaMapper.getParser().parseLine("\\demographics\\area\\1_14\\,4,0,true,1_14,null,null," +
                                                                        "4678695,4682727,77,77")).get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta(
                columnMetaMapper.getParser().parseLine("\\demographics\\area\\1_15\\,4,0,true,1_15,null,null," +
                                                                        "4682727,4688727,125,125")).get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta(
                columnMetaMapper.getParser().parseLine("\\demographics\\area\\1_16\\,4,0,true,1_16,null,null," +
                                                                        "4688727,4694276,114,114")).get());
        columnMetas.add(columnMetaMapper.mapCSVRowToColumnMeta(
                columnMetaMapper.getParser().parseLine("\\demographics\\area\\1_17\\,4,0,true,1_17,null,null," +
                                                                        "4694276,4699538,107,107")).get());
        ColumnMeta columnMeta = this.dictionaryLoaderService.flattenCategoricalColumnMeta(columnMetas);

        DatasetModel dataset = new DatasetModel("TEST", "", "", "");
        dataset = this.datasetService.save(dataset);

        ConceptModel concept = new ConceptModel(dataset.getDatasetId(), "area", "", "categorical", columnMeta.name(), null);
        concept = this.conceptService.save(concept);

//        this.dictionaryLoaderService.buildValuesMetadata(columnMeta, concept.getConceptNodeId());
        List<ConceptMetadataModel> metadataModel =
                this.conceptMetadataService.findByConceptID(concept.getConceptNodeId());
        assertNotNull(metadataModel);
        assertFalse(metadataModel.isEmpty());
        assertEquals(this.columnMetaUtility.parseValues(metadataModel.getFirst().getValue()).toString(), columnMeta.categoryValues().toString());

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
    void shouldProduceValidValuesMetadata_Continuous() throws IOException {
        List<ColumnMeta> columnMetas = new ArrayList<>();
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta(
                this.columnMetaMapper.getParser().parseLine("\\examination\\body measures\\Waist " +
                                                                             "Circumference (cm)\\,8,0,false,,32.0,170.7," +
                                                                             "10198148,10514943,8317,8317")).get());
        ColumnMeta columnMeta = this.dictionaryLoaderService.flattenCategoricalColumnMeta(columnMetas);

        DatasetModel dataset = new DatasetModel("TEST2", "", "", "");
        dataset = this.datasetService.save(dataset);

        ConceptModel concept = new ConceptModel(dataset.getDatasetId(), "area2", "", "categorical", columnMeta.name()
                , null);
        concept = this.conceptService.save(concept);

//        this.dictionaryLoaderService.buildValuesMetadata(columnMeta, concept.getConceptNodeId());
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
    void shouldProcessColumnMetas_Categorical() throws IOException {
        List<ColumnMeta> columnMetas = new ArrayList<>();
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta(
                this.columnMetaMapper.getParser().parseLine("\\laboratory\\acrylamide\\Acrylamide (pmoL per G" +
                                                                             " Hb)\\100\\,3,0,true,100,null,null,12066159," +
                                                                             "12067144,2,2")).get());
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta(
                this.columnMetaMapper.getParser().parseLine("\\laboratory\\acrylamide\\Acrylamide (pmoL per G" +
                                                                             " Hb)\\101\\,3,0,true,101,null,null,12067144," +
                                                                             "12068169,3,3")).get());
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta(
                this.columnMetaMapper.getParser().parseLine("\\laboratory\\acrylamide\\Acrylamide (pmoL per G" +
                                                                             " Hb)\\103\\,3,0,true,103,null,null,12068169," +
                                                                             "12069274,5,5")).get());
        this.dictionaryLoaderService.processColumnMetas(columnMetas);

        Optional<ConceptModel> laboratory = this.conceptService.findByConcept("\\laboratory\\");
        assertTrue(laboratory.isPresent());
        assertEquals("laboratory", laboratory.get().getName());

        Optional<ConceptModel> acrylamideGHB =
                this.conceptService.findByConcept("\\laboratory\\acrylamide\\Acrylamide (pmoL per G Hb)\\");
        assertTrue(acrylamideGHB.isPresent());
        assertEquals("Acrylamide (pmoL per G Hb)", acrylamideGHB.get().getName());

        Optional<ConceptModel> acrylamide = this.conceptService.findByConcept("\\laboratory\\acrylamide\\");
        assertTrue(acrylamide.isPresent());
        assertEquals("acrylamide", acrylamide.get().getName());

        List<ConceptMetadataModel> acrylamideMeta =
                this.conceptMetadataService.findByConceptID(acrylamideGHB.get().getConceptNodeId());
        assertFalse(acrylamideMeta.isEmpty());
        assertEquals("values", acrylamideMeta.getFirst().getKey());
        assertEquals(List.of("100", "101", "103"),
                this.columnMetaUtility.parseValues(acrylamideMeta.getFirst().getValue()));
    }

    @Test
    void shouldProcessColumnMetas_SingleRow_Categorical() throws IOException {
        List<ColumnMeta> columnMetas = new ArrayList<>();
        String csvRow = "\\questionnaire\\pharmaceutical\\HYDROCHLOROTHIAZIDE__LOSARTAN\\,1,0,true,0µ1,null,null,182782403,182875293,2420,2420";
        // Apply parsing before wrapping in ColumnMeta
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta(
                this.columnMetaMapper.getParser().parseLine(csvRow)).get());
        this.dictionaryLoaderService.processColumnMetas(columnMetas);
        Optional<ConceptModel> questionnaire = this.conceptService.findByConcept("\\questionnaire\\");
        assertTrue(questionnaire.isPresent());
        assertEquals("questionnaire", questionnaire.get().getName());

        Optional<ConceptModel> questionnaireLosartan =
                this.conceptService.findByConcept("\\questionnaire\\pharmaceutical\\HYDROCHLOROTHIAZIDE__LOSARTAN\\");
        assertTrue(questionnaireLosartan.isPresent());
        assertEquals("HYDROCHLOROTHIAZIDE__LOSARTAN", questionnaireLosartan.get().getName());

        List<ConceptMetadataModel> questionnaireLosartanMeta =
                this.conceptMetadataService.findByConceptID(questionnaireLosartan.get().getConceptNodeId());
        assertFalse(questionnaireLosartanMeta.isEmpty());
        assertEquals("values", questionnaireLosartanMeta.getFirst().getKey());
        assertEquals(List.of("0", "1"), this.columnMetaUtility.parseValues(questionnaireLosartanMeta.getFirst().getValue()));
    }

    @Test
    void shouldProcessColumMetas_Continuous() throws IOException {
        List<ColumnMeta> columnMetas = new ArrayList<>();
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta(
                this.columnMetaMapper.getParser().parseLine("\\laboratory\\biochemistry\\Creatinine, urine " +
                                                                             "(umol per L)\\,8,0,false,,0.0,68422.0," +
                                                                             "28621157,29175570,14570,9068")).get());
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta(
                this.columnMetaMapper.getParser().parseLine("\\laboratory\\biochemistry\\Creatinine, urine (umol per L)\\10078\\,5,0,true,10078,null,null,29175570,29176735,6,6")).get());
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta(
                this.columnMetaMapper.getParser().parseLine("\\laboratory\\biochemistry\\Creatinine, urine (umol per L)\\10166\\,5,0,true,10166,null,null,29176735,29178026,9,9")).get());
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta(
                this.columnMetaMapper.getParser().parseLine("\\laboratory\\biochemistry\\Creatinine, urine " +
                                                                             "(umol per L)\\10254\\,5,0,true,10254,null," +
                                                                             "null,29178026,29179443,12,12")).get());

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
    void shouldProcessColumMetas_Continuous_IncreasedMax() throws IOException {
        List<ColumnMeta> columnMetas = new ArrayList<>();
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta(
                this.columnMetaMapper.getParser().parseLine("\\laboratory\\biochemistry\\Creatinine, urine " +
                                                                             "(umol per L)\\,8,0,false,,0.0,68422.0," +
                                                                             "28621157,29175570,14570,9068")).get());
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta(
                this.columnMetaMapper.getParser().parseLine("\\laboratory\\biochemistry\\Creatinine, urine (umol per L)\\10078\\,5,0,true,10078,null,null,29175570,29176735,6,6")).get());
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta(
                this.columnMetaMapper.getParser().parseLine("\\laboratory\\biochemistry\\Creatinine, urine " +
                                                                             "(umol per L)\\10166.0\\,5,0,true,10166.0,null," +
                                                                             "null,29176735,29178026,9,9")).get());
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta(
                this.columnMetaMapper.getParser().parseLine("\\laboratory\\biochemistry\\Creatinine, urine " +
                                                                             "(umol per L)\\76543\\,5,0,true,76543,null," +
                                                                             "null,29178026,29179443,12,12")).get());

        this.dictionaryLoaderService.processColumnMetas(columnMetas);
        Optional<ConceptModel> byConcept = this.conceptService.findByConcept("\\laboratory\\biochemistry\\Creatinine, urine (umol per L)\\");
        assertTrue(byConcept.isPresent());

        List<ConceptMetadataModel> conceptMetadata = this.conceptMetadataService.findByConceptID(byConcept.get().getConceptNodeId());
        assertFalse(conceptMetadata.isEmpty());
        ConceptMetadataModel metadata = conceptMetadata.getFirst();
        Float max = this.columnMetaUtility.parseMax(metadata.getValue());
        Float min = this.columnMetaUtility.parseMin(metadata.getValue());

        assertEquals(0.0f, min);
        assertEquals(76543.0f, max);
    }

    @Test
    void shouldProcessColumMetas_Continuous_DecreasedMin() throws IOException {
        List<ColumnMeta> columnMetas = new ArrayList<>();
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta(
                this.columnMetaMapper.getParser().parseLine("\\laboratory\\biochemistry\\Creatinine, urine " +
                                                                             "(umol per L)\\,8,0,false,,0.0,68422.0," +
                                                                             "28621157,29175570,14570,9068")).get());
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta(
                this.columnMetaMapper.getParser().parseLine("\\laboratory\\biochemistry\\Creatinine, urine (umol per L)\\10078\\,5,0,true,10078,null,null,29175570,29176735,6,6")).get());
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta(
                this.columnMetaMapper.getParser().parseLine("\\laboratory\\biochemistry\\Creatinine, urine " +
                                                                             "(umol per L)\\10.0\\,5,0,true,-10.0,null," +
                                                                             "null,29176735,29178026,9,9")).get());
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta(
                this.columnMetaMapper.getParser().parseLine("\\laboratory\\biochemistry\\Creatinine, urine " +
                                                                             "(umol per L)\\76543\\,5,0,true,76543,null," +
                                                                             "null,29178026,29179443,12,12")).get());

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
        assertDoesNotThrow(() -> this.dictionaryLoaderService.processColumnMetaCSV(thousandGenomesFilePath, resourcePath +
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

    @Test
    void processColumnMetaCSV_withStudyFilter_onlyIncludedProcessed() throws IOException {
        // Build a tiny CSV with two different study roots
        String line1 = "\\phs001234\\demo\\AGE\\,8,0,false,,0.0,100.0,0,10,1,1";
        String line2 = "\\phs009999\\demo\\AGE\\,8,0,false,,0.0,100.0,10,20,1,1";
        Path tmpCsv = Files.createTempFile("cmfilter", ".csv");
        Files.write(tmpCsv, List.of(line1, line2), StandardCharsets.UTF_8);
        Path tmpErr = Files.createTempFile("cmerr", ".csv");

        // Allow only the first study (full ref)
        String result = this.dictionaryLoaderService.processColumnMetaCSV(tmpCsv.toString(), tmpErr.toString(), List.of("phs001234"));
        assertEquals("Success", result);

        // Only one dataset should be created and it should be the allowed one
        List<DatasetModel> datasets = this.datasetService.findAll();
        assertEquals(1, datasets.size());
        assertEquals("phs001234", datasets.getFirst().getRef());

        // Concepts created should be for a single path (root + demo + AGE)
        List<ConceptModel> concepts = this.conceptService.findAll();
        assertEquals(3, concepts.size());
    }

    @Test
    void processColumnMetaCSV_withBaseStudyFilter_matchesVersioned() throws IOException {
        String line1 = "\\phs001234\\demo\\AGE\\,8,0,false,,0.0,100.0,0,10,1,1";
        String line2 = "\\phs009999\\demo\\AGE\\,8,0,false,,0.0,100.0,10,20,1,1";
        Path tmpCsv = Files.createTempFile("cmbase", ".csv");
        Files.write(tmpCsv, List.of(line1, line2), StandardCharsets.UTF_8);
        Path tmpErr = Files.createTempFile("cmerr2", ".csv");

        String result = this.dictionaryLoaderService.processColumnMetaCSV(tmpCsv.toString(), tmpErr.toString(), List.of("phs001234"));
        assertEquals("Success", result);

        List<DatasetModel> datasets = this.datasetService.findAll();
        assertEquals(1, datasets.size());
        assertEquals("phs001234", datasets.getFirst().getRef());

        List<ConceptModel> concepts = this.conceptService.findAll();
        assertEquals(3, concepts.size());
    }

    @Test
    void shouldParseLongValues() throws IOException {
        String csvRow = "\\1000Genomes\\open_access-1000Genomes\\BIOSAMPLE ID\\,12,0,true,NAµSAME122789µSAME122790µSAME122791µSAME122792µSAME122793µSAME122794µSAME122795µSAME122796µSAME122797µSAME122804µSAME122805µSAME122806µSAME122807µSAME122808µSAME122809µSAME122810µSAME122811µSAME122812µSAME122813µSAME122814µSAME122815µSAME122816µSAME122817µSAME122818µSAME122819µSAME122820µSAME122821µSAME122822µSAME122823µSAME122824µSAME122825µSAME122826µSAME122827µSAME122828µSAME122829µSAME122831µSAME122832µSAME122833µSAME122834µSAME122835µSAME122836µSAME122838µSAME122839µSAME122841µSAME122842µSAME122844µSAME122845µSAME122846µSAME122847µSAME122848µSAME122849µSAME122851µSAME122852µSAME122853µSAME122854µSAME122855µSAME122856µSAME122857µSAME122859µSAME122860µSAME122861µSAME122862µSAME122863µSAME122864µSAME122865µSAME122866µSAME122867µSAME122868µSAME122869µSAME122870µSAME122871µSAME122872µSAME122873µSAME122874µSAME122875µSAME122876µSAME122879µSAME122881µSAME122882µSAME122883µSAME122884µSAME122885µSAME122886µSAME122887µSAME122893µSAME122894µSAME122901µSAME122902µSAME122903µSAME122912µSAME122914µSAME122915µSAME122916µSAME122917µSAME122918µSAME122919µSAME122920µSAME122921µSAME122922µSAME122923µSAME122924µSAME122925µSAME122926µSAME122927µSAME122928µSAME122929µSAME122930µSAME122931µSAME122932µSAME122933µSAME122934µSAME122935µSAME122936µSAME122937µSAME122938µSAME122939µSAME122940µSAME122941µSAME122942µSAME122943µSAME122944µSAME122945µSAME122946µSAME122947µSAME122948µSAME122949µSAME122951µSAME122953µSAME122954µSAME122955µSAME122956µSAME122958µSAME122959µSAME122960µSAME122961µSAME122969µSAME122970µSAME122971µSAME122972µSAME122974µSAME122975µSAME122976µSAME122977µSAME122978µSAME122979µSAME122980µSAME122981µSAME122982µSAME122983µSAME122984µSAME122985µSAME122986µSAME122987µSAME122988µSAME122989µSAME122990µSAME122991µSAME122992µSAME122993µSAME122994µSAME122996µSAME122997µSAME122998µSAME122999µSAME123000µSAME123001µSAME123002µSAME123003µSAME123004µSAME123005µSAME123006µSAME123007µSAME123008µSAME123009µSAME123010µSAME123011µSAME123012µSAME123013µSAME123014µSAME123015µSAME123016µSAME123017µSAME123019µSAME123020µSAME123021µSAME123022µSAME123023µSAME123024µSAME123025µSAME123026µSAME123027µSAME123028µSAME123029µSAME123030µSAME123031µSAME123032µSAME123033µSAME123034µSAME123035µSAME123036µSAME123037µSAME123040µSAME123041µSAME123042µSAME123043µSAME123044µSAME123045µSAME123046µSAME123047µSAME123048µSAME123049µSAME123050µSAME123051µSAME123052µSAME123053µSAME123054µSAME123056µSAME123057µSAME123058µSAME123059µSAME123060µSAME123061µSAME123062µSAME123063µSAME123064µSAME123065µSAME123066µSAME123067µSAME123068µSAME123069µSAME123070µSAME123071µSAME123073µSAME123075µSAME123076µSAME123077µSAME123078µSAME123079µSAME123080µSAME123081µSAME123082µSAME123083µSAME123084µSAME123085µSAME123086µSAME123087µSAME123095µSAME123098µSAME123099µSAME123100µSAME123101µSAME123102µSAME123103µSAME123104µSAME123105µSAME123106µSAME123107µSAME123108µSAME123109µSAME123110µSAME123111µSAME123112µSAME123113µSAME123114µSAME123115µSAME123116µSAME123117µSAME123118µSAME123119µSAME123120µSAME123121µSAME123122µSAME123123µSAME123125µSAME123128µSAME123129µSAME123130µSAME123131µSAME123132µSAME123135µSAME123138µSAME123139µSAME123140µSAME123141µSAME123142µSAME123143µSAME123144µSAME123145µSAME123146µSAME123147µSAME123148µSAME123149µSAME123150µSAME123151µSAME123152µSAME123153µSAME123154µSAME123155µSAME123156µSAME123158µSAME123159µSAME123160µSAME123161µSAME123163µSAME123165µSAME123166µSAME123167µSAME123168µSAME123169µSAME123170µSAME123171µSAME123172µSAME123173µSAME123174µSAME123175µSAME123178µSAME123179µSAME123180µSAME123181µSAME123190µSAME123191µSAME123192µSAME123193µSAME123194µSAME123195µSAME123196µSAME123197µSAME123198µSAME123199µSAME123200µSAME123201µSAME123202µSAME123203µSAME123205µSAME123206µSAME123207µSAME123208µSAME123209µSAME123210µSAME123211µSAME123212µSAME123213µSAME123214µSAME123215µSAME123216µSAME123217µSAME123218µSAME123219µSAME123220µSAME123221µSAME123222µSAME123223µSAME123224µSAME123225µSAME123226µSAME123227µSAME123228µSAME123229µSAME123231µSAME123232µSAME123233µSAME123234µSAME123235µSAME123236µSAME123237µSAME123238µSAME123239µSAME123240µSAME123241µSAME123242µSAME123243µSAME123244µSAME123245µSAME123246µSAME123247µSAME123248µSAME123249µSAME123250µSAME123251µSAME123252µSAME123253µSAME123254µSAME123255µSAME123256µSAME123257µSAME123259µSAME123260µSAME123261µSAME123262µSAME123263µSAME123264µSAME123265µSAME123266µSAME123267µSAME123268µSAME123271µSAME123272µSAME123273µSAME123274µSAME123275µSAME123276µSAME123284µSAME123285µSAME123286µSAME123287µSAME123288µSAME123289µSAME123290µSAME123291µSAME123292µSAME123293µSAME123294µSAME123295µSAME123296µSAME123297µSAME123298µSAME123299µSAME123300µSAME123305µSAME123307µSAME123308µSAME123309µSAME123310µSAME123311µSAME123312µSAME123315µSAME123316µSAME123317µSAME123318µSAME123319µSAME123320µSAME123321µSAME123322µSAME123323µSAME123324µSAME123325µSAME123326µSAME123327µSAME123328µSAME123329µSAME123330µSAME123331µSAME123332µSAME123334µSAME123335µSAME123338µSAME123339µSAME123340µSAME123341µSAME123342µSAME123343µSAME123344µSAME123359µSAME123360µSAME123361µSAME123362µSAME123363µSAME123364µSAME123365µSAME123366µSAME123367µSAME123368µSAME123369µSAME123370µSAME123371µSAME123372µSAME123373µSAME123374µSAME123375µSAME123386µSAME123387µSAME123388µSAME123389µSAME123390µSAME123391µSAME123392µSAME123393µSAME123394µSAME123395µSAME123396µSAME123397µSAME123398µSAME123399µSAME123400µSAME123401µSAME123402µSAME123403µSAME123404µSAME123405µSAME123406µSAME123407µSAME123408µSAME123412µSAME123413µSAME123414µSAME123415µSAME123416µSAME123417µSAME123418µSAME123419µSAME123420µSAME123421µSAME123422µSAME123423µSAME123424µSAME123425µSAME123426µSAME123427µSAME123428µSAME123429µSAME123430µSAME123431µSAME123432µSAME123433µSAME123434µSAME123435µSAME123436µSAME123438µSAME123442µSAME123443µSAME123444µSAME123445µSAME123446µSAME123447µSAME123448µSAME123449µSAME123450µSAME123451µSAME123452µSAME123453µSAME123454µSAME123455µSAME123456µSAME123457µSAME123458µSAME123459µSAME123460µSAME123461µSAME123462µSAME123463µSAME123471µSAME123472µSAME123473µSAME123474µSAME123475µSAME123476µSAME123477µSAME123478µSAME123479µSAME123480µSAME123481µSAME123482µSAME123483µSAME123484µSAME123485µSAME123486µSAME123487µSAME123490µSAME123494µSAME123495µSAME123496µSAME123497µSAME123498µSAME123500µSAME123501µSAME123502µSAME123504µSAME123515µSAME123516µSAME123518µSAME123519µSAME123520µSAME123521µSAME123522µSAME123523µSAME123524µSAME123525µSAME123526µSAME123527µSAME123528µSAME123529µSAME123530µSAME123531µSAME123533µSAME123535µSAME123536µSAME123537µSAME123539µSAME123540µSAME123541µSAME123542µSAME123543µSAME123544µSAME123545µSAME123547µSAME123548µSAME123549µSAME123550µSAME123551µSAME123552µSAME123553µSAME123554µSAME123555µSAME123556µSAME123557µSAME123558µSAME123560µSAME123561µSAME123562µSAME123563µSAME123565µSAME123566µSAME123567µSAME123568µSAME123569µSAME123570µSAME123571µSAME123572µSAME123573µSAME123574µSAME123575µSAME123576µSAME123577µSAME123578µSAME123579µSAME123580µSAME123581µSAME123582µSAME123583µSAME123584µSAME123585µSAME123586µSAME123587µSAME123588µSAME123589µSAME123590µSAME123591µSAME123592µSAME123593µSAME123594µSAME123595µSAME123596µSAME123597µSAME123598µSAME123599µSAME123600µSAME123601µSAME123602µSAME123603µSAME123604µSAME123605µSAME123606µSAME123607µSAME123609µSAME123610µSAME123611µSAME123613µSAME123614µSAME123615µSAME123616µSAME123617µSAME123618µSAME123619µSAME123620µSAME123621µSAME123622µSAME123623µSAME123624µSAME123625µSAME123626µSAME123627µSAME123628µSAME123629µSAME123630µSAME123631µSAME123632µSAME123633µSAME123634µSAME123636µSAME123637µSAME123638µSAME123639µSAME123640µSAME123641µSAME123642µSAME123643µSAME123644µSAME123645µSAME123646µSAME123647µSAME123648µSAME123649µSAME123650µSAME123651µSAME123652µSAME123653µSAME123654µSAME123655µSAME123658µSAME123659µSAME123660µSAME123661µSAME123662µSAME123663µSAME123664µSAME123665µSAME123666µSAME123667µSAME123668µSAME123669µSAME123670µSAME123671µSAME123672µSAME123673µSAME123674µSAME123675µSAME123676µSAME123677µSAME123678µSAME123679µSAME123681µSAME123682µSAME123683µSAME123684µSAME123686µSAME123688µSAME123691µSAME123693µSAME123694µSAME123695µSAME123696µSAME123697µSAME123698µSAME123699µSAME123700µSAME123701µSAME123706µSAME123707µSAME123710µSAME123711µSAME123712µSAME123713µSAME123715µSAME123716µSAME123717µSAME123718µSAME123719µSAME123720µSAME123721µSAME123722µSAME123723µSAME123724µSAME123727µSAME123728µSAME123734µSAME123735µSAME123737µSAME123738µSAME123739µSAME123740µSAME123741µSAME123742µSAME123748µSAME123749µSAME123750µSAME123751µSAME123752µSAME123753µSAME123754µSAME123755µSAME123756µSAME123757µSAME123758µSAME123759µSAME123760µSAME123761µSAME123762µSAME123763µSAME123764µSAME123765µSAME123766µSAME123767µSAME123768µSAME123769µSAME123770µSAME123771µSAME123772µSAME123773µSAME123774µSAME123775µSAME123776µSAME123777µSAME123782µSAME123783µSAME123784µSAME123785µSAME123786µSAME123787µSAME123788µSAME123789µSAME123790µSAME123791µSAME123792µSAME123793µSAME123794µSAME123795µSAME123796µSAME123797µSAME123798µSAME123799µSAME123800µSAME123801µSAME123802µSAME123803µSAME123804µSAME123805µSAME123806µSAME123807µSAME123808µSAME123809µSAME123810µSAME123811µSAME123812µSAME123813µSAME123814µSAME123815µSAME123817µSAME123818µSAME123819µSAME123820µSAME123821µSAME123822µSAME123829µSAME123830µSAME123831µSAME123834µSAME123835µSAME123836µSAME123837µSAME123838µSAME123839µSAME123840µSAME123841µSAME123842µSAME123843µSAME123844µSAME123845µSAME123846µSAME123847µSAME123848µSAME123849µSAME123850µSAME123851µSAME123852µSAME123853µSAME123854µSAME123855µSAME123856µSAME123857µSAME123858µSAME123859µSAME123861µSAME123869µSAME123872µSAME123873µSAME123874µSAME123875µSAME123876µSAME123877µSAME123878µSAME123879µSAME123880µSAME123881µSAME123882µSAME123883µSAME123884µSAME123885µSAME123886µSAME123887µSAME123888µSAME123889µSAME123890µSAME123891µSAME123892µSAME123893µSAME123894µSAME123895µSAME123896µSAME123897µSAME123898µSAME123899µSAME123900µSAME123902µSAME123903µSAME123904µSAME123905µSAME123909µSAME123910µSAME123911µSAME123912µSAME123914µSAME123915µSAME123916µSAME123917µSAME123921µSAME123922µSAME123923µSAME123924µSAME123925µSAME123926µSAME123927µSAME123928µSAME123943µSAME123944µSAME123945µSAME123946µSAME123947µSAME123948µSAME123949µSAME123950µSAME123951µSAME123952µSAME123953µSAME123954µSAME123955µSAME123956µSAME123957µSAME123958µSAME123959µSAME123960µSAME123961µSAME123962µSAME123963µSAME123964µSAME123965µSAME123966µSAME123967µSAME123968µSAME123969µSAME123971µSAME123972µSAME123973µSAME123974µSAME123975µSAME123976µSAME123977µSAME123978µSAME123979µSAME123980µSAME123981µSAME123982µSAME123983µSAME123984µSAME123985µSAME123986µSAME123987µSAME123988µSAME123989µSAME123990µSAME123991µSAME123993µSAME123994µSAME123995µSAME123996µSAME123997µSAME123998µSAME123999µSAME124000µSAME124002µSAME124003µSAME124007µSAME124008µSAME124009µSAME124011µSAME124012µSAME124013µSAME124014µSAME124015µSAME124016µSAME124017µSAME124018µSAME124019µSAME124020µSAME124021µSAME124022µSAME124023µSAME124024µSAME124025µSAME124026µSAME124027µSAME124028µSAME124030µSAME124031µSAME124032µSAME124033µSAME124034µSAME124035µSAME124036µSAME124037µSAME124038µSAME124041µSAME124042µSAME124043µSAME124044µSAME124046µSAME124049µSAME124050µSAME124052µSAME124053µSAME124054µSAME124055µSAME124056µSAME124057µSAME124058µSAME124059µSAME124060µSAME124061µSAME124062µSAME124063µSAME124064µSAME124065µSAME124066µSAME124067µSAME124068µSAME124069µSAME124070µSAME124071µSAME124073µSAME124075µSAME124076µSAME124077µSAME124078µSAME124080µSAME124086µSAME124087µSAME124088µSAME124089µSAME124090µSAME124091µSAME124092µSAME124093µSAME124094µSAME124095µSAME124097µSAME124098µSAME124099µSAME124100µSAME124101µSAME124102µSAME124103µSAME124104µSAME124106µSAME124107µSAME124108µSAME124109µSAME124110µSAME124111µSAME124112µSAME124113µSAME124114µSAME124115µSAME124116µSAME124117µSAME124118µSAME124122µSAME124123µSAME124124µSAME124126µSAME124127µSAME124128µSAME124130µSAME124131µSAME124133µSAME124134µSAME124135µSAME124136µSAME124137µSAME124138µSAME124139µSAME124140µSAME124142µSAME124143µSAME124144µSAME124145µSAME124146µSAME124147µSAME124148µSAME124149µSAME124150µSAME124151µSAME124152µSAME124153µSAME124154µSAME124155µSAME124156µSAME124157µSAME124158µSAME124159µSAME124160µSAME124161µSAME124162µSAME124163µSAME124164µSAME124165µSAME124166µSAME124167µSAME124168µSAME124169µSAME124170µSAME124171µSAME124172µSAME124173µSAME124174µSAME124175µSAME124176µSAME124177µSAME124178µSAME124179µSAME124180µSAME124181µSAME124182µSAME124183µSAME124184µSAME124185µSAME124186µSAME124187µSAME124188µSAME124190µSAME124193µSAME124194µSAME124195µSAME124196µSAME124197µSAME124198µSAME124199µSAME124200µSAME124201µSAME124207µSAME124208µSAME124209µSAME124211µSAME124216µSAME124217µSAME124218µSAME124219µSAME124220µSAME124221µSAME124222µSAME124227µSAME124228µSAME124229µSAME124230µSAME124231µSAME124232µSAME124233µSAME124234µSAME124235µSAME124236µSAME124237µSAME124247µSAME124248µSAME124249µSAME124252µSAME124253µSAME124254µSAME124256µSAME124257µSAME124258µSAME124259µSAME124260µSAME124261µSAME124262µSAME124263µSAME124264µSAME124265µSAME124266µSAME124267µSAME124268µSAME124269µSAME124270µSAME124271µSAME124272µSAME124273µSAME124274µSAME124275µSAME124276µSAME124277µSAME124278µSAME124279µSAME124280µSAME124283µSAME124287µSAME124288µSAME124289µSAME124290µSAME124291µSAME124292µSAME124293µSAME124294µSAME124295µSAME124296µSAME124297µSAME124298µSAME124299µSAME124300µSAME124301µSAME124302µSAME124303µSAME124304µSAME124305µSAME124306µSAME124307µSAME124308µSAME124309µSAME124310µSAME124311µSAME124312µSAME124313µSAME124314µSAME124315µSAME124316µSAME124317µSAME124318µSAME124319µSAME124320µSAME124321µSAME124322µSAME124323µSAME124324µSAME124325µSAME124326µSAME124327µSAME124328µSAME124329µSAME124330µSAME124331µSAME124332µSAME124333µSAME124334µSAME124335µSAME124336µSAME124337µSAME124338µSAME124339µSAME124340µSAME124341µSAME124342µSAME124343µSAME124344µSAME124345µSAME124346µSAME124347µSAME124348µSAME124349µSAME124350µSAME124351µSAME124352µSAME124353µSAME124354µSAME124355µSAME124356µSAME124357µSAME124358µSAME124359µSAME124360µSAME124361µSAME124362µSAME124363µSAME124364µSAME124365µSAME124366µSAME124367µSAME124368µSAME124369µSAME124370µSAME124371µSAME124372µSAME124373µSAME124374µSAME124375µSAME124376µSAME124377µSAME124378µSAME124379µSAME124380µSAME124381µSAME124382µSAME124383µSAME124384µSAME124386µSAME124387µSAME124388µSAME124389µSAME124390µSAME124391µSAME124392µSAME124393µSAME124395µSAME124396µSAME124397µSAME124398µSAME124399µSAME124400µSAME124401µSAME124402µSAME124403µSAME124404µSAME124406µSAME124407µSAME124414µSAME124415µSAME124416µSAME124417µSAME124419µSAME124420µSAME124421µSAME124422µSAME124423µSAME124424µSAME124425µSAME124426µSAME124427µSAME124428µSAME124431µSAME124432µSAME124433µSAME124434µSAME124435µSAME124436µSAME124437µSAME124438µSAME124439µSAME124440µSAME124441µSAME124442µSAME124443µSAME124444µSAME124445µSAME124446µSAME124447µSAME124448µSAME124449µSAME124450µSAME124451µSAME124452µSAME124453µSAME124454µSAME124455µSAME124456µSAME124458µSAME124465µSAME124466µSAME124469µSAME124470µSAME124471µSAME124472µSAME124473µSAME124474µSAME124475µSAME124476µSAME124477µSAME124478µSAME124479µSAME124480µSAME124481µSAME124482µSAME124483µSAME124484µSAME124485µSAME124486µSAME124487µSAME124488µSAME124489µSAME124490µSAME124491µSAME124492µSAME124493µSAME124494µSAME124496µSAME124497µSAME124498µSAME124501µSAME124502µSAME124504µSAME124506µSAME124507µSAME124508µSAME124509µSAME124510µSAME124511µSAME124512µSAME124514µSAME124517µSAME124518µSAME124519µSAME124521µSAME124522µSAME124524µSAME124525µSAME124530µSAME124531µSAME124532µSAME124533µSAME124534µSAME124535µSAME124537µSAME124538µSAME124539µSAME124540µSAME124541µSAME124542µSAME124543µSAME124544µSAME124545µSAME124546µSAME124547µSAME124548µSAME124549µSAME124550µSAME124551µSAME124552µSAME124553µSAME124554µSAME124555µSAME124556µSAME124557µSAME124558µSAME124559µSAME124560µSAME124561µSAME124562µSAME124563µSAME124564µSAME124565µSAME124566µSAME124569µSAME124570µSAME124571µSAME124572µSAME124573µSAME124574µSAME124575µSAME124576µSAME124577µSAME124578µSAME124579µSAME124580µSAME124581µSAME124582µSAME124583µSAME124584µSAME124585µSAME124586µSAME124587µSAME124588µSAME124589µSAME124590µSAME124591µSAME124592µSAME124593µSAME124594µSAME124595µSAME124596µSAME124597µSAME124598µSAME124599µSAME124600µSAME124601µSAME124605µSAME124606µSAME124607µSAME124608µSAME124609µSAME124610µSAME124611µSAME124612µSAME124613µSAME124614µSAME124616µSAME124617µSAME124618µSAME124619µSAME124620µSAME124621µSAME124622µSAME124623µSAME124624µSAME124625µSAME124629µSAME124630µSAME124631µSAME124632µSAME124633µSAME124634µSAME124635µSAME124636µSAME124637µSAME124638µSAME124639µSAME124640µSAME124641µSAME124642µSAME124646µSAME124647µSAME124650µSAME124651µSAME124653µSAME124654µSAME124655µSAME124656µSAME124657µSAME124658µSAME124659µSAME124660µSAME124661µSAME124662µSAME124663µSAME124664µSAME124665µSAME124666µSAME124668µSAME124669µSAME124670µSAME124671µSAME124672µSAME124673µSAME124674µSAME124675µSAME124676µSAME124677µSAME124678µSAME124679µSAME124680µSAME124681µSAME124682µSAME124683µSAME124684µSAME124685µSAME124686µSAME124687µSAME124688µSAME124689µSAME124690µSAME124692µSAME124693µSAME124694µSAME124695µSAME124696µSAME124697µSAME124700µSAME124701µSAME124702µSAME124703µSAME124704µSAME124705µSAME124706µSAME124707µSAME124710µSAME124711µSAME124712µSAME124713µSAME124714µSAME124715µSAME124716µSAME124718µSAME124719µSAME124722µSAME124726µSAME124727µSAME124729µSAME124730µSAME124731µSAME124732µSAME124733µSAME124734µSAME124735µSAME124736µSAME124737µSAME124739µSAME124740µSAME124741µSAME124742µSAME124743µSAME124744µSAME124745µSAME124746µSAME124747µSAME124748µSAME124749µSAME124750µSAME124751µSAME124752µSAME124753µSAME124754µSAME124755µSAME124756µSAME124757µSAME124758µSAME124759µSAME124760µSAME124761µSAME124762µSAME124763µSAME124764µSAME124765µSAME124766µSAME124767µSAME124768µSAME124769µSAME124770µSAME124771µSAME124772µSAME124773µSAME124774µSAME124775µSAME124776µSAME124777µSAME124778µSAME124779µSAME124780µSAME124783µSAME124784µSAME124785µSAME124786µSAME124787µSAME124788µSAME124789µSAME124790µSAME124791µSAME124792µSAME124793µSAME124794µSAME124795µSAME124796µSAME124797µSAME124798µSAME124804µSAME124810µSAME124811µSAME124812µSAME124818µSAME124820µSAME124821µSAME124822µSAME124824µSAME124825µSAME124826µSAME124827µSAME124828µSAME124829µSAME124830µSAME124831µSAME124832µSAME124833µSAME124834µSAME124835µSAME124838µSAME124839µSAME124844µSAME124845µSAME124848µSAME124849µSAME124850µSAME124851µSAME124852µSAME124854µSAME124855µSAME124856µSAME124857µSAME124859µSAME124860µSAME124861µSAME124862µSAME124863µSAME124864µSAME124865µSAME124866µSAME124867µSAME124868µSAME124869µSAME124870µSAME124872µSAME124873µSAME124874µSAME124875µSAME124876µSAME124877µSAME124878µSAME124879µSAME124881µSAME124882µSAME124883µSAME124884µSAME124885µSAME124886µSAME124887µSAME124888µSAME124889µSAME124890µSAME124891µSAME124892µSAME124893µSAME124894µSAME124895µSAME124896µSAME124897µSAME124898µSAME124901µSAME124902µSAME124903µSAME124904µSAME124905µSAME124906µSAME124907µSAME124908µSAME124909µSAME124910µSAME124911µSAME124912µSAME124913µSAME124914µSAME124915µSAME124917µSAME124919µSAME124920µSAME124921µSAME124922µSAME124923µSAME124924µSAME124925µSAME124926µSAME124927µSAME124929µSAME124930µSAME124931µSAME124932µSAME124933µSAME124934µSAME124935µSAME124936µSAME124937µSAME124939µSAME124940µSAME124941µSAME124942µSAME124943µSAME124944µSAME124945µSAME124946µSAME124947µSAME124948µSAME124950µSAME124951µSAME124952µSAME124953µSAME124955µSAME124956µSAME124957µSAME124958µSAME124959µSAME124960µSAME124961µSAME124962µSAME124963µSAME124964µSAME124965µSAME124966µSAME124971µSAME124972µSAME124973µSAME124975µSAME124976µSAME124978µSAME124979µSAME124980µSAME124981µSAME124983µSAME124984µSAME124985µSAME124986µSAME124987µSAME124988µSAME124989µSAME124990µSAME124991µSAME124999µSAME125000µSAME125001µSAME125002µSAME125005µSAME125006µSAME125007µSAME125008µSAME125009µSAME125010µSAME125011µSAME125012µSAME125013µSAME125014µSAME125015µSAME125016µSAME125017µSAME125020µSAME125021µSAME125022µSAME125023µSAME125024µSAME125025µSAME125026µSAME125027µSAME125028µSAME125029µSAME125030µSAME125031µSAME125032µSAME125033µSAME125034µSAME125035µSAME125036µSAME125037µSAME125038µSAME125039µSAME125040µSAME125042µSAME125043µSAME125045µSAME125046µSAME125047µSAME125048µSAME125049µSAME125050µSAME125051µSAME125052µSAME125053µSAME125055µSAME125056µSAME125057µSAME125058µSAME125059µSAME125060µSAME125061µSAME125062µSAME125063µSAME125064µSAME125065µSAME125066µSAME125067µSAME125068µSAME125069µSAME125070µSAME125071µSAME125073µSAME125074µSAME125075µSAME125076µSAME125077µSAME125078µSAME125079µSAME125080µSAME125081µSAME125091µSAME125092µSAME125093µSAME125094µSAME125095µSAME125096µSAME125097µSAME125098µSAME125099µSAME125100µSAME125101µSAME125102µSAME125103µSAME125104µSAME125105µSAME125106µSAME125107µSAME125108µSAME125109µSAME125110µSAME125111µSAME125112µSAME125113µSAME125115µSAME125116µSAME125117µSAME125118µSAME125119µSAME125120µSAME125121µSAME125122µSAME125123µSAME125124µSAME125125µSAME125126µSAME125127µSAME125128µSAME125129µSAME125130µSAME125131µSAME125132µSAME125133µSAME125134µSAME125135µSAME125136µSAME125137µSAME125138µSAME125139µSAME125140µSAME125141µSAME125142µSAME125143µSAME125144µSAME125145µSAME125146µSAME125147µSAME125148µSAME125149µSAME125150µSAME125151µSAME125152µSAME125153µSAME125154µSAME125155µSAME125156µSAME125157µSAME125158µSAME125159µSAME125160µSAME125161µSAME125162µSAME125163µSAME125167µSAME125168µSAME125171µSAME125180µSAME125181µSAME125182µSAME125183µSAME125184µSAME125186µSAME125187µSAME125188µSAME125189µSAME125190µSAME125191µSAME125192µSAME125193µSAME125194µSAME125195µSAME125196µSAME125197µSAME125198µSAME125199µSAME125205µSAME125217µSAME125218µSAME125219µSAME125221µSAME125223µSAME125224µSAME125226µSAME125227µSAME125228µSAME125229µSAME125230µSAME125231µSAME125235µSAME125236µSAME125237µSAME125238µSAME125239µSAME125240µSAME125241µSAME125242µSAME125243µSAME125244µSAME125246µSAME125247µSAME125248µSAME125249µSAME125250µSAME125252µSAME125253µSAME125255µSAME125258µSAME125259µSAME125260µSAME125261µSAME125262µSAME125263µSAME125264µSAME125265µSAME125266µSAME125267µSAME125268µSAME125269µSAME125271µSAME125272µSAME125273µSAME125274µSAME125275µSAME125276µSAME125278µSAME125279µSAME125280µSAME125281µSAME125282µSAME125283µSAME125284µSAME125285µSAME125286µSAME125287µSAME125288µSAME125289µSAME125290µSAME125304µSAME125305µSAME125307µSAME125308µSAME125309µSAME125310µSAME125311µSAME125313µSAME125315µSAME125316µSAME125317µSAME125318µSAME125319µSAME125320µSAME125321µSAME125322µSAME125323µSAME125324µSAME125325µSAME125326µSAME125327µSAME125328µSAME125329µSAME125330µSAME125331µSAME125332µSAME125333µSAME125334µSAME125335µSAME125336µSAME125338µSAME125339µSAME125340µSAME125341µSAME125342µSAME125343µSAME125344µSAME125345µSAME125346µSAME125347µSAME125348µSAME125357µSAME125358µSAME125359µSAME125360µSAME125361µSAME125362µSAME125363µSAME125364µSAME125365µSAME125366µSAME125367µSAME125368µSAME125369µSAME125370µSAME125371µSAME125372µSAME125373µSAME125378µSAME125380µSAME125381µSAME125382µSAME125383µSAME125384µSAME125385µSAME125386µSAME125389µSAME125390µSAME125391µSAME125392µSAME125393µSAME125394µSAME125395µSAME125396µSAME125397µSAME125398µSAME125399µSAME125401µSAME125409µSAME125410µSAME125411µSAME125412µSAME125413µSAME125414µSAME125415µSAME125416µSAME1839015µSAME1839016µSAME1839017µSAME1839018µSAME1839019µSAME1839020µSAME1839021µSAME1839022µSAME1839023µSAME1839024µSAME1839025µSAME1839026µSAME1839027µSAME1839028µSAME1839029µSAME1839030µSAME1839031µSAME1839032µSAME1839033µSAME1839034µSAME1839035µSAME1839036µSAME1839037µSAME1839038µSAME1839039µSAME1839040µSAME1839041µSAME1839042µSAME1839043µSAME1839044µSAME1839045µSAME1839046µSAME1839047µSAME1839048µSAME1839049µSAME1839050µSAME1839051µSAME1839052µSAME1839053µSAME1839054µSAME1839055µSAME1839056µSAME1839057µSAME1839058µSAME1839059µSAME1839060µSAME1839061µSAME1839062µSAME1839063µSAME1839064µSAME1839065µSAME1839066µSAME1839067µSAME1839068µSAME1839069µSAME1839070µSAME1839071µSAME1839072µSAME1839073µSAME1839074µSAME1839075µSAME1839076µSAME1839077µSAME1839078µSAME1839079µSAME1839080µSAME1839081µSAME1839082µSAME1839083µSAME1839084µSAME1839085µSAME1839086µSAME1839087µSAME1839088µSAME1839089µSAME1839090µSAME1839091µSAME1839092µSAME1839093µSAME1839094µSAME1839095µSAME1839096µSAME1839097µSAME1839098µSAME1839099µSAME1839100µSAME1839101µSAME1839102µSAME1839103µSAME1839104µSAME1839105µSAME1839106µSAME1839107µSAME1839108µSAME1839109µSAME1839110µSAME1839111µSAME1839112µSAME1839113µSAME1839114µSAME1839115µSAME1839116µSAME1839117µSAME1839118µSAME1839119µSAME1839120µSAME1839121µSAME1839122µSAME1839123µSAME1839124µSAME1839125µSAME1839126µSAME1839127µSAME1839128µSAME1839129µSAME1839130µSAME1839131µSAME1839132µSAME1839133µSAME1839134µSAME1839135µSAME1839136µSAME1839137µSAME1839138µSAME1839139µSAME1839140µSAME1839141µSAME1839142µSAME1839143µSAME1839144µSAME1839145µSAME1839146µSAME1839147µSAME1839148µSAME1839149µSAME1839150µSAME1839151µSAME1839152µSAME1839153µSAME1839154µSAME1839155µSAME1839156µSAME1839157µSAME1839158µSAME1839159µSAME1839160µSAME1839161µSAME1839162µSAME1839163µSAME1839164µSAME1839165µSAME1839166µSAME1839167µSAME1839168µSAME1839169µSAME1839170µSAME1839171µSAME1839172µSAME1839173µSAME1839174µSAME1839175µSAME1839176µSAME1839177µSAME1839178µSAME1839179µSAME1839180µSAME1839181µSAME1839182µSAME1839183µSAME1839184µSAME1839185µSAME1839186µSAME1839187µSAME1839188µSAME1839189µSAME1839190µSAME1839191µSAME1839192µSAME1839193µSAME1839194µSAME1839195µSAME1839196µSAME1839197µSAME1839198µSAME1839199µSAME1839200µSAME1839201µSAME1839202µSAME1839203µSAME1839204µSAME1839205µSAME1839206µSAME1839207µSAME1839208µSAME1839209µSAME1839210µSAME1839211µSAME1839212µSAME1839213µSAME1839214µSAME1839215µSAME1839216µSAME1839217µSAME1839218µSAME1839219µSAME1839220µSAME1839221µSAME1839222µSAME1839223µSAME1839224µSAME1839225µSAME1839226µSAME1839227µSAME1839228µSAME1839229µSAME1839230µSAME1839231µSAME1839232µSAME1839233µSAME1839234µSAME1839235µSAME1839236µSAME1839237µSAME1839238µSAME1839239µSAME1839240µSAME1839241µSAME1839242µSAME1839243µSAME1839244µSAME1839245µSAME1839246µSAME1839247µSAME1839248µSAME1839249µSAME1839250µSAME1839251µSAME1839252µSAME1839253µSAME1839254µSAME1839255µSAME1839256µSAME1839257µSAME1839258µSAME1839259µSAME1839260µSAME1839261µSAME1839262µSAME1839263µSAME1839264µSAME1839265µSAME1839266µSAME1839267µSAME1839268µSAME1839269µSAME1839270µSAME1839271µSAME1839272µSAME1839273µSAME1839274µSAME1839275µSAME1839276µSAME1839277µSAME1839278µSAME1839279µSAME1839280µSAME1839281µSAME1839282µSAME1839283µSAME1839284µSAME1839285µSAME1839286µSAME1839287µSAME1839288µSAME1839289µSAME1839290µSAME1839291µSAME1839292µSAME1839293µSAME1839294µSAME1839295µSAME1839296µSAME1839297µSAME1839298µSAME1839299µSAME1839300µSAME1839301µSAME1839302µSAME1839303µSAME1839304µSAME1839305µSAME1839306µSAME1839307µSAME1839308µSAME1839309µSAME1839310µSAME1839311µSAME1839312µSAME1839313µSAME1839314µSAME1839315µSAME1839316µSAME1839317µSAME1839318µSAME1839319µSAME1839320µSAME1839321µSAME1839322µSAME1839323µSAME1839324µSAME1839325µSAME1839326µSAME1839327µSAME1839328µSAME1839329µSAME1839330µSAME1839331µSAME1839332µSAME1839333µSAME1839334µSAME1839335µSAME1839336µSAME1839337µSAME1839338µSAME1839339µSAME1839340µSAME1839341µSAME1839342µSAME1839343µSAME1839344µSAME1839345µSAME1839346µSAME1839347µSAME1839348µSAME1839349µSAME1839350µSAME1839351µSAME1839352µSAME1839353µSAME1839354µSAME1839355µSAME1839356µSAME1839357µSAME1839358µSAME1839359µSAME1839360µSAME1839361µSAME1839362µSAME1839363µSAME1839364µSAME1839365µSAME1839366µSAME1839367µSAME1839368µSAME1839369µSAME1839370µSAME1839371µSAME1839372µSAME1839373µSAME1839374µSAME1839375µSAME1839376µSAME1839377µSAME1839378µSAME1839379µSAME1839380µSAME1839381µSAME1839382µSAME1839383µSAME1839384µSAME1839385µSAME1839386µSAME1839387µSAME1839388µSAME1839389µSAME1839390µSAME1839391µSAME1839392µSAME1839393µSAME1839394µSAME1839395µSAME1839396µSAME1839397µSAME1839398µSAME1839399µSAME1839400µSAME1839401µSAME1839402µSAME1839403µSAME1839404µSAME1839405µSAME1839406µSAME1839407µSAME1839408µSAME1839409µSAME1839410µSAME1839411µSAME1839412µSAME1839413µSAME1839414µSAME1839415µSAME1839416µSAME1839417µSAME1839418µSAME1839419µSAME1839420µSAME1839421µSAME1839422µSAME1839423µSAME1839424µSAME1839425µSAME1839426µSAME1839427µSAME1839428µSAME1839429µSAME1839430µSAME1839431µSAME1839432µSAME1839433µSAME1839434µSAME1839435µSAME1839436µSAME1839437µSAME1839438µSAME1839439µSAME1839440µSAME1839441µSAME1839442µSAME1839443µSAME1839444µSAME1839445µSAME1839446µSAME1839447µSAME1839448µSAME1839449µSAME1839450µSAME1839451µSAME1839452µSAME1839453µSAME1839454µSAME1839455µSAME1839456µSAME1839457µSAME1839458µSAME1839459µSAME1839460µSAME1839461µSAME1839462µSAME1839463µSAME1839464µSAME1839465µSAME1839466µSAME1839467µSAME1839468µSAME1839469µSAME1839470µSAME1839471µSAME1839472µSAME1839473µSAME1839474µSAME1839475µSAME1839476µSAME1839477µSAME1839478µSAME1839479µSAME1839480µSAME1839481µSAME1839482µSAME1839483µSAME1839484µSAME1839485µSAME1839486µSAME1839487µSAME1839488µSAME1839489µSAME1839490µSAME1839491µSAME1839492µSAME1839493µSAME1839494µSAME1839495µSAME1839496µSAME1839497µSAME1839498µSAME1839499µSAME1839500µSAME1839501µSAME1839502µSAME1839503µSAME1839504µSAME1839505µSAME1839506µSAME1839507µSAME1839508µSAME1839509µSAME1839510µSAME1839511µSAME1839512µSAME1839513µSAME1839514µSAME1839515µSAME1839516µSAME1839517µSAME1839518µSAME1839519µSAME1839520µSAME1839521µSAME1839522µSAME1839523µSAME1839524µSAME1839525µSAME1839526µSAME1839527µSAME1839528µSAME1839529µSAME1839530µSAME1839531µSAME1839532µSAME1839533µSAME1839534µSAME1839535µSAME1839536µSAME1839537µSAME1839538µSAME1839539µSAME1839540µSAME1839541µSAME1839542µSAME1839543µSAME1839544µSAME1839545µSAME1839546µSAME1839547µSAME1839548µSAME1839549µSAME1839550µSAME1839551µSAME1839552µSAME1839553µSAME1839554µSAME1839555µSAME1839556µSAME1839557µSAME1839558µSAME1839559µSAME1839560µSAME1839561µSAME1839562µSAME1839563µSAME1839564µSAME1839565µSAME1839566µSAME1839567µSAME1839568µSAME1839569µSAME1839570µSAME1839571µSAME1839572µSAME1839573µSAME1839574µSAME1839575µSAME1839576µSAME1839577µSAME1839578µSAME1839579µSAME1839580µSAME1839581µSAME1839582µSAME1839583µSAME1839584µSAME1839585µSAME1839586µSAME1839587µSAME1839588µSAME1839589µSAME1839590µSAME1839591µSAME1839592µSAME1839593µSAME1839594µSAME1839595µSAME1839596µSAME1839597µSAME1839598µSAME1839599µSAME1839600µSAME1839601µSAME1839602µSAME1839603µSAME1839604µSAME1839605µSAME1839606µSAME1839607µSAME1839608µSAME1839609µSAME1839610µSAME1839611µSAME1839612µSAME1839613µSAME1839614µSAME1839615µSAME1839616µSAME1839617µSAME1839618µSAME1839619µSAME1839620µSAME1839621µSAME1839622µSAME1839623µSAME1839624µSAME1839625µSAME1839626µSAME1839627µSAME1839628µSAME1839629µSAME1839630µSAME1839631µSAME1839632µSAME1839633µSAME1839634µSAME1839635µSAME1839636µSAME1839637µSAME1839638µSAME1839639µSAME1839640µSAME1839641µSAME1839642µSAME1839643µSAME1839644µSAME1839645µSAME1839646µSAME1839647µSAME1839648µSAME1839649µSAME1839650µSAME1839651µSAME1839652µSAME1839653µSAME1839654µSAME1839655µSAME1839656µSAME1839657µSAME1839658µSAME1839659µSAME1839660µSAME1839661µSAME1839662µSAME1839663µSAME1839664µSAME1839665µSAME1839666µSAME1839667µSAME1839668µSAME1839669µSAME1839670µSAME1839671µSAME1839672µSAME1839673µSAME1839674µSAME1839675µSAME1839676µSAME1839677µSAME1839678µSAME1839679µSAME1839680µSAME1839681µSAME1839682µSAME1839683µSAME1839684µSAME1839685µSAME1839686µSAME1839687µSAME1839688µSAME1839689µSAME1839690µSAME1839691µSAME1839692µSAME1839693µSAME1839694µSAME1839695µSAME1839696µSAME1839697µSAME1839698µSAME1839699µSAME1839700µSAME1839701µSAME1839702µSAME1839703µSAME1839704µSAME1839705µSAME1839706µSAME1839707µSAME1839708µSAME1839709µSAME1839710µSAME1839711µSAME1839712µSAME1839713µSAME1839714µSAME1839715µSAME1839716µSAME1839717µSAME1839718µSAME1839719µSAME1839720µSAME1839721µSAME1839722µSAME1839723µSAME1839724µSAME1839725µSAME1839726µSAME1839727µSAME1839728µSAME1839729µSAME1839730µSAME1839731µSAME1839732µSAME1839733µSAME1839734µSAME1839735µSAME1839736µSAME1839737µSAME1839738µSAME1839739µSAME1839740µSAME1839741µSAME1839742µSAME1839743µSAME1839744µSAME1839745µSAME1839746µSAME1839747µSAME1839748µSAME1839749µSAME1839750µSAME1839751µSAME1839752µSAME1839753µSAME1839754µSAME1839755µSAME1839756µSAME1839757µSAME1839758µSAME1839759µSAME1839760µSAME1839761µSAME1839762µSAME1839763µSAME1839764µSAME1839765µSAME1839766µSAME1839767µSAME1839768µSAME1839769µSAME1839770µSAME1839771µSAME1839772µSAME1839773µSAME1839774µSAME1839775µSAME1839776µSAME1839777µSAME1839778µSAME1839779µSAME1839780µSAME1839781µSAME1839782µSAME1839783µSAME1839784µSAME1839785µSAME1839786µSAME1839787µSAME1839788µSAME1839789µSAME1839790µSAME1839791µSAME1839792µSAME1839793µSAME1839794µSAME1839795µSAME1839796µSAME1839797µSAME1839798µSAME1839799µSAME1839800µSAME1839801µSAME1839802µSAME1839803µSAME1839804µSAME1839805µSAME1839806µSAME1839807µSAME1839808µSAME1839809µSAME1839810µSAME1839811µSAME1839812µSAME1839813µSAME1839814µSAME1839815µSAME1839816µSAME1839817µSAME1839818µSAME1839819µSAME1839820µSAME1839821µSAME1839822µSAME1839823µSAME1839824µSAME1839825µSAME1839826µSAME1839827µSAME1839828µSAME1839829µSAME1839830µSAME1839831µSAME1839832µSAME1839833µSAME1839834µSAME1839835µSAME1839836µSAME1839837µSAME1839838µSAME1839839µSAME1839840µSAME1839841µSAME1839842µSAME1839843µSAME1839844µSAME1839845µSAME1839846µSAME1839847µSAME1839848µSAME1839849µSAME1839850µSAME1839851µSAME1839852µSAME1839853µSAME1839854µSAME1839855µSAME1839856µSAME1839857µSAME1839858µSAME1839859µSAME1839860µSAME1839861µSAME1839862µSAME1839863µSAME1839864µSAME1839865µSAME1839866µSAME1839867µSAME1839868µSAME1839869µSAME1839870µSAME1839871µSAME1839872µSAME1839873µSAME1839874µSAME1839875µSAME1839876µSAME1839877µSAME1839878µSAME1839879µSAME1839880µSAME1839881µSAME1839882µSAME1839883µSAME1839884µSAME1839885µSAME1839886µSAME1839887µSAME1839888µSAME1839889µSAME1839890µSAME1839891µSAME1839892µSAME1839893µSAME1839894µSAME1839895µSAME1839896µSAME1839897µSAME1839898µSAME1839899µSAME1839900µSAME1839901µSAME1839902µSAME1839903µSAME1839904µSAME1839905µSAME1839906µSAME1839907µSAME1839908µSAME1839909µSAME1839910µSAME1839911µSAME1839912µSAME1839913µSAME1839914µSAME1839915µSAME1839916µSAME1839917µSAME1839918µSAME1839919µSAME1839920µSAME1839921µSAME1839922µSAME1839923µSAME1839924µSAME1839925µSAME1839926µSAME1839927µSAME1839928µSAME1839929µSAME1839930µSAME1839931µSAME1839932µSAME1839933µSAME1839934µSAME1839935µSAME1839936µSAME1839937µSAME1839938µSAME1839939µSAME1839940µSAME1839941µSAME1839942µSAME1839943µSAME1839944µSAME1839945µSAME1839946µSAME1839947µSAME1839948µSAME1839949µSAME1839950µSAME1839951µSAME1839952µSAME1839953µSAME1839954µSAME1839955µSAME1839956µSAME1839957µSAME1839958µSAME1839959µSAME1839960µSAME1839961µSAME1839962µSAME1839963µSAME1839964µSAME1839965µSAME1839966µSAME1839967µSAME1839968µSAME1839969µSAME1839970µSAME1839971µSAME1839972µSAME1839973µSAME1839974µSAME1839975µSAME1839976µSAME1839977µSAME1839978µSAME1839979µSAME1839980µSAME1839981µSAME1839982µSAME1839983µSAME1839984µSAME1839985µSAME1839986µSAME1839987µSAME1839988µSAME1839989µSAME1839990µSAME1839991µSAME1839992µSAME1839993µSAME1839994µSAME1839995µSAME1839996µSAME1839997µSAME1839998µSAME1839999µSAME1840000µSAME1840001µSAME1840002µSAME1840003µSAME1840004µSAME1840005µSAME1840006µSAME1840007µSAME1840008µSAME1840009µSAME1840010µSAME1840011µSAME1840012µSAME1840013µSAME1840014µSAME1840015µSAME1840016µSAME1840017µSAME1840018µSAME1840019µSAME1840020µSAME1840021µSAME1840022µSAME1840023µSAME1840024µSAME1840025µSAME1840026µSAME1840027µSAME1840028µSAME1840029µSAME1840030µSAME1840031µSAME1840032µSAME1840033µSAME1840034µSAME1840035µSAME1840036µSAME1840037µSAME1840038µSAME1840039µSAME1840040µSAME1840041µSAME1840042µSAME1840043µSAME1840044µSAME1840045µSAME1840046µSAME1840047µSAME1840048µSAME1840049µSAME1840050µSAME1840051µSAME1840052µSAME1840053µSAME1840054µSAME1840055µSAME1840056µSAME1840057µSAME1840058µSAME1840059µSAME1840060µSAME1840061µSAME1840062µSAME1840063µSAME1840064µSAME1840065µSAME1840066µSAME1840067µSAME1840068µSAME1840069µSAME1840070µSAME1840071µSAME1840072µSAME1840073µSAME1840074µSAME1840075µSAME1840076µSAME1840077µSAME1840078µSAME1840079µSAME1840080µSAME1840081µSAME1840082µSAME1840083µSAME1840084µSAME1840085µSAME1840086µSAME1840087µSAME1840088µSAME1840089µSAME1840090µSAME1840091µSAME1840092µSAME1840093µSAME1840094µSAME1840095µSAME1840096µSAME1840097µSAME1840098µSAME1840099µSAME1840100µSAME1840101µSAME1840102µSAME1840103µSAME1840104µSAME1840105µSAME1840106µSAME1840107µSAME1840108µSAME1840109µSAME1840110µSAME1840111µSAME1840112µSAME1840113µSAME1840114µSAME1840115µSAME1840116µSAME1840117µSAME1840118µSAME1840119µSAME1840120µSAME1840121µSAME1840122µSAME1840123µSAME1840124µSAME1840125µSAME1840126µSAME1840127µSAME1840128µSAME1840129µSAME1840130µSAME1840131µSAME1840132µSAME1840133µSAME1840134µSAME1840135µSAME1840136µSAME1840137µSAME1840138µSAME1840139µSAME1840140µSAME1840141µSAME1840142µSAME1840143µSAME1840144µSAME1840145µSAME1840146µSAME1840147µSAME1840148µSAME1840149µSAME1840150µSAME1840151µSAME1840152µSAME1840153µSAME1840154µSAME1840155µSAME1840156µSAME1840157µSAME1840158µSAME1840159µSAME1840160µSAME1840161µSAME1840162µSAME1840163µSAME1840164µSAME1840165µSAME1840166µSAME1840167µSAME1840168µSAME1840169µSAME1840170µSAME1840171µSAME1840172µSAME1840173µSAME1840174µSAME1840175µSAME1840176µSAME1840177µSAME1840178µSAME1840179µSAME1840180µSAME1840181µSAME1840182µSAME1840183µSAME1840184µSAME1840185µSAME1840186µSAME1840187µSAME1840188µSAME1840189µSAME1840190µSAME1840191µSAME1840192µSAME1840193µSAME1840194µSAME1840195µSAME1840196µSAME1840197µSAME1840198µSAME1840199µSAME1840200µSAME1840201µSAME1840202µSAME1840203µSAME1840204µSAME1840205µSAME1840206µSAME1840207µSAME1840208µSAME1840209µSAME1840210µSAME1840211µSAME1840212µSAME1840213µSAME1840214µSAME1840215µSAME1840216µSAME1840217µSAME1840218µSAME1840219µSAME1840220µSAME1840221µSAME1840222µSAME1840223µSAME1840224µSAME1840225µSAME1840226µSAME1840227µSAME1840228µSAME1840229µSAME1840230µSAME1840231µSAME1840232µSAME1840233µSAME1840234µSAME1840235µSAME1840236µSAME1840237µSAME1840238µSAME1840239µSAME1840240µSAME1840241µSAME1840242µSAME1840243µSAME1840244µSAME1840245µSAME1840246µSAME1840247µSAME1840248µSAME1840249µSAME1840250µSAME1840251µSAME1840252µSAME1840253µSAME1840254µSAME1840255µSAME1840256µSAME1840257µSAME1840258µSAME1840259µSAME1840260µSAME1840261µSAME1840262µSAME1840263µSAME1840264µSAME1840265µSAME1840266µSAME1840267µSAME1840268µSAME1840269µSAME1840270µSAME1840271µSAME1840272µSAME1840273µSAME1840274µSAME1840275µSAME1840276µSAME1840277µSAME1840278µSAME1840279µSAME1840280µSAME1840281µSAME1840282µSAME1840283µSAME1840284µSAME1840285µSAME1840286µSAME1840287µSAME1840288µSAME1840289µSAME1840290µSAME1840291µSAME1840292µSAME1840293µSAME1840294µSAME1840295µSAME1840296µSAME1840297µSAME1840298µSAME1840299µSAME1840300µSAME1840301µSAME1840302µSAME1840303µSAME1840304µSAME1840305µSAME1840306µSAME1840307µSAME1840308µSAME1840309µSAME1840310µSAME1840311µSAME1840312µSAME1840313µSAME1840314µSAME1840315µSAME1840316µSAME1840317µSAME1840318µSAME1840319µSAME1840320µSAME1840321µSAME1840322µSAME1840323µSAME1840324µSAME1840325µSAME1840326µSAME1840327µSAME1840328µSAME1840329µSAME1840330µSAME1840331µSAME1840332µSAME1840333µSAME1840334µSAME1840335µSAME1840336µSAME1840337µSAME1840338µSAME1840339µSAME1840340µSAME1840341µSAME1840342µSAME1840343µSAME1840344µSAME1840345µSAME1840346µSAME1840347µSAME1840348µSAME1840349µSAME1840350µSAME1840351µSAME1840352µSAME1840353µSAME1840354µSAME1840355µSAME1840356µSAME1840357µSAME1840358µSAME1840359µSAME1840360µSAME1840361µSAME1840362µSAME1840363µSAME1840364µSAME1840365µSAME1840366µSAME1840367µSAME1840368µSAME1840369µSAME1840370µSAME1840371µSAME1840372µSAME1840373µSAME1840374µSAME1840375µSAME1840376µSAME1840377µSAME1840378µSAME1840379µSAME1840380µSAME1840381µSAME1840382µSAME1840383µSAME1840384µSAME1840385µSAME1840386µSAME1840387µSAME1840388µSAME1840389µSAME1840390µSAME1840391µSAME1840392µSAME1840393µSAME1840394µSAME1840395µSAME1840396µSAME1840397µSAME1840398µSAME1840399µSAME1840400µSAME1840401µSAME1840402µSAME1840403µSAME1840404µSAME1840405µSAME1840406µSAME1840407µSAME1840408µSAME1840409µSAME1840410µSAME1840411µSAME1840412µSAME1840413µSAME1840414µSAME1840415µSAME1840416µSAMEA3302610µSAMEA3302611µSAMEA3302612µSAMEA3302614µSAMEA3302615µSAMEA3302616µSAMEA3302618µSAMEA3302619µSAMEA3302620µSAMEA3302621µSAMEA3302623µSAMEA3302624µSAMEA3302625µSAMEA3302627µSAMEA3302630µSAMEA3302632µSAMEA3302633µSAMEA3302634µSAMEA3302635µSAMEA3302636µSAMEA3302639µSAMEA3302641µSAMEA3302642µSAMEA3302645µSAMEA3302646µSAMEA3302648µSAMEA3302649µSAMEA3302652µSAMEA3302653µSAMEA3302654µSAMEA3302655µSAMEA3302656µSAMEA3302659µSAMEA3302660µSAMEA3302662µSAMEA3302663µSAMEA3302664µSAMEA3302665µSAMEA3302666µSAMEA3302667µSAMEA3302668µSAMEA3302669µSAMEA3302670µSAMEA3302672µSAMEA3302673µSAMEA3302674µSAMEA3302675µSAMEA3302677µSAMEA3302678µSAMEA3302679µSAMEA3302681µSAMEA3302682µSAMEA3302684µSAMEA3302685µSAMEA3302687µSAMEA3302688µSAMEA3302689µSAMEA3302691µSAMEA3302692µSAMEA3302693µSAMEA3302694µSAMEA3302695µSAMEA3302696µSAMEA3302697µSAMEA3302698µSAMEA3302699µSAMEA3302700µSAMEA3302703µSAMEA3302704µSAMEA3302705µSAMEA3302706µSAMEA3302708µSAMEA3302709µSAMEA3302710µSAMEA3302712µSAMEA3302713µSAMEA3302714µSAMEA3302715µSAMEA3302716µSAMEA3302717µSAMEA3302718µSAMEA3302719µSAMEA3302720µSAMEA3302722µSAMEA3302723µSAMEA3302724µSAMEA3302725µSAMEA3302727µSAMEA3302728µSAMEA3302729µSAMEA3302730µSAMEA3302731µSAMEA3302732µSAMEA3302733µSAMEA3302734µSAMEA3302735µSAMEA3302736µSAMEA3302737µSAMEA3302739µSAMEA3302740µSAMEA3302742µSAMEA3302743µSAMEA3302744µSAMEA3302745µSAMEA3302746µSAMEA3302747µSAMEA3302749µSAMEA3302750µSAMEA3302752µSAMEA3302753µSAMEA3302754µSAMEA3302755µSAMEA3302756µSAMEA3302757µSAMEA3302758µSAMEA3302759µSAMEA3302761µSAMEA3302762µSAMEA3302763µSAMEA3302764µSAMEA3302765µSAMEA3302766µSAMEA3302767µSAMEA3302768µSAMEA3302769µSAMEA3302770µSAMEA3302771µSAMEA3302773µSAMEA3302776µSAMEA3302777µSAMEA3302778µSAMEA3302779µSAMEA3302780µSAMEA3302781µSAMEA3302782µSAMEA3302784µSAMEA3302785µSAMEA3302786µSAMEA3302787µSAMEA3302790µSAMEA3302791µSAMEA3302792µSAMEA3302793µSAMEA3302795µSAMEA3302796µSAMEA3302797µSAMEA3302798µSAMEA3302800µSAMEA3302802µSAMEA3302804µSAMEA3302806µSAMEA3302807µSAMEA3302808µSAMEA3302809µSAMEA3302810µSAMEA3302811µSAMEA3302813µSAMEA3302814µSAMEA3302815µSAMEA3302816µSAMEA3302817µSAMEA3302818µSAMEA3302819µSAMEA3302820µSAMEA3302822µSAMEA3302823µSAMEA3302824µSAMEA3302825µSAMEA3302826µSAMEA3302827µSAMEA3302828µSAMEA3302829µSAMEA3302830µSAMEA3302831µSAMEA3302832µSAMEA3302833µSAMEA3302834µSAMEA3302835µSAMEA3302836µSAMEA3302837µSAMEA3302838µSAMEA3302839µSAMEA3302840µSAMEA3302841µSAMEA3302842µSAMEA3302843µSAMEA3302844µSAMEA3302845µSAMEA3302846µSAMEA3302848µSAMEA3302849µSAMEA3302850µSAMEA3302851µSAMEA3302853µSAMEA3302855µSAMEA3302856µSAMEA3302857µSAMEA3302858µSAMEA3302859µSAMEA3302861µSAMEA3302862µSAMEA3302863µSAMEA3302864µSAMEA3302865µSAMEA3302866µSAMEA3302867µSAMEA3302868µSAMEA3302869µSAMEA3302870µSAMEA3302874µSAMEA3302875µSAMEA3302876µSAMEA3302877µSAMEA3302878µSAMEA3302880µSAMEA3302882µSAMEA3302884µSAMEA3302887µSAMEA3302888µSAMEA3302889µSAMEA3302890µSAMEA3302891µSAMEA3302893µSAMEA3302894µSAMEA3302895µSAMEA3302896µSAMEA3302897µSAMEA3302900µSAMEA3302901µSAMEA3302902µSAMEA3302903µSAMEA3302904µSAMEA3302905µSAMEA3302906µSAMEA3302908µSAMEA3302909µSAMEA3302911µSAMEA3449876µSAMEA3449877µSAMEA3449879µSAMEA6604124,null,null,871826,1125048,4978,4978";
        Optional<ColumnMeta> columnMeta = columnMetaMapper.mapCSVRowToColumnMeta(columnMetaMapper.getParser().parseLine(csvRow));
        assertTrue(columnMeta.isPresent());
        assertEquals("\\1000Genomes\\open_access-1000Genomes\\BIOSAMPLE ID\\", columnMeta.get().name());

        this.dictionaryLoaderService.processColumnMetas(List.of(columnMeta.get()));

        // get the column meta from the dictionary
        List<ConceptModel> all = this.conceptService.findAll();
        assertEquals(3, all.size());

        // Verify the concept path \1000Genomes\open_access-1000Genomes\BIOSAMPLE ID\ has been added and has metadata
        ConceptModel conceptModel = all.stream()
                .filter(concept -> concept.getConceptPath().equals("\\1000Genomes\\open_access-1000Genomes\\BIOSAMPLE ID\\"))
                .findFirst()
                .orElse(null);

        assertNotNull(conceptModel);

        List<ConceptMetadataModel> byConceptID = this.conceptMetadataService.findByConceptID(conceptModel.getConceptNodeId());
        assertEquals(1, byConceptID.size());
        ConceptMetadataModel conceptMetadataModel = byConceptID.getFirst();
        List<String> strings = this.columnMetaUtility.parseValues(conceptMetadataModel.getValue());
        assertEquals(strings.size(), columnMeta.get().categoryValues().size());
    }

    @Test
    void shouldMarkNonLeafNodesAsIntermediate() throws IOException {
        List<ColumnMeta> columnMetas = new ArrayList<>();
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta(
                this.columnMetaMapper.getParser().parseLine("\\laboratory\\acrylamide\\Acrylamide (pmoL per G" +
                                                                             " Hb)\\100\\,3,0,true,100,null,null,12066159," +
                                                                             "12067144,2,2")).get());
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta(
                this.columnMetaMapper.getParser().parseLine("\\laboratory\\acrylamide\\Acrylamide (pmoL per G" +
                                                                             " Hb)\\101\\,3,0,true,101,null,null,12067144," +
                                                                             "12068169,3,3")).get());
        columnMetas.add(this.columnMetaMapper.mapCSVRowToColumnMeta(this.columnMetaMapper.getParser().parseLine("\\laboratory\\acrylamide\\Acrylamide (pmoL per G" +
                                                                             " Hb)\\103\\,3,0,true,103,null,null,12068169," +
                                                                             "12069274,5,5")).get());
        this.dictionaryLoaderService.processColumnMetas(columnMetas);

        // find all concepts
        Optional<ConceptModel> laboratory = this.conceptService.findByConcept("\\laboratory\\");
        assertTrue(laboratory.isPresent());
        assertEquals("laboratory", laboratory.get().getName());
        assertEquals(laboratory.get().getConceptType(), ConceptTypes.CATEGORICAL.getConceptType());

        Optional<ConceptModel> acrylamideGHB =
                this.conceptService.findByConcept("\\laboratory\\acrylamide\\Acrylamide (pmoL per G Hb)\\");
        assertTrue(acrylamideGHB.isPresent());
        assertEquals("Acrylamide (pmoL per G Hb)", acrylamideGHB.get().getName());
        assertEquals(acrylamideGHB.get().getConceptType(), ConceptTypes.CATEGORICAL.getConceptType());

        Optional<ConceptModel> acrylamide = this.conceptService.findByConcept("\\laboratory\\acrylamide\\");
        assertTrue(acrylamide.isPresent());
        assertEquals("acrylamide", acrylamide.get().getName());
        assertEquals(acrylamide.get().getConceptType(), ConceptTypes.CATEGORICAL.getConceptType());
    }

}