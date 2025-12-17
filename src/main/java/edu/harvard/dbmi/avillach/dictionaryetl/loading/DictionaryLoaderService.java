package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import edu.harvard.dbmi.avillach.dictionaryetl.Utility.ColumnMetaUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.*;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetService;
import edu.harvard.dbmi.avillach.dictionaryetl.loading.model.ConceptNode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Service
public class DictionaryLoaderService {

    private final Logger log = LoggerFactory.getLogger(DictionaryLoaderService.class);
    private final ColumnMetaMapper columnMetaMapper;
    private final DatasetService datasetService;
    private final ConceptService conceptService;
    private final ConceptMetadataService conceptMetadataService;
    private final ColumnMetaUtility columnMetaUtility;
    private final ConcurrentFullPathTree concurrentFullPathTree;
    private final CSVParser csvParser;

    private static final int BATCH_SIZE = 5000;
    private final BlockingQueue<ConceptMetadataModel> metadataBatchQueue = new LinkedBlockingQueue<>();

    @Autowired
    public DictionaryLoaderService(ColumnMetaMapper columnMetaMapper, DatasetService datasetService, ConceptService conceptService, ConceptMetadataService conceptMetadataService, DataSource dataSource, ColumnMetaUtility columnMetaUtility, ConcurrentFullPathTree concurrentFullPathTree, CSVParser csvParser) throws SQLException {
        this.columnMetaMapper = columnMetaMapper;
        this.datasetService = datasetService;
        this.conceptService = conceptService;
        this.conceptMetadataService = conceptMetadataService;
        this.columnMetaUtility = columnMetaUtility;
        this.concurrentFullPathTree = concurrentFullPathTree;
        this.csvParser = csvParser;
    }

    private final Set<String> columnMetaErrors = new HashSet<>();

    public String processColumnMetaCSV(String csvPath, String errorFile) throws RuntimeException {
        return processColumnMetaCSV(csvPath, errorFile, null);
    }

    public String processColumnMetaCSV(String csvPath, String errorFile, List<String> studies) throws RuntimeException {
        String baseDir = System.getProperty("hpds.data.dir", "/opt/local/hpds");

        if (errorFile == null) {
            errorFile = Path.of(baseDir, "columnMetaErrors.csv").toString();
        } else if (!errorFile.endsWith(".csv")) {
            return "The error file must be a csv.";
        }

        if (csvPath == null) {
            csvPath = Path.of(baseDir, "columnMeta.csv").toString();
        }

        final Set<String> allowedStudies = (studies == null || studies.isEmpty()) ? new HashSet<>() :
                studies.stream()
                        .filter(Objects::nonNull)
                        .map(String::trim)
                        .map(String::toLowerCase)
                        .collect(Collectors.toSet());

        try (ExecutorService columnMetaScopeExecutor = Executors.newVirtualThreadPerTaskExecutor()) {
            try (BufferedReader br = new BufferedReader(new FileReader(csvPath));
                 CSVReader csvReader = new CSVReaderBuilder(br).withCSVParser(this.csvParser).build()
            ) {
                String[] cells;
                String currentConcept = null;
                List<ColumnMeta> group = new ArrayList<>();
                boolean groupAllowed = true;

                while ((cells = csvReader.readNext()) != null) {
                    ColumnMeta meta = null;
                    try {
                         meta = this.columnMetaMapper.mapCSVRowToColumnMeta(cells);
                    } catch (ArrayIndexOutOfBoundsException e) {
                        String error = StringUtils.joinWith(",", Arrays.stream(cells).toArray());
                        this.columnMetaErrors.add("Unable to process columnMeta %s".formatted(error));
                    }

                    if (meta != null) {
                        String conceptName = meta.name();

                        if (!conceptName.equals(currentConcept)) {
                            if (!group.isEmpty()) {
                                // We must create a shallow copy of a group because we are going to clear it immediately after
                                // When we clear it, the group collection passed to the thread will be cleared.
                                ArrayList<ColumnMeta> columnMetas = new ArrayList<>(group);
                                columnMetaScopeExecutor.submit(() -> this.processColumnMetas(columnMetas));
                            }

                            group.clear();
                            currentConcept = conceptName;
                            groupAllowed = isAllowedByStudy(conceptName, allowedStudies);
                        }

                        if (groupAllowed) {
                            group.add(meta);
                        }
                    }
                }

                if (!group.isEmpty()) {
                    ArrayList<ColumnMeta> columnMetas = new ArrayList<>(group);
                    columnMetaScopeExecutor.submit(() -> this.processColumnMetas(columnMetas));
                }
            }
        } catch (IOException | CsvValidationException e) {
            throw new RuntimeException(e);
        }

        this.persistConcepts(allowedStudies);

        if (!this.columnMetaErrors.isEmpty()) {
            this.printColumnMetaErrorsToCSV(errorFile);
            return "Hydration has completed with errors. Errors can be found at: " + errorFile;
        }

        return "Success";
    }

     protected void persistConcepts(Set<String> allowedStudies) {
        CompletableFuture<Void> dbWriterFuture = startBatchMetadataConsumer();
        try {
            log.info("Persisting Tree to Database");

            List<DatasetModel> allByRefs;
            if (allowedStudies.isEmpty()) {
                allByRefs = this.datasetService.findAll();
            } else {
                 allByRefs = this.datasetService.findAllByRefs(allowedStudies.stream().toList());
            }

            this.persistTreeToDatabase(allByRefs);
            this.metadataBatchQueue.add(new ConceptMetadataModel()); // Empty conceptMetadataModel is the poison pill
            log.info("Waiting for column meta processing to complete.");
        } finally {
            dbWriterFuture.join();
            log.info("All tasks have been processed and batches flushed. Shutting down.");
        }
    }

    private CompletableFuture<Void> startBatchMetadataConsumer() {
        return CompletableFuture.runAsync(() -> {
            List<ConceptMetadataModel> batch = new ArrayList<>();
            while (true) {
                try {
                    ConceptMetadataModel metadataModel = this.metadataBatchQueue.take();
                    if (metadataModel.getConceptNodeId() == null) {
                        // NOTE: Poison pill has been reached. We should never have a metadataModel with a null concept node ID.

                        // Save the final batch
                        if (!batch.isEmpty()) {
                            this.conceptMetadataService.saveAll(batch);
                            batch.clear();
                        }

                        log.info("startBatchMetadataConsumer poison pill reached.");
                        return; // Done
                    }

                    batch.add(metadataModel);
                    if (batch.size() >= BATCH_SIZE) {
                        this.conceptMetadataService.saveAll(batch);
                        batch.clear();
                    }
                } catch (InterruptedException e) {
                    log.info("Error in metadata consumer thread. {}", e.getMessage());
                    this.columnMetaErrors.add("Error in metadata consumer thread. " + e.getMessage());
                }
            }
        }, Executors.newVirtualThreadPerTaskExecutor());
    }

    private void persistTreeToDatabase(List<DatasetModel> datasets) {
        HashMap<String, Long> datasetIDs = new HashMap<>();
        datasets.forEach(dataset -> datasetIDs.put(dataset.getRef(), dataset.getDatasetId()));
        Collection<ConceptNode> currentLayer = concurrentFullPathTree.getRoot().getChildren().values();

        List<DatasetModel> newDatasets = new ArrayList<>(currentLayer.size() - datasetIDs.size());
        currentLayer.forEach(node -> {
            if (!datasetIDs.containsKey(node.getDatasetRef())) {
                newDatasets.add(new DatasetModel(node.getDatasetRef(), "", "", ""));
            }
        });
        this.datasetService.saveAll(newDatasets);
        newDatasets.parallelStream().forEach(dataset -> datasetIDs.put(dataset.getRef(), dataset.getDatasetId()));

        int numberOfConceptPaths = 0;
        List<ConceptModel> batchModels = new ArrayList<>(BATCH_SIZE);
        List<ConceptNode> batchNodes = new ArrayList<>(BATCH_SIZE);
        while (!currentLayer.isEmpty()) {
            List<ConceptNode> nextLayer = new ArrayList<>();

            for (ConceptNode node : currentLayer) {
                nextLayer.addAll(node.getChildren().values());

                if (!"ROOT".equals(node.getParent().getConceptPath())) {
                    node.getConceptModel().setParentId(node.getParent().getConceptModel().getConceptNodeId());
                }

                node.getConceptModel().setDatasetId(datasetIDs.get(node.getDatasetRef()));
                batchModels.add(node.getConceptModel());
                batchNodes.add(node);

                if (batchModels.size() >= BATCH_SIZE) {
                    flushBatch(batchModels, batchNodes);
                }
            }

            if (!batchModels.isEmpty()) {
                flushBatch(batchModels, batchNodes);
            }

            numberOfConceptPaths += currentLayer.size();
            currentLayer = nextLayer;
        }

        log.info("Number of concept paths processed: {}", numberOfConceptPaths);
        concurrentFullPathTree.registry.clear();
    }

    private void flushBatch(List<ConceptModel> batchModels, List<ConceptNode> batchNodes) {
        conceptService.saveAll(batchModels);

        for (ConceptNode node : batchNodes) {
            ColumnMeta columnMeta = node.getColumnMeta();

            // If the node has columnMeta we watch to add it to a collection
            if (columnMeta != null) {
                queueValuesMetadata(columnMeta, node.getConceptModel().getConceptNodeId());
            }
        }

        // clear the batch after it has been flushed
        batchModels.clear();
        batchNodes.clear();
    }

    protected void processColumnMetas(List<ColumnMeta> columnMetas) {
        try {
            ColumnMeta columnMeta;
            if (columnMetas.size() == 1) {
                columnMeta = columnMetas.getFirst();
            } else {
                boolean isContinuous = columnMetas.stream().anyMatch(meta -> !meta.categorical());
                if (!isContinuous) {
                    columnMeta = flattenCategoricalColumnMeta(columnMetas);
                } else {
                    columnMeta = flattenContinuousColumnMeta(columnMetas);
                }
            }

            this.concurrentFullPathTree.ingestColumnMeta(columnMeta);
        } catch (IllegalArgumentException e) {
            this.columnMetaErrors.add(e.getMessage());
        }
    }

    private ColumnMeta flattenContinuousColumnMeta(List<ColumnMeta> columnMetas) throws IllegalArgumentException {
        if (columnMetas.getFirst().categorical()) {
            throw new IllegalArgumentException("Cannot flatten rows. Mixed concept types. Must be continuous OR categorical " +
                                       "for a concept path. ColumnMetas: " + StringUtils.joinWith(",", columnMetas));
        }

        final Double[] min = {columnMetas.getFirst().min()};
        final Double[] max = {columnMetas.getFirst().max()};

        for (ColumnMeta columnMeta : columnMetas) {
            if (columnMeta.categorical()) {
                if (columnMeta.categoryValues().size() > 1) {
                    throw new IllegalArgumentException("Cannot flatten rows. Mixed concept types. Must be continuous OR categorical " +
                                                       "for a concept path. ColumnMetas: " + StringUtils.joinWith(",", columnMetas));
                }

                double value = Double.parseDouble(columnMeta.categoryValues().getFirst());
                min[0] = Math.min(min[0], value);
                max[0] = Math.max(max[0], value);
            } else {
                min[0] = Math.min(min[0], columnMeta.min());
                max[0] = Math.max(max[0], columnMeta.max());
            }
        }

        return new ColumnMeta(
                columnMetas.getFirst().name(),
                null,
                null,
                false,
                columnMetas.getFirst().categoryValues(),
                min[0],
                max[0],
                null,
                null,
                null,
                null
        );
    }

    protected ColumnMeta flattenCategoricalColumnMeta(List<ColumnMeta> columnMetas) {
        Set<String> setOfVals = new HashSet<>();
        columnMetas.forEach(columnMeta -> setOfVals.addAll(columnMeta.categoryValues()));

        List<String> values = new ArrayList<>(setOfVals);
        return new ColumnMeta(
                columnMetas.getFirst().name(),
                null,
                null,
                columnMetas.getFirst().categorical(),
                values,
                columnMetas.getFirst().min(),
                columnMetas.getFirst().max(),
                null,
                null,
                null,
                null
        );
    }

    protected void queueValuesMetadata(ColumnMeta columnMeta, Long conceptID) {
        try {
            List<String> values = columnMeta.categoryValues();
            if (!columnMeta.categorical()) {
                values = List.of(String.valueOf(columnMeta.min()), String.valueOf(columnMeta.max()));
            }

            String valuesJson = this.columnMetaUtility.listToJson(values);

            ConceptMetadataModel metadataModel = new ConceptMetadataModel(conceptID, "values", valuesJson);
            this.metadataBatchQueue.add(metadataModel);

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    protected boolean isAllowedByStudy(String conceptPath, Set<String> allowedStudies) {
        if (allowedStudies == null || allowedStudies.isEmpty()) {
            return true;
        }

        String root = rootSegment(conceptPath);
        if (root == null) {
            return false;
        }

        return allowedStudies.contains(root.toLowerCase());
    }

    protected String rootSegment(String conceptPath) {
        if (!StringUtils.isNotBlank(conceptPath)) {
            return null;
        }

        return conceptPath.split("\\\\")[1];
    }

    protected void printColumnMetaErrorsToCSV(String csvFilePath) {
        List<String[]> errors = this.columnMetaErrors.stream().map(error -> new String[]{error}).toList();
        try (CSVWriter writer = new CSVWriter(new FileWriter(csvFilePath, StandardCharsets.UTF_8))) {
            writer.writeAll(errors);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}