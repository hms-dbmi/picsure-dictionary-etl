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
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

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

    // --- Batch Configuration ---
    private static final int BATCH_SIZE = 5000;
    private final BlockingQueue<ConceptMetadataModel> metadataBatchQueue = new LinkedBlockingQueue<>();
    // ---------------------------

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
            errorFile = java.nio.file.Path.of(baseDir, "columnMetaErrors.csv").toString();
        } else if (!errorFile.endsWith(".csv")) {
            return "The error file must be a csv.";
        }

        if (csvPath == null) {
            csvPath = java.nio.file.Path.of(baseDir, "columnMeta.csv").toString();
        }

        final Set<String> allowedStudies = (studies == null || studies.isEmpty()) ? null :
                studies.stream()
                        .filter(Objects::nonNull)
                        .map(String::trim)
                        .map(String::toLowerCase)
                        .collect(java.util.stream.Collectors.toSet());

        CompletableFuture<Void> dbWriterFuture = startBatchMetadataConsumer();
        try (ExecutorService columnMetaScopeExecutor = Executors.newVirtualThreadPerTaskExecutor()) {
            try (BufferedReader br = new BufferedReader(new FileReader(csvPath));
                 CSVReader csvReader = new CSVReaderBuilder(br).withCSVParser(this.csvParser).build()
            ) {
                String[] columns;
                String currentConcept = null;
                List<ColumnMeta> group = new ArrayList<>();
                boolean groupAllowed = true;

                while ((columns = csvReader.readNext()) != null) {
                    ColumnMeta meta = null;
                    try {
                         meta = this.columnMetaMapper.mapCSVRowToColumnMeta(columns);
                    } catch (ArrayIndexOutOfBoundsException e) {
                        String error = StringUtils.joinWith(",", Arrays.stream(columns).toArray());
                        this.columnMetaErrors.add("Unable to process columnMeta %s".formatted(error));
                    }

                    if (meta != null) {
                        String conceptName = meta.name();

                        if (!conceptName.equals(currentConcept)) {
                            if (!group.isEmpty()) {
                                // We must create a shallow copy of group because we are going to clear it immediately after
                                // when we clear it the group collection passed to the thread will be cleared. This creates
                                // race conditions
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

        // The tree TreePath has been built. We now want to start inserting records
        log.info("Persisting Tree to Database");
        this.persistTreeToDatabase();

        this.metadataBatchQueue.add(new ConceptMetadataModel()); // Empty conceptMetadataModel is the posion pill
        dbWriterFuture.join();
        log.info("All tasks have been processed and batches flushed. Shutting down.");

        if (!this.columnMetaErrors.isEmpty()) {
            this.printColumnMetaErrorsToCSV(errorFile);
            return "Hydration has completed with errors. Errors can be found at: " + errorFile;
        }

        return "Success";
    }

    /**
     * NEW: Consumer thread that drains the queue and writes to DB in chunks.
     */
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
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }, Executors.newVirtualThreadPerTaskExecutor());
    }

    private void persistTreeToDatabase() {
        HashMap<String, Long> datasetIDs = new HashMap<>();
        this.datasetService.findAll().forEach(dataset -> datasetIDs.put(dataset.getRef(), dataset.getDatasetId()));
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

            // flush remaining models in the layer
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
    }

    private ColumnMeta flattenContinuousColumnMeta(List<ColumnMeta> columnMetas) {
        if (columnMetas.getFirst().categorical()) {
            throw new RuntimeException("Cannot flatten rows. Mixed concept types. Must be continuous OR categorical " +
                                       "for a concept path.");
        }

        final Double[] min = {columnMetas.getFirst().min()};
        final Double[] max = {columnMetas.getFirst().max()};

        columnMetas.forEach(columnMeta -> {
            if (columnMeta.categorical()) {
                if (columnMeta.categoryValues().size() > 1) {
                    throw new RuntimeException("Cannot flatten rows. Mixed concept types. Must be continuous OR categorical " +
                                               "for a concept path.");
                }

                double value = Double.parseDouble(columnMeta.categoryValues().getFirst());
                min[0] = Math.min(min[0], value);
                max[0] = Math.max(max[0], value);
            } else {
                min[0] = Math.min(min[0], columnMeta.min());
                max[0] = Math.max(max[0], columnMeta.max());
            }
        });

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

    private static boolean isAllowedByStudy(String conceptPath, Set<String> allowedStudies) {
        if (allowedStudies == null || allowedStudies.isEmpty()) {
            return true;
        }
        String root = rootSegment(conceptPath);
        if (root == null) return false;
        String rootLc = root.toLowerCase();
        return allowedStudies.contains(rootLc);
    }

    private static String rootSegment(String conceptPath) {
        if (conceptPath == null || conceptPath.isEmpty()) return null;
        int start = conceptPath.startsWith("\\") ? 1 : 0;
        int end = conceptPath.indexOf("\\", start);
        if (end == -1) return conceptPath.substring(start);
        return conceptPath.substring(start, end);
    }

    private void printColumnMetaErrorsToCSV(String csvFilePath) {
        List<String[]> errors = this.columnMetaErrors.stream().map(error -> new String[]{error}).toList();
        try (CSVWriter writer = new CSVWriter(new FileWriter(csvFilePath, StandardCharsets.UTF_8))) {
            writer.writeAll(errors);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}