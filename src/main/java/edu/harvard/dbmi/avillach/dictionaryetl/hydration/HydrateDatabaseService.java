package edu.harvard.dbmi.avillach.dictionaryetl.hydration;

import edu.harvard.dbmi.avillach.dictionaryetl.concept.*;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.io.*;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

@Service
public class HydrateDatabaseService {

    private final Logger log = LoggerFactory.getLogger(HydrateDatabaseService.class);
    private final ColumnMetaMapper columnMetaMapper;
    private final DatasetService datasetService;
    private final ConceptService conceptService;
    private final ConceptMetadataService conceptMetadataService;

    @Autowired
    public HydrateDatabaseService(ColumnMetaMapper columnMetaMapper, DatasetService datasetService, ConceptService conceptService, ConceptMetadataService conceptMetadataService, DataSource dataSource) throws SQLException {
        this.columnMetaMapper = columnMetaMapper;
        this.datasetService = datasetService;
        this.conceptService = conceptService;
        this.conceptMetadataService = conceptMetadataService;

        int maxConnections = dataSource.getConnection().getMetaData().getMaxConnections();
        this.fixedThreadPool = Executors.newFixedThreadPool(maxConnections);
    }

    private final ConcurrentHashMap<String, Long> datasetRefIDs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> conceptPaths = new ConcurrentHashMap<>();
    private volatile DatasetModel userDefinedDataset;
    private final AtomicInteger task = new AtomicInteger();
    private final LinkedBlockingQueue<List<ColumnMeta>> readyToLoadMetadata = new LinkedBlockingQueue<>();
    private final ExecutorService fixedThreadPool;
    private volatile boolean running = true;

    /**
     * If a List of ColumnMetas in the readyToLoadMetadata cannot be processed due to an error it will be inserted into
     * this list.
     */
    private static ConcurrentSkipListSet<List<ColumnMeta>> columnMetaErrors =
            new ConcurrentSkipListSet<>(Comparator.comparing(metas -> metas.getFirst().name())
            );

    /**
     * Uses the columnMeta.csv that is created as part of the HPDS ETL to hydrate the data-dictionary database.
     * The CSV file is expected to exist at /opt/local/hpds/columnMeta.csv.
     *
     * @return boolean Returns true if successful, else returns false
     */
    public boolean processColumnMetaCSV(String csvPath, String datasetName, String errorPath) {
        if (errorPath == null) {
            // TODO: The default error path will be used.
            // Record all ColumnMetas that failed to print in a new map.
            errorPath = "/opt/local/hpds/columnMeta.csv";
        }

        if (StringUtils.hasLength(datasetName)) {
            this.userDefinedDataset = this.datasetService.save(new DatasetModel(datasetName, "", "", ""));
        }

        this.startProcessing();
        try (BufferedReader br = new BufferedReader(new FileReader(csvPath))) {
            final String[] previousConceptPath = {""};
            List<ColumnMeta> columnMetas = new ArrayList<>();
            Stream<String> lines = br.lines();
            lines.forEach(line -> {
                Optional<ColumnMeta> columnMeta = this.columnMetaMapper.mapCSVRowToColumnMeta(line);
                if (columnMeta.isPresent()) {
                    if (!previousConceptPath[0].equals(columnMeta.get().name())) {
                        // We have reached a new concept path. We can add this list of column metas to the queue
                        // and start processing the next set of concept paths.
                        if (!columnMetas.isEmpty()) {
                            readyToLoadMetadata.add(new ArrayList<>(columnMetas));
                            columnMetas.clear();
                            previousConceptPath[0] = columnMeta.get().name();
                            this.task.getAndAdd(1);
                        }
                    }

                    columnMetas.add(columnMeta.get());
                }
            });

            // Wait until the queue has finished processing.
            while (this.task.get() != 0) {
                Thread.sleep(100); // Small sleep to prevent busy waiting
            }

            log.info("All tasks have been processed. Shutting down executor.");
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            running = false;
            this.fixedThreadPool.shutdownNow();
            log.error("Unable to process the following conceptPaths: {}", columnMetaErrors.size());
            columnMetaErrors.forEach(columnMetas -> log.error(columnMetas.getFirst().name()));
        }

        return true;
    }

    /**
     * This method creates a thread that continues to process the queue until the processingColumnMetaCSV sets running
     * to false.
     * <br/>
     * This method will continue until stopped by setting running to false or if an exception occurs.
     */
    private void startProcessing() {
        Thread watcherThread = new Thread(() -> {
            while (running) {
                try {
                    // take() is a blocking operation. It will block the thread until an item becomes available to
                    // process.
                    List<ColumnMeta> columnMetas = this.readyToLoadMetadata.take();
                    if (!columnMetas.isEmpty()) {
                        this.fixedThreadPool.submit(() -> processColumnMetas(columnMetas));
                    }
                } catch (InterruptedException e) {
                    log.error(e.getMessage());
                }
            }
        });

        watcherThread.setDaemon(true);
        watcherThread.start();
    }

    /**
     * Processes the provided list of columns metas ultimately creating database records to represent the concept path.
     *
     * @param columnMetas One or more ColumnMetas. The list MUST not contain more than one ColumnMeta for continuous
     *                    variables. It will not be processed correctly. The code operates under the assumption that only categorical
     *                    variables can have multiple rows in the columnMeta.csv.
     */
    protected void processColumnMetas(List<ColumnMeta> columnMetas) {
        ColumnMeta columnMeta;
        ConceptNode rootConceptNode;
        try {
            if (columnMetas.size() == 1) {
                columnMeta = columnMetas.getFirst();
                rootConceptNode = buildConceptHierarchy(columnMetas.getFirst().name());
            } else {
                boolean isContinuous = columnMetas.stream().anyMatch(meta -> !meta.categorical());
                if (!isContinuous) {
                    columnMeta = flattenColumnMeta(columnMetas);
                    rootConceptNode = buildConceptHierarchy(columnMeta.name());
                } else {
                    throw new RuntimeException("Continuous variable concept paths must have one row in the CSV data.");
                }
            }

            createDatabaseEntries(rootConceptNode, columnMeta);
        } catch (Exception e) {
            log.error("Error processing concept path: {} with values for column metas: {}",
                    columnMetas.getFirst().name(),
                    e.getMessage(), e);
            columnMetaErrors.add(columnMetas);
        } finally {
            // Decrement the task counter no matter what happens
            task.getAndDecrement();
        }
    }

    /**
     * Is some cases we need to flatten a group of ColumnMetaRows down to a single ColumnMeta.
     * This is the case for Concept Paths in the columnMeta.csv that have more than one row. This should only occur
     * for categorical concepts.
     * ColumnMeta
     *
     * @param columnMetas List of ColumnMeta that have the same concept path and are categorical
     * @return A single ColumnMeta that contains ALL the values combined into a single list.
     */
    protected ColumnMeta flattenColumnMeta(List<ColumnMeta> columnMetas) {
        Set<String> setOfVals = new HashSet<>();
        columnMetas.forEach(columnMeta -> {
            setOfVals.addAll(columnMeta.categoryValues());
        });

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

    /**
     * Creates a hierarchy of concepts for a given concept path.
     * <br/>
     * <strong>Example:</strong>
     * conceptPath = \examination\blood pressure\mean diastolic\
     * <strong>>Result:</strong>
     * Root ConceptNode = \examination\
     * First Child = \examination\blood pressure\
     * Second Child = \examination\blood pressure\mean diastolic\
     * <br/>
     * There will be as many Concept Nodes as there are segments in the concept path.
     *
     * @param conceptPath The root ConceptNode for the given concept path.
     * @return The root concept node of the given concept path.
     */
    protected ConceptNode buildConceptHierarchy(String conceptPath) {
        ConceptNode root = null;
        String[] segments = conceptPath.split("\\\\");
        StringBuilder currentPath = new StringBuilder();
        ConceptNode parent = null;
        for (String segment : segments) {
            if (segment.isEmpty()) continue;

            if (currentPath.isEmpty()) {
                currentPath.append("\\");
            }

            currentPath.append(segment).append("\\");
            String nodePath = currentPath.toString();
            ConceptNode currentNode = new ConceptNode(nodePath, segment);

            if (root == null) {
                root = currentNode;
            }

            if (parent != null) {
                parent.setChild(currentNode);
            }

            parent = currentNode;
        }

        return root;
    }

    protected void createDatabaseEntries(ConceptNode rootConceptNode, ColumnMeta columnMeta) {
        Long datasetID;
        if (this.userDefinedDataset != null) {
            datasetID = this.userDefinedDataset.getDatasetId();
        } else {
            datasetID = this.getDatasetRefID(rootConceptNode.getName());
        }

        ConceptNode currentNode = rootConceptNode;
        Long parentConceptID = null;
        while (currentNode != null) {
            Long conceptNodeID = createConceptModel(currentNode, columnMeta, datasetID, parentConceptID);
            if (currentNode.getChild() == null) {
                // We have reached the leaf node. We can create concept metadata
                buildValuesMetadata(columnMeta, conceptNodeID);
            }

            currentNode = currentNode.getChild();
            parentConceptID = conceptNodeID;
        }
    }

    private Long createConceptModel(ConceptNode currentNode, ColumnMeta columnMeta, Long datasetID,
                                    Long parentConceptID) {
        return this.conceptPaths.computeIfAbsent(currentNode.getName(), name -> {
            String conceptPath = currentNode.getConceptPath();
            ConceptModel conceptModel = new ConceptModel(
                    datasetID,
                    name,
                    "",
                    ConceptTypes.CATEGORICAL.conceptType(columnMeta.categorical()),
                    conceptPath,
                    parentConceptID
            );

            conceptModel = this.conceptService.save(conceptModel);
            return conceptModel.getConceptNodeId();
        });
    }

    protected void buildValuesMetadata(ColumnMeta columnMeta, Long conceptID) {
        List<String> values = columnMeta.categoryValues();
        if (!columnMeta.categorical()) {
            values = List.of(String.valueOf(columnMeta.min()), String.valueOf(columnMeta.max()));
        }

        this.conceptMetadataService.save(new ConceptMetadataModel(conceptID, "values", values.toString()));
    }

    private Long getDatasetRefID(String datasetRef) {
        return this.datasetRefIDs.computeIfAbsent(datasetRef, ref -> {
            DatasetModel datasetModel = new DatasetModel(ref, "", "", "");
            datasetModel = this.datasetService.save(datasetModel); // Blocking call
            return datasetModel.getDatasetId();
        });
    }

    private void printColumnMetaErrorsToCSV() {

    }
}
