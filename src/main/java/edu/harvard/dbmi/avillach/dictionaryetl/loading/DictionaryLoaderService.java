package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import com.fasterxml.jackson.core.JsonProcessingException;
import edu.harvard.dbmi.avillach.dictionaryetl.Utility.ColumnMetaUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.columnmeta.AbstractColumnMetaProcessor;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.*;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

@Service
public class DictionaryLoaderService extends AbstractColumnMetaProcessor {

    private final Logger log = LoggerFactory.getLogger(DictionaryLoaderService.class);
    private final DatasetService datasetService;
    private final ConceptService conceptService;
    private final ConceptMetadataService conceptMetadataService;
    private final ColumnMetaUtility columnMetaUtility;

    private final ConcurrentHashMap<String, Long> datasetRefIDs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> conceptPaths = new ConcurrentHashMap<>();
    private final DataSource dataSource;

    @Autowired
    public DictionaryLoaderService(ColumnMetaMapper columnMetaMapper, DatasetService datasetService, ConceptService conceptService, ConceptMetadataService conceptMetadataService, DataSource dataSource, ColumnMetaUtility columnMetaUtility) throws SQLException {
        super(columnMetaMapper);
        this.datasetService = datasetService;
        this.conceptService = conceptService;
        this.conceptMetadataService = conceptMetadataService;
        this.columnMetaUtility = columnMetaUtility;
        this.dataSource = dataSource;
    }

    /**
     * This method creates a thread that continues to process the queue until the processingColumnMetaCSV sets running
     * to false.
     * <br/>
     * This method will continue until stopped by setting running to false or if an exception occurs.
     */
    protected void startProcessing() {
        Thread loadingThread = new Thread(() -> {
            while (this.running) {
                try {
                    ColumnMeta columnMeta = this.processedColumnMetas.take();
                    this.fixedThreadPool.submit(() -> {
                        ConceptNode rootConceptNode = buildConceptHierarchy(columnMeta.name());
                        createDatabaseEntries(rootConceptNode, columnMeta);
                    });
                } catch (InterruptedException e) {
                    log.error(e.getMessage());
                }
            }
        });

        loadingThread.setDaemon(true);
        loadingThread.start();
    }

    @Override
    protected ExecutorService createThreadPool() {
        try {
            int maxConnections = this.dataSource.getConnection().getMetaData().getMaxConnections();
            return Executors.newFixedThreadPool(maxConnections);
        }  catch (SQLException e) {
            log.error(e.getMessage());
        }

        return null;
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
        Long datasetID = this.getDatasetRefID(rootConceptNode.getName());
        ConceptNode currentNode = rootConceptNode;
        Long parentConceptID = null;
        while (currentNode != null) {
            Long conceptNodeID = null;
            if (currentNode.getChild() == null) {
                // We have reached the leaf node. We can create concept metadata
                conceptNodeID = createConceptModel(currentNode, columnMeta, datasetID, parentConceptID);
                buildValuesMetadata(columnMeta, conceptNodeID);
            } else {
                conceptNodeID = createConceptModel(currentNode, null, datasetID, parentConceptID);
            }

            currentNode = currentNode.getChild();
            parentConceptID = conceptNodeID;
        }
    }

    private Long createConceptModel(ConceptNode currentNode, ColumnMeta columnMeta, Long datasetID,
                                    Long parentConceptID) {
        log.debug("Creating concept model for concept path: {}", currentNode.getName());
        return this.conceptPaths.computeIfAbsent(currentNode.getName(), name -> {
            Optional<ConceptModel> optConceptModel = this.conceptService.findByConcept(name);
            ConceptModel conceptModel;
            if (optConceptModel.isEmpty()) {
                String conceptPath = currentNode.getConceptPath();
                conceptModel = new ConceptModel(
                        datasetID,
                        name,
                        "",
                        ConceptTypes.conceptTypeFromColumnMeta(columnMeta),
                        conceptPath,
                        parentConceptID
                );

                conceptModel = this.conceptService.save(conceptModel);
            } else {
                conceptModel = optConceptModel.get();
            }

            return conceptModel.getConceptNodeId();
        });
    }

    protected void buildValuesMetadata(ColumnMeta columnMeta, Long conceptID) {
        try {
            List<String> values = columnMeta.categoryValues();
            if (!columnMeta.categorical()) {
                values = List.of(String.valueOf(columnMeta.min()), String.valueOf(columnMeta.max()));
            }

            String valuesJson = this.columnMetaUtility.listToJson(values);
            ConceptMetadataModel metadataModel = new ConceptMetadataModel(conceptID, "values", valuesJson);
            this.conceptMetadataService.save(metadataModel);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private Long getDatasetRefID(String datasetRef) {
        return this.datasetRefIDs.computeIfAbsent(datasetRef, ref -> {
            Optional<DatasetModel> optDatasetModel = this.datasetService.findByRef(ref);
            DatasetModel datasetModel;
            if (optDatasetModel.isPresent()) {
                datasetModel = optDatasetModel.get();
            } else {
                datasetModel = new DatasetModel(ref, "", "", "");
                datasetModel = this.datasetService.save(datasetModel); // Blocking call
            }

            return datasetModel.getDatasetId();
        });
    }

}
