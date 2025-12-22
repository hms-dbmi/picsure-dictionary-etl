package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataService;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptService;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetService;
import edu.harvard.dbmi.avillach.dictionaryetl.loading.dto.LoadingContext;
import edu.harvard.dbmi.avillach.dictionaryetl.loading.model.ConceptNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;

@Component
public class ColumnMetaTreePersister {

    private final Logger log = LoggerFactory.getLogger(ColumnMetaTreePersister.class);

    private final DatasetService datasetService;
    private final ConceptService conceptService;
    private final ConceptMetadataService conceptMetadataService;

    private static final int BATCH_SIZE = 5000;

    public ColumnMetaTreePersister(DatasetService datasetService, ConceptService conceptService, ConceptMetadataService conceptMetadataService) {
        this.datasetService = datasetService;
        this.conceptService = conceptService;
        this.conceptMetadataService = conceptMetadataService;
    }

    protected void persist(LoadingContext context) {
        try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {
            CompletableFuture<Void> dbWriterFuture = startBatchMetadataConsumer(executorService, context);
            try {
                log.info("Writing tree to database");
                persistConceptTreeModel(getDatasetModels(context.allowedStudies()), context);
            } finally {
                // Ensure the poison pill is added to the metadataBatchQueue
                // even if an exception occurs
                context.metadataBatchQueue().add(new ConceptMetadataModel());
            }

            log.info("Waiting for column meta processing to complete.");
            dbWriterFuture.join();
            log.info("All tasks have been processed and batches flushed. Shutting down.");
        }
    }

    private CompletableFuture<Void> startBatchMetadataConsumer(Executor executor, LoadingContext context) {
        return CompletableFuture.runAsync(() -> {
            List<ConceptMetadataModel> batch = new ArrayList<>();
            while (true) {
                try {
                    ConceptMetadataModel metadataModel = context.metadataBatchQueue().take();
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
                    context.loadingErrorRegistry().addError("Error in metadata consumer thread. " + e.getMessage());
                }
            }
        }, executor);
    }

    private List<DatasetModel> getDatasetModels(Set<String> allowedStudies) {
        List<DatasetModel> allByRefs;
        if (allowedStudies.isEmpty()) {
            allByRefs = this.datasetService.findAll();
        } else {
            allByRefs = this.datasetService.findAllByRefs(allowedStudies.stream().toList());
        }
        return allByRefs;
    }

    private void persistConceptTreeModel(List<DatasetModel> datasets, LoadingContext context) {
        HashMap<String, Long> datasetIDs = new HashMap<>();
        datasets.forEach(dataset -> datasetIDs.put(dataset.getRef(), dataset.getDatasetId()));
        Collection<ConceptNode> currentLayer = context.conceptModelTree().getRoot().getChildren().values();

        List<DatasetModel> newDatasets = new ArrayList<>(currentLayer.size() - datasetIDs.size());
        currentLayer.forEach(node -> {
            if (!datasetIDs.containsKey(node.getDatasetRef())) {
                newDatasets.add(new DatasetModel(node.getDatasetRef(), "", "", ""));
            }
        });
        this.datasetService.saveAll(newDatasets);
        newDatasets.forEach(dataset -> datasetIDs.put(dataset.getRef(), dataset.getDatasetId()));

        int numberOfConceptPaths = 0;
        List<ConceptModel> batchModels = new ArrayList<>(BATCH_SIZE);
        List<ConceptNode> batchNodes = new ArrayList<>(BATCH_SIZE);
        int depth = 0;
        while (!currentLayer.isEmpty()) {
            List<ConceptNode> nextLayer = new ArrayList<>();
            log.info("{} concept nodes at depth {}", currentLayer.size(), depth);

            for (ConceptNode node : currentLayer) {
                nextLayer.addAll(node.getChildren().values());

                if (!"ROOT".equals(node.getParent().getConceptPath())) {
                    node.getConceptModel().setParentId(node.getParent().getConceptModel().getConceptNodeId());
                }

                node.getConceptModel().setDatasetId(datasetIDs.get(node.getDatasetRef()));
                batchModels.add(node.getConceptModel());
                batchNodes.add(node);

                if (batchModels.size() >= BATCH_SIZE) {
                    this.conceptService.saveAll(batchModels);
                    queueMetadata(batchNodes, context);
                    flushBatch(batchModels, batchNodes);
                }
            }

            if (!batchModels.isEmpty()) {
                this.conceptService.saveAll(batchModels);
                queueMetadata(batchNodes, context);
                flushBatch(batchModels, batchNodes);
            }

            numberOfConceptPaths += currentLayer.size();
            currentLayer = nextLayer;
            depth++;
        }

        log.info("Number of concept paths processed: {}", numberOfConceptPaths);
    }

    private void queueMetadata(List<ConceptNode> batchNodes, LoadingContext context) {
        for (ConceptNode node : batchNodes) {
            ConceptMetadataModel conceptMetadataModel = node.getConceptMetadataModel();

            // If the node has columnMeta we watch to add it to a collection
            if (conceptMetadataModel != null) {
                conceptMetadataModel.setConceptNodeId(node.getConceptModel().getConceptNodeId());
                context.metadataBatchQueue().add(conceptMetadataModel);
            }
        }
    }

    private void flushBatch(List<ConceptModel> batchModels, List<ConceptNode> batchNodes) {
        batchModels.clear();
        batchNodes.clear();
    }

}
