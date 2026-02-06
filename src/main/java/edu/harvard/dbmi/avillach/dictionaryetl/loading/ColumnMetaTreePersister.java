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
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            log.info("Writing tree to database");
            List<CompletableFuture<Void>> metadataFutures = new ArrayList<>();
            List<ConceptMetadataModel> pendingMetadata = new ArrayList<>();

            persistConceptTreeModel(getDatasetModels(context.allowedStudies()), context, executor, metadataFutures, pendingMetadata);

            // Flush any remaining metadata
            if (!pendingMetadata.isEmpty()) {
                metadataFutures.add(saveMetadataBatchAsync(List.copyOf(pendingMetadata), executor));
            }

            log.info("Waiting for {} metadata batch(es) to complete.", metadataFutures.size());
            CompletableFuture.allOf(metadataFutures.toArray(new CompletableFuture[0])).join();
            log.info("All metadata batches saved. Shutting down.");
        }
    }

    private CompletableFuture<Void> saveMetadataBatchAsync(List<ConceptMetadataModel> batch, Executor executor) {
        return CompletableFuture.runAsync(() -> conceptMetadataService.saveAll(batch), executor);
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

    private void persistConceptTreeModel(
            List<DatasetModel> datasets,
            LoadingContext context,
            Executor executor,
            List<CompletableFuture<Void>> metadataFutures,
            List<ConceptMetadataModel> pendingMetadata
    ) {
        HashMap<String, Long> datasetIDs = new HashMap<>();
        datasets.forEach(dataset -> datasetIDs.put(dataset.getRef(), dataset.getDatasetId()));
        Collection<ConceptNode> currentLayer = context.conceptModelTree().getRoot().getChildren().values();

        List<DatasetModel> newDatasets = new ArrayList<>(Math.max(0, currentLayer.size() - datasetIDs.size()));
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
                    collectMetadata(batchNodes, pendingMetadata, metadataFutures, executor);
                    batchModels.clear();
                    batchNodes.clear();
                }
            }

            if (!batchModels.isEmpty()) {
                this.conceptService.saveAll(batchModels);
                collectMetadata(batchNodes, pendingMetadata, metadataFutures, executor);
                batchModels.clear();
                batchNodes.clear();
            }

            numberOfConceptPaths += currentLayer.size();
            currentLayer = nextLayer;
            depth++;
        }

        log.info("Number of concept paths processed: {}", numberOfConceptPaths);
    }

    private void collectMetadata(
            List<ConceptNode> batchNodes,
            List<ConceptMetadataModel> pendingMetadata,
            List<CompletableFuture<Void>> metadataFutures,
            Executor executor
    ) {
        for (ConceptNode node : batchNodes) {
            ConceptMetadataModel conceptMetadataModel = node.getConceptMetadataModel();
            if (conceptMetadataModel != null) {
                conceptMetadataModel.setConceptNodeId(node.getConceptModel().getConceptNodeId());
                pendingMetadata.add(conceptMetadataModel);

                if (pendingMetadata.size() >= BATCH_SIZE) {
                    metadataFutures.add(saveMetadataBatchAsync(List.copyOf(pendingMetadata), executor));
                    pendingMetadata.clear();
                }
            }
        }
    }

}
