package edu.harvard.dbmi.avillach.dictionaryetl.loading.dto;

import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataModel;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public record LoadingContext(
        ConceptModelTree conceptModelTree,
        BlockingQueue<ConceptMetadataModel> metadataBatchQueue,
        LoadingErrorRegistry loadingErrorRegistry,
        Set<String> allowedStudies,
        String columnMetaCsvPath,
        String errorFilePath
) {
    public LoadingContext(Set<String> allowedStudies, String columnMetaCsvPath, String errorFilePath) {
        this(
            new ConceptModelTree(),
            new LinkedBlockingQueue<>(),
            new LoadingErrorRegistry(),
            allowedStudies,
            columnMetaCsvPath,
            errorFilePath
        );
    }
}
