package edu.harvard.dbmi.avillach.dictionaryetl.loading.dto;

import java.util.Set;

public record LoadingContext(
        ConceptModelTree conceptModelTree,
        LoadingErrorRegistry loadingErrorRegistry,
        Set<String> allowedStudies,
        String columnMetaCsvPath,
        String errorFilePath
) {
    public LoadingContext(Set<String> allowedStudies, String columnMetaCsvPath, String errorFilePath) {
        this(
            new ConceptModelTree(),
            new LoadingErrorRegistry(),
            allowedStudies,
            columnMetaCsvPath,
            errorFilePath
        );
    }
}
