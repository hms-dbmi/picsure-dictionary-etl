package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class DictionaryLoaderService {

    private final Logger log = LoggerFactory.getLogger(DictionaryLoaderService.class);

    private final ColumnMetaGroupingPipeline columnMetaGroupingPipeline;
    private final ColumnMetaTreePersister columnMetaTreePersister;
    private final ColumnMetaErrorWriter columnMetaErrorWriter;

    @Autowired
    public DictionaryLoaderService(ColumnMetaGroupingPipeline columnMetaGroupingPipeline, ColumnMetaTreePersister columnMetaTreePersister, ColumnMetaErrorWriter columnMetaErrorWriter) {
        this.columnMetaGroupingPipeline = columnMetaGroupingPipeline;
        this.columnMetaTreePersister = columnMetaTreePersister;
        this.columnMetaErrorWriter = columnMetaErrorWriter;
    }

    public String processColumnMetaCSV(String csvPath, String errorFile) throws RuntimeException {
        return processColumnMetaCSV(csvPath, errorFile, List.of());
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

        final Set<String> allowedStudies = studies.stream()
                        .filter(Objects::nonNull)
                        .map(String::trim)
                        .map(String::toLowerCase)
                        .collect(Collectors.toSet());

        this.columnMetaGroupingPipeline.run(csvPath, allowedStudies);
        this.columnMetaTreePersister.persist(allowedStudies);
        this.columnMetaErrorWriter.writeErrors(errorFile);

        return "Success";
    }



}