package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import edu.harvard.dbmi.avillach.dictionaryetl.loading.dto.LoadingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    public String processColumnMetaCSV(String csvPath, String errorFile) {
        return processColumnMetaCSV(csvPath, errorFile, List.of());
    }

    public String processColumnMetaCSV(String csvPath, String errorFile, List<String> studies) {
        String baseDir = System.getProperty("hpds.data.dir", "/opt/local/hpds");

        if (errorFile == null) {
            errorFile = Path.of(baseDir, "columnMetaErrors.csv").toString();
        } else if (!errorFile.endsWith(".csv")) {
            return "The error file must be a csv.";
        }

        if (csvPath == null) {
            csvPath = baseDir;
        }

        List<Path> csvFiles = findColumnMetaFiles(Path.of(csvPath));
        if (csvFiles.isEmpty()) {
            log.warn("No columnMeta.csv files found in {}", csvPath);
            return "No columnMeta.csv files found in " + csvPath;
        }

        final Set<String> allowedStudies = studies.stream()
                        .filter(Objects::nonNull)
                        .map(String::trim)
                        .map(String::toLowerCase)
                        .collect(Collectors.toSet());

        log.info("Processing Studies: {}", allowedStudies);
        log.info("Found {} columnMeta.csv file(s) to process", csvFiles.size());

        for (Path csvFile : csvFiles) {
            log.info("Processing file: {}", csvFile);
            LoadingContext context = new LoadingContext(allowedStudies, csvFile.toString(), errorFile);
            try {
                this.columnMetaGroupingPipeline.run(context);
                this.columnMetaTreePersister.persist(context);
            } catch (Exception e) {
                log.info(e.getMessage());
            } finally {
                this.columnMetaErrorWriter.writeErrors(context);
            }
        }

        return "Success";
    }

    /**
     * Finds columnMeta.csv files to process. If the given path is a regular file, it is returned directly.
     * If it is a directory, all files named "columnMeta.csv" are found recursively in subdirectories.
     */
    private List<Path> findColumnMetaFiles(Path path) {
        if (Files.isRegularFile(path)) {
            return List.of(path);
        }

        try (Stream<Path> walk = Files.walk(path)) {
            return walk
                .filter(Files::isRegularFile)
                .filter(p -> p.getFileName().toString().equals("columnMeta.csv"))
                .sorted()
                .collect(Collectors.toList());
        } catch (IOException e) {
            log.error("Failed to search for columnMeta.csv files in {}", path, e);
            return List.of();
        }
    }



}