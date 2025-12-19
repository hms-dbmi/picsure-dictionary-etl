package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import com.opencsv.CSVWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

@Component
public class ColumnMetaErrorWriter {

    private final Logger log = LoggerFactory.getLogger(ColumnMetaErrorWriter.class);
    private final LoadingErrorRegistry loadingErrorRegistry;

    public ColumnMetaErrorWriter(LoadingErrorRegistry loadingErrorRegistry) {
        this.loadingErrorRegistry = loadingErrorRegistry;
    }

    public void writeErrors(String csvFilePath) {
        Set<String> errorSet = this.loadingErrorRegistry.getErrors();
        if (errorSet.isEmpty()) {
            log.info("No errors detected!");
            return;
        }

        List<String[]> errors = errorSet.stream()
                .map(error -> new String[]{error})
                .toList();

        log.info("There were errors during processing. Writing errors to file: {}", csvFilePath);
        try (CSVWriter writer = new CSVWriter(new FileWriter(csvFilePath, StandardCharsets.UTF_8))) {
            writer.writeAll(errors);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write error file", e);
        }
    }

}
