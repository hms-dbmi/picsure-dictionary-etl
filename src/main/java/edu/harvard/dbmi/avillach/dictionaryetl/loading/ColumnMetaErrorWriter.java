package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import com.opencsv.CSVWriter;
import edu.harvard.dbmi.avillach.dictionaryetl.loading.dto.LoadingContext;
import edu.harvard.dbmi.avillach.dictionaryetl.loading.dto.LoadingErrorRegistry;
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

    public void writeErrors(LoadingContext context) {
        Set<String> errorSet = context.loadingErrorRegistry().getErrors();
        if (errorSet.isEmpty()) {
            log.info("No errors detected!");
            return;
        }

        List<String[]> errors = errorSet.stream()
                .map(error -> new String[]{error})
                .toList();

        log.info("There were errors during processing. Writing errors to file: {}", context.errorFilePath());
        try (CSVWriter writer = new CSVWriter(new FileWriter(context.errorFilePath(), StandardCharsets.UTF_8))) {
            writer.writeAll(errors);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write error file", e);
        }
    }

}
