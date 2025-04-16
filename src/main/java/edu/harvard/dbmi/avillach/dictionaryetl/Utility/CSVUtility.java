package edu.harvard.dbmi.avillach.dictionaryetl.Utility;

import com.opencsv.CSVWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

@Component
public class CSVUtility {

    private static final Logger log = LoggerFactory.getLogger(CSVUtility.class);

    public void createCSVFile(String fullPath, String... headers) {
        File file = new File(fullPath);
        if (file.exists()) {
            boolean delete = file.delete();
            if (!delete) {
                throw new RuntimeException("Unable to delete existing file. File: " + fullPath);
            }
        }

        try {
            log.info("Creating CSV file: {}", fullPath);
            // create the parent directories if they do not exist
            File parentDir = file.getParentFile();
            if (parentDir != null && !parentDir.exists()) {
                boolean mkdirs = parentDir.mkdirs();
                if (!mkdirs) {
                    throw new RuntimeException("Unable to create parent directories. Path: " + parentDir.getAbsolutePath());
                }
            }
            boolean newFile = file.createNewFile();
            if (!newFile) {
                throw new RuntimeException("Unable to create new file. File: " + fullPath);
            }

            try (CSVWriter writer = new CSVWriter(new FileWriter(fullPath))) {
                log.info("Writing headers to CSV file: {}", fullPath);
                log.info("Headers: {}", String.join(", ", headers));
                writer.writeNext(headers);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
