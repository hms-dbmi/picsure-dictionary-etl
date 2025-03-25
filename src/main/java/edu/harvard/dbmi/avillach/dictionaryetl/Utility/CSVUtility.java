package edu.harvard.dbmi.avillach.dictionaryetl.Utility;

import com.opencsv.CSVWriter;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

@Component
public class CSVUtility {

    public void createCSVFile(String fullPath, String... headers) {
        // if the file exists, delete it.
        File file = new File(fullPath);
        if (file.exists()) {
            boolean delete = file.delete();
            if (!delete) {
                throw new RuntimeException("Unable to delete existing file. File: " + fullPath);
            }
        }

        try {
            boolean newFile = file.createNewFile();
            if (!newFile) {
                throw new RuntimeException("Unable to create new file. File: " + fullPath);
            }

            try (CSVWriter writer = new CSVWriter(new FileWriter(fullPath))) {
                writer.writeNext(headers);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
