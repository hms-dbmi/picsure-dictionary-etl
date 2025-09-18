package edu.harvard.dbmi.avillach.dictionaryetl.Utility;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;

/**
 * Utility class for CSV file operations
 */
@Component
public class CSVUtility {

    private static final Logger log = LoggerFactory.getLogger(CSVUtility.class);
    private static final int DEFAULT_BATCH_SIZE = 1000;

    /**
     * Creates a new CSV file with the specified headers
     *
     * @param fullPath Full path to the CSV file
     * @param headers  Headers for the CSV file
     */
    public void createCSVFile(String fullPath, String... headers) {
        File file = new File(fullPath);
        if (file.exists()) {
            boolean delete = file.delete();
            if (!delete) {
                throw new RuntimeException("Unable to delete existing file. File: " + fullPath);
            }
        }

        try {
            log.debug("Creating CSV file: {}", fullPath);
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
                log.debug("Writing headers to CSV file: {}", fullPath);
                log.debug("Headers: {}", String.join(", ", headers));
                writer.writeNext(headers);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Writes data to a CSV file in batches
     *
     * @param filePath   Full path to the CSV file
     * @param dataItems  List of data items to write
     * @param rowMapper  Function to map a data item to a CSV row
     * @param <T>        Type of data item
     */
    public <T> void writeDataToCSV(String filePath, List<T> dataItems, Function<T, String[]> rowMapper) {
        writeDataToCSV(filePath, dataItems, rowMapper, DEFAULT_BATCH_SIZE);
    }

    /**
     * Writes data to a CSV file in batches
     *
     * @param filePath   Full path to the CSV file
     * @param dataItems  List of data items to write
     * @param rowMapper  Function to map a data item to a CSV row
     * @param batchSize  Size of batches for writing
     * @param <T>        Type of data item
     */
    public <T> void writeDataToCSV(String filePath, List<T> dataItems, Function<T, String[]> rowMapper, int batchSize) {
        try (CSVWriter writer = new CSVWriter(new FileWriter(filePath, true))) {
            List<String[]> batch = new ArrayList<>();

            for (T item : dataItems) {
                String[] row = rowMapper.apply(item);
                batch.add(row);

                if (batch.size() >= batchSize) {
                    writer.writeAll(batch);
                    writer.flush();
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                writer.writeAll(batch);
                writer.flush();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error writing to CSV file: " + filePath, e);
        }
    }

    /**
     * Merges a source CSV file into a destination CSV file, skipping the header row
     *
     * @param sourceFilePath      Source CSV file path
     * @param destinationFilePath Destination CSV file path
     */
    public void mergeCSVFiles(String sourceFilePath, String destinationFilePath) {
        try (CSVReader reader = new CSVReader(new FileReader(sourceFilePath));
             CSVWriter writer = new CSVWriter(new FileWriter(destinationFilePath, true))) {
            // Skip the header row
            reader.readNext();
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                writer.writeNext(nextLine);
            }
        } catch (IOException | CsvValidationException e) {
            throw new RuntimeException("Error merging CSV files", e);
        }
    }

    /**
     * Removes a directory if it is empty
     *
     * @param directoryPath Path to the directory
     * @return true if directory was deleted, false otherwise
     */
    public boolean removeDirectoryIfEmpty(String directoryPath) {
        File directory = new File(directoryPath);
        if (directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (files != null && files.length == 0) {
                boolean deleted = directory.delete();
                if (deleted) {
                    log.info("Deleted empty directory: {}", directoryPath);
                    return true;
                } else {
                    log.warn("Failed to delete empty directory: {}", directoryPath);
                }
            }
        }

        return false;
    }

    /**
     * Removes a directory if it exists. This includes all files and subdirectories.
     * @param directoryPath Path to the directory
     * @return true if the root directory was deleted, false otherwise. If we are unable to delete the directory, we will
     * return false.
     */
    public boolean removeDirectoryIfExists(String directoryPath) {
        File directory = new File(directoryPath);
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    boolean deleted;
                    if (file.isDirectory()) {
                        deleted = removeDirectoryIfExists(file.getAbsolutePath());
                    } else {
                        deleted = file.delete();
                    }

                    if (!deleted) {
                        log.warn("Failed to delete file: {}", file.getAbsolutePath());
                        return false;
                    }
                }
            }

            boolean deleted = directory.delete();
            if (deleted) {
                log.info("Deleted directory: {}", directoryPath);
                return true;
            } else {
                log.warn("Failed to delete directory: {}", directoryPath);
            }
        }

        return false;
    }

    public static Map<String, Integer> buildCsvInputsHeaderMap(String[] inputHeaders) {
            Map<String, Integer> inputsHeaders = new HashMap<>();
            for (int i = 0; i < inputHeaders.length; i++) {
                    inputsHeaders.put(inputHeaders[i], i);
            }
            return inputsHeaders;
    }
    /*Used to get metadata keys for concepts, categories, facets and facet categories for concept/facet mappings*/
    public static List<String> getExtraColumns(String[] coreHeaders, Map<String, Integer> inputHeaderMap){
        List<String> metaColumnNames = new ArrayList<>();
        if (!inputHeaderMap.keySet().containsAll(Arrays.asList(coreHeaders))) {
                        return null;
         }
        inputHeaderMap.keySet().forEach(k -> {
              if (!Arrays.asList(coreHeaders).contains(k)) {
                 metaColumnNames.add(k);
              }
          });
          return metaColumnNames;
    }
}
