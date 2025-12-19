package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import edu.harvard.dbmi.avillach.dictionaryetl.loading.model.ColumnMeta;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.query.spi.CloseableIterator;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;

@Component
public class ColumnMetaSource {

    private final ColumnMetaMapper columnMetaMapper;
    private final LoadingErrorRegistry loadingErrorRegistry;
    private final CSVParser csvParser;

    public ColumnMetaSource(ColumnMetaMapper columnMetaMapper, LoadingErrorRegistry loadingErrorRegistry, CSVParser csvParser) {
        this.columnMetaMapper = columnMetaMapper;
        this.loadingErrorRegistry = loadingErrorRegistry;
        this.csvParser = csvParser;
    }

    public CloseableIterator<ColumnMeta> read(String csvPath) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(csvPath));
            CSVReader csvReader = new CSVReaderBuilder(br).withCSVParser(this.csvParser).build();
            return new ColumnMetaIterator(csvReader);
        } catch (IOException e) {
            throw new RuntimeException("Failed to open CSV file: " + csvPath, e);
        }
    }

    private class ColumnMetaIterator implements CloseableIterator<ColumnMeta> {

        private final CSVReader csvReader;
        private ColumnMeta nextElement;
        private boolean hasNextConsumed = true; // force fetch on first call

        private ColumnMetaIterator(CSVReader csvReader) {
            this.csvReader = csvReader;
        }

        @Override
        public void close() {
            try {
                this.csvReader.close();
            } catch (IOException e) {
                loadingErrorRegistry.addError("Error closing CSV reader: " + e.getMessage());
            }
        }

        @Override
        public boolean hasNext() throws RuntimeException {
            if (hasNextConsumed) {
                nextElement = fetchNextValid();
                hasNextConsumed = false;
            }

            return nextElement != null;
        }

        @Override
        public ColumnMeta next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            hasNextConsumed = true;
            return nextElement;
        }

        private ColumnMeta fetchNextValid() throws RuntimeException {
            try {
                String[] cells;
                while ((cells = csvReader.readNext()) != null) {
                    try {
                        return columnMetaMapper.mapCSVRowToColumnMeta(cells);
                    } catch (Exception e) {
                        String error = StringUtils.joinWith(",", Arrays.stream(cells).toArray());
                        loadingErrorRegistry.addError("Unable to process columnMeta %s".formatted(error));
                    }
                }

            } catch (CsvValidationException | IOException e) {
                throw new RuntimeException(e);
            }

            return null;
        }

    }
}
