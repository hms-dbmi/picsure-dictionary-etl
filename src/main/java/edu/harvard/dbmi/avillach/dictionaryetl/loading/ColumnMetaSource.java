package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import edu.harvard.dbmi.avillach.dictionaryetl.loading.dto.LoadingContext;
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
    private final CSVParser csvParser;

    public ColumnMetaSource(ColumnMetaMapper columnMetaMapper, CSVParser csvParser) {
        this.columnMetaMapper = columnMetaMapper;
        this.csvParser = csvParser;
    }

    public CloseableIterator<ColumnMeta> read(LoadingContext context) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(context.columnMetaCsvPath()));
            CSVReader csvReader = new CSVReaderBuilder(br).withCSVParser(this.csvParser).build();
            return new ColumnMetaIterator(csvReader, context);
        } catch (IOException e) {
            throw new RuntimeException("Failed to open CSV file: " + context.columnMetaCsvPath(), e);
        }
    }

    private class ColumnMetaIterator implements CloseableIterator<ColumnMeta> {

        private final CSVReader csvReader;
        private final LoadingContext context;
        private ColumnMeta nextElement;
        private boolean hasNextConsumed = true; // force fetch on first call

        private ColumnMetaIterator(CSVReader csvReader, LoadingContext context) {
            this.csvReader = csvReader;
            this.context = context;
        }

        @Override
        public void close() {
            try {
                this.csvReader.close();
            } catch (IOException e) {
                this.context.loadingErrorRegistry().addError("Error closing CSV reader: " + e.getMessage());
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
                        this.context.loadingErrorRegistry().addError("Unable to process columnMeta %s".formatted(error));
                    }
                }

            } catch (CsvValidationException | IOException e) {
                throw new RuntimeException(e);
            }

            return null;
        }

    }
}
