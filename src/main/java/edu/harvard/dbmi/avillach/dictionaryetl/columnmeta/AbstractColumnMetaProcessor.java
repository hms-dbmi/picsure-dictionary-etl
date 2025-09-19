package edu.harvard.dbmi.avillach.dictionaryetl.columnmeta;

import com.opencsv.CSVWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

@Component
public abstract class AbstractColumnMetaProcessor {
    private final Logger log = LoggerFactory.getLogger(AbstractColumnMetaProcessor.class);

    private final LinkedBlockingQueue<List<ColumnMeta>> readyToLoadMetadata = new LinkedBlockingQueue<>();
    private final ColumnMetaMapper columnMetaMapper;
    /**
     * If a List of ColumnMetas in the readyToLoadMetadata cannot be processed due to an error it will be inserted into
     * this list.
     */
    private final ConcurrentSkipListSet<List<ColumnMeta>> columnMetaErrors =
            new ConcurrentSkipListSet<>(Comparator.comparing(metas -> metas.getFirst().name())
            );

    private final AtomicInteger task = new AtomicInteger();

    protected ExecutorService fixedThreadPool = null;
    protected final LinkedBlockingQueue<ColumnMeta> processedColumnMetas = new LinkedBlockingQueue<>();
    protected volatile boolean running = true;

    public AbstractColumnMetaProcessor(ColumnMetaMapper columnMetaMapper) throws SQLException {
        this.columnMetaMapper = columnMetaMapper;
    }

    /**
     * Initializes and starts the processing threads needed for handling column metadata.
     * This method is called by {@link #processColumnMetaCSV(String, String)} before processing begins.
     * 
     * <p>Implementations should:</p>
     * <ul>
     *   <li>Create and start any necessary threads for processing</li>
     *   <li>Set up any required consumers for the {@link #processedColumnMetas} queue</li>
     *   <li>Ensure threads are properly configured (e.g., as daemon threads if appropriate)</li>
     * </ul>
     * 
     * <p>The implementation should continue processing until {@link #running} is set to false.</p>
     * 
     * @see #processColumnMetaCSV(String, String)
     * @see #processColumnMetas(List)
     */
    protected abstract void startProcessing();

    /**
     * Creates and returns an {@link ExecutorService} to be used for parallel processing of column metadata.
     * This method is called during the initialization of the {@link AbstractColumnMetaProcessor}.
     *
     * <p>Implementations should:</p>
     * <ul>
     *   <li>Create an appropriate thread pool based on system resources or application requirements</li>
     *   <li>Consider database connection limits if database operations will be performed</li>
     *   <li>Return a non-null {@link ExecutorService} instance</li>
     * </ul>
     *
     * <p>The returned {@link ExecutorService} will be used for processing column metadata in parallel.</p>
     *
     * @return A non-null {@link ExecutorService} instance to be used for parallel processing
     * @throws IllegalStateException if the implementation returns null
     */
    protected abstract ExecutorService createThreadPool();

    /**
     * Uses the columnMeta.csv that is created as part of the HPDS ETL to hydrate the data-dictionary database.
     * The CSV file is expected to exist at /opt/local/hpds/columnMeta.csv.
     */
    public String processColumnMetaCSV(String csvPath, String errorFile) throws RuntimeException {
        this.fixedThreadPool = createThreadPool();
        if (this.fixedThreadPool == null) {
            throw new IllegalStateException("Subclass must provide a non-null ExecutorService");
        }

        if (errorFile == null) {
            errorFile = "/opt/local/hpds/columnMetaErrors.csv";
        } else if (!errorFile.endsWith(".csv")) {
            return "The error file must be a csv.";
        }

        if (csvPath == null) {
            csvPath = "/opt/local/hpds/columnMeta.csv";
        }

        this.processGroupedColumnMetas();
        this.startProcessing();
        try (BufferedReader br = new BufferedReader(new FileReader(csvPath))) {
            AtomicReference<String> previousConceptPath = new AtomicReference<>("");
            List<ColumnMeta> columnMetas = new ArrayList<>();
            Stream<String> lines = br.lines();
            lines.forEach(line -> {
                Optional<ColumnMeta> columnMeta = this.columnMetaMapper.mapCSVRowToColumnMeta(line);
                if (columnMeta.isPresent()) {
                    if (!previousConceptPath.get().equals(columnMeta.get().name())) {
                        // We have reached a new concept path. We can add this list of column metas to the queue
                        // and start processing the next set of concept paths.
                        if (!columnMetas.isEmpty()) {
                            readyToLoadMetadata.add(new ArrayList<>(columnMetas));
                            columnMetas.clear();
                            previousConceptPath.set(columnMeta.get().name());
                            this.task.getAndAdd(1);
                        }
                    }

                    columnMetas.add(columnMeta.get());
                }
            });

            // Wait until the queue has finished processing.
            while (this.task.get() != 0) {
                Thread.sleep(100); // Small sleep to prevent busy waiting
            }

            log.info("All tasks have been processed. Shutting down executor.");
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            running = false;
        }

        if (!this.columnMetaErrors.isEmpty()) {
            this.printColumnMetaErrorsToCSV(errorFile);
            return "Processing completed with errors. Some column metadata could not be processed. " +
                   "Details have been written to error file: " + errorFile;
        }

        return "Success";
    }

    private void processGroupedColumnMetas() {
        Thread processingThread = new Thread(() -> {
            while (running) {
                try {
                    // take() is a blocking operation. It will block the thread until an item becomes available to
                    // process.
                    List<ColumnMeta> columnMetas = this.readyToLoadMetadata.take();
                    if (!columnMetas.isEmpty()) {
                        this.fixedThreadPool.submit(() -> {
                            Optional<ColumnMeta> processColumnMeta = processColumnMetas(columnMetas);
                            processColumnMeta.ifPresent(processedColumnMetas::add);
                        });
                    }
                } catch (InterruptedException e) {
                    log.error(e.getMessage());
                }
            }
        });

        processingThread.setDaemon(true);
        processingThread.start();
    }

    /**
     * Processes the provided list of columns metas ultimately creating database records to represent the concept path.
     *
     * @param columnMetas One or more ColumnMetas. The list MUST not contain more than one ColumnMeta for continuous
     *                    variables. It will not be processed correctly. The code operates under the assumption that only categorical
     *                    variables can have multiple rows in the columnMeta.csv.
     */
    public Optional<ColumnMeta> processColumnMetas(List<ColumnMeta> columnMetas) {
        ColumnMeta columnMeta = null;
        try {
            if (columnMetas.size() == 1) {
                columnMeta = columnMetas.getFirst();
            } else {
                boolean isContinuous = columnMetas.stream().anyMatch(meta -> !meta.categorical());
                if (!isContinuous) {
                    columnMeta = flattenCategoricalColumnMeta(columnMetas);
                } else {
                    columnMeta = flattenContinuousColumnMeta(columnMetas);
                }
            }
        } catch (Exception e) {
            log.error("Error processing concept path: {} with values for column metas: {}",
                    columnMetas.getFirst().name(),
                    e.getMessage());
            columnMetaErrors.add(columnMetas);
        } finally {
            // Decrement the task counter no matter what happens
            task.getAndDecrement();
        }

        return columnMeta != null ? Optional.of(columnMeta) : Optional.empty();
    }

    /**
     * In some cases we need to flatten a group of ColumnMetaRows down to a single ColumnMeta.
     * This is the case for Continuous Concept Paths in the columnMeta.csv that have more than one row.
     *
     * @param columnMetas A List of ColumnMeta where the first ColumnMeta is expected to be a continuous value.
     * @return ColumnMeta that has a min and max based on all ColumnMetas in the list.
     */
    protected ColumnMeta flattenContinuousColumnMeta(List<ColumnMeta> columnMetas) {
        // This is a special case. Where the parent (first element) being rolled into must be continuous.
        if (columnMetas.getFirst().categorical()) {
            throw new RuntimeException("Cannot flatten rows. Mixed concept types. Must be continuous OR categorical " +
                                       "for a concept path.");
        }

        // As the list is processed the min and max will adjust to based on the "values" of other concepts.
        final Double[] min = {columnMetas.getFirst().min()};
        final Double[] max = {columnMetas.getLast().max()};

        columnMetas.forEach(columnMeta -> {
            if (columnMeta.categorical()) {
                if (columnMeta.categoryValues().size() > 1) {
                    throw new RuntimeException("Cannot flatten rows. Mixed concept types. Must be continuous OR categorical " +
                                               "for a concept path.");
                }

                double value = Double.parseDouble(columnMeta.categoryValues().getFirst());
                min[0] = Math.min(min[0], value);
                max[0] = Math.max(max[0], value);
            } else {
                min[0] = Math.min(min[0], columnMeta.min());
                max[0] = Math.max(max[0], columnMeta.max());
            }
        });

        return new ColumnMeta(
                columnMetas.getFirst().name(),
                null,
                null,
                false,
                columnMetas.getFirst().categoryValues(),
                min[0],
                max[0],
                null,
                null,
                null,
                null
        );
    }

    /**
     * In some cases we need to flatten a group of ColumnMetaRows down to a single ColumnMeta.
     * This is the case for Categorical Concept Paths in the columnMeta.csv that have more than one row.
     *
     * @param columnMetas List of ColumnMeta that have the same concept path and are categorical
     * @return A single ColumnMeta that contains ALL the values combined into a single list.
     */
    public ColumnMeta flattenCategoricalColumnMeta(List<ColumnMeta> columnMetas) {
        Set<String> setOfVals = new HashSet<>();
        columnMetas.forEach(columnMeta -> {
            setOfVals.addAll(columnMeta.categoryValues());
        });

        List<String> values = new ArrayList<>(setOfVals);
        return new ColumnMeta(
                columnMetas.getFirst().name(),
                null,
                null,
                columnMetas.getFirst().categorical(),
                values,
                columnMetas.getFirst().min(),
                columnMetas.getFirst().max(),
                null,
                null,
                null,
                null
        );
    }

    private void printColumnMetaErrorsToCSV(String csvFilePath) {
        try (CSVWriter writer = new CSVWriter(new FileWriter(csvFilePath))) {
            this.columnMetaErrors.forEach(columnMetas ->
                    columnMetas.forEach(columnMeta ->
                            writer.writeNext(new String[]{
                                    columnMeta.name(),
                                    columnMeta.widthInBytes(),
                                    columnMeta.columnOffset(),
                                    String.valueOf(columnMeta.categorical()),
                                    String.join("µ", columnMeta.categoryValues()),
                                    columnMeta.allObservationsOffset(),
                                    columnMeta.allObservationsLength(),
                                    columnMeta.observationCount(),
                                    columnMeta.patientCount()
                            })));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
