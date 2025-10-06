package edu.harvard.dbmi.avillach.dictionaryetl.export;

import com.opencsv.*;
import edu.harvard.dbmi.avillach.dictionaryetl.Utility.*;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.*;
import edu.harvard.dbmi.avillach.dictionaryetl.consent.*;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.*;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.*;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.*;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class DictionaryCSVService {

    private static final Logger log = LoggerFactory.getLogger(DictionaryCSVService.class);
    private final DatasetService datasetService;
    private final DatasetMetadataService datasetMetadataService;
    private final ConceptService conceptService;
    private final ConceptMetadataService conceptMetadataService;
    private final ColumnMetaUtility columnMetaUtility;
    private final FacetService facetService;
    private final FacetCategoryService facetCategoryService;
    private final ConsentService consentService;

    private final CSVUtility csvUtility;

    private final ExecutorService fixedThreadPool;
    private final int maxConnections;
    private final LinkedBlockingQueue<String> readyConceptCSVs = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<String> readyFacetConceptListCSVs = new LinkedBlockingQueue<>();
    private final AtomicInteger datasetCounter = new AtomicInteger(0);

    @Autowired
    public DictionaryCSVService(
            DatasetService datasetService, DatasetMetadataService datasetMetadataService, ConceptService conceptService,
            ConceptMetadataService conceptMetadataService, ColumnMetaUtility columnMetaUtility, FacetService facetService,
            FacetCategoryService facetCategoryService, ConsentService consentService, CSVUtility csvUtility, DataSource dataSource
    ) throws SQLException {
        this.datasetService = datasetService;
        this.datasetMetadataService = datasetMetadataService;
        this.conceptService = conceptService;
        this.conceptMetadataService = conceptMetadataService;
        this.columnMetaUtility = columnMetaUtility;
        this.facetService = facetService;
        this.facetCategoryService = facetCategoryService;
        this.consentService = consentService;
        this.csvUtility = csvUtility;

        this.maxConnections = dataSource.getConnection().getMetaData().getMaxConnections() - 2;
        this.fixedThreadPool = Executors.newFixedThreadPool(maxConnections);
    }

    public void generateFullIngestCSVs(String path, String... datasetRefs) {
        Long startTime = System.currentTimeMillis();
        List<DataSetRefDto> datasets;
        if (datasetRefs.length == 0) {
            datasets = this.datasetService.getAllDatasetRefsSorted();
        } else {
            datasets = this.datasetService.getDatasetRefsSorted(datasetRefs);
        }

        Long[] datasetIDs = datasets.stream().map(DataSetRefDto::getDatasetId).toArray(Long[]::new);

        // Create and populate dataset CSV
        // -----------------------------------------------------------------------------------------
        List<String> datasetMetadataKeys = this.datasetMetadataService.getAllKeyNames(datasetIDs);
        generateDatasetsCSV(path + "/Datasets.csv", datasetMetadataKeys, datasets);
        // -----------------------------------------------------------------------------------------

        // Create consent CSV
        // -----------------------------------------------------------------------------------------
        String fullConsentPath = path + "/Consents.csv";
        String[] consentsCsvWithHeaders = createConsentsCsvWithHeaders(fullConsentPath);
        // -----------------------------------------------------------------------------------------

        // Create Concept CSV
        // -----------------------------------------------------------------------------------------
        String fullConceptPath = path + "/Concepts.csv";
        String fullConceptPathDatasetCSVDir = path + "/Concept_Dataset_CSVs/";

        List<String> conceptMetadataKeys = this.conceptMetadataService.findMetadataKeysByDatasetID(datasetIDs);
        conceptMetadataKeys.remove("values");

        String[] conceptCSVHeaders = getConceptCSVHeaders(conceptMetadataKeys);
        this.csvUtility.createCSVFile(fullConceptPath, conceptCSVHeaders);
        // -----------------------------------------------------------------------------------------

        // Create and Populate Facet Categories CSV
        // -----------------------------------------------------------------------------------------
        String fullFacetCategoryPath = path + "/Facet_Categories.csv";

        List<String> facetCategoryMetadataKeyNames = this.facetCategoryService.getFacetCategoryMetadataKeyNames();
        String[] facetCategoriesHeaders = getFacetCategoriesHeaders(facetCategoryMetadataKeyNames);
        List<FacetCategoryModel> facetCategoryModels = this.facetCategoryService.findAll();

        this.csvUtility.createCSVFile(fullFacetCategoryPath, facetCategoriesHeaders);
        generateFacetCategoriesCSV(fullFacetCategoryPath, facetCategoriesHeaders, facetCategoryMetadataKeyNames, facetCategoryModels);
        // -----------------------------------------------------------------------------------------

        // Create and Populate Facet CSV
        // -----------------------------------------------------------------------------------------
        String fullFacetPath = path + "/Facets.csv";

        List<String> facetMetadataKeyNames = this.facetService.getFacetMetadataKeyNames();
        List<FacetModel> facetModels = this.facetService.findAll();

        String[] facetCSVHeaders = getFacetCSVHeaders(facetMetadataKeyNames);
        this.csvUtility.createCSVFile(fullFacetPath, facetCSVHeaders);
        generateFacetCSV(fullFacetPath, facetCSVHeaders, facetMetadataKeyNames, facetCategoryModels, facetModels);
        // -----------------------------------------------------------------------------------------

        // Create Facet Concept List CSV
        // -----------------------------------------------------------------------------------------
        String fullFacetConceptListDatasetCSVDir = path + "/Facet_Concept_List_Dataset_CSVs/";
        String fullFacetConceptListPath = path + "/Facet_Concept_Lists.csv";

        String[] facetConceptListHeaders = facetModels.stream().map(FacetModel::getName).toArray(String[]::new);
        this.csvUtility.createCSVFile(fullFacetConceptListPath, facetConceptListHeaders);
        // ------------------------------------------------------------------------------------------

        Map<String, Integer> facetNameToPosition = new HashMap<>();
        for (int i = 0; i < facetModels.size(); i++) {
            facetNameToPosition.put(facetModels.get(i).getName(), i);
        }

        Thread mergeConceptCSVsThread = this.startMergingConceptCSVs(fullConceptPath);
        Thread mergeFacetConceptListCSVThread = this.startMergingFacetConceptListCSVs(fullFacetConceptListPath);
        int numberOfParallelFunctionsPerIteration = 2;
        for (DataSetRefDto dataset : datasets) {
            log.info("Populating CSVs for dataset: {}", dataset.getRef());
            generateConsentsCSV(fullConsentPath, dataset, consentsCsvWithHeaders);
            List<ConceptModel> conceptModels = this.conceptService.findByDatasetID(dataset.getDatasetId());
            if (maxConnections > 5) {
                this.fixedThreadPool.submit(
                        () -> generateConceptsCSV(
                                fullConceptPathDatasetCSVDir, conceptCSVHeaders, conceptMetadataKeys, dataset,
                                conceptModels
                        ));
                this.fixedThreadPool.submit(
                        () -> generateFacetConceptListCSVForDataset(
                                fullFacetConceptListDatasetCSVDir, dataset, facetConceptListHeaders,
                                conceptModels, facetNameToPosition
                        ));
            } else {
                generateConceptsCSV(fullConceptPathDatasetCSVDir, conceptCSVHeaders, conceptMetadataKeys, dataset, conceptModels);
                generateFacetConceptListCSVForDataset(
                        fullFacetConceptListDatasetCSVDir, dataset, facetConceptListHeaders, conceptModels, facetNameToPosition);
            }
        }

        // When the datasetCounter reaches the number of datasets * number of parallel functions per iteration
        // we know that all the tasks have finished
        while (this.datasetCounter.get() < datasets.size() * numberOfParallelFunctionsPerIteration) {
            try {
                // busy wait for the tasks to finish
                Thread.sleep(5000); // 5 seconds
                log.info(
                        "Waiting for datasets to be populated, {} out of {} tasks completed", this.datasetCounter.get(),
                        datasets.size() * numberOfParallelFunctionsPerIteration
                );
            } catch (InterruptedException e) {
                log.error("Error waiting for tasks to finish", e);
                Thread.currentThread().interrupt();
            }
        }

        // Add final stop signal to the queues
        this.readyConceptCSVs.add("STOP");
        this.readyFacetConceptListCSVs.add("STOP");

        try {
            // wait for the threads to finish
            mergeConceptCSVsThread.join();
            mergeFacetConceptListCSVThread.join();
        } catch (InterruptedException e) {
            log.error("Error waiting for CSV merge threads to finish", e);
            Thread.currentThread().interrupt();
        }

        csvUtility.removeDirectoryIfEmpty(fullConceptPathDatasetCSVDir);
        csvUtility.removeDirectoryIfEmpty(fullFacetConceptListDatasetCSVDir);

        Long endTime = System.currentTimeMillis();
        long minutes = (endTime - startTime) / 1000 / 60;
        long seconds = (endTime - startTime) / 1000 % 60;
        log.info("CSV generation took: {} minutes and {} seconds", minutes, seconds);
    }

    /**
     * Creates a thread that merges CSV files from a queue into a destination file
     *
     * @param destinationFile Destination file path
     * @param queue           Queue of CSV files to merge
     * @param threadName      Name of the thread
     * @return Thread that merges CSV files
     */
    private Thread startMergingCSVs(String destinationFile, LinkedBlockingQueue<String> queue, String threadName) {
        Thread mergeThread = new Thread(() -> {
            try {
                while (true) {
                    String csvFilePath = queue.take();
                    if (csvFilePath.equals("STOP")) {
                        log.info("Stopping {}", threadName);
                        break;
                    }

                    log.info("Merging CSV file: {}", csvFilePath);
                    try {
                        csvUtility.mergeCSVFiles(csvFilePath, destinationFile);
                        File csvFile = new File(csvFilePath);
                        if (csvFile.delete()) {
                            log.info("Deleted CSV file: {}", csvFilePath);
                        } else {
                            log.warn("Failed to delete CSV file: {}", csvFilePath);
                        }
                    } catch (Exception e) {
                        log.error("Error merging CSV files", e);
                    }
                }
            } catch (InterruptedException e) {
                log.error("Error merging CSV files", e);
                Thread.currentThread().interrupt();
            } finally {
                log.info("All {} have been merged", threadName.toLowerCase());
            }
        });

        mergeThread.setDaemon(true);
        mergeThread.setName(threadName);
        mergeThread.start();
        log.info("Started {} thread", threadName);
        return mergeThread;
    }

    private Thread startMergingFacetConceptListCSVs(String destinationFile) {
        return startMergingCSVs(destinationFile, this.readyFacetConceptListCSVs, "Facet-Concept-List-CSV-Merge-Thread");
    }

    private Thread startMergingConceptCSVs(String destinationFile) {
        return startMergingCSVs(destinationFile, this.readyConceptCSVs, "Concept-CSV-Merge-Thread");
    }

    /**
     * Creates a copy of the Facet_Concept_List.csv file for a provided dataset.
     *
     * @param fullFacetConceptListDatasetCSVDir The directory the individual dataset CSVs will be stored in.
     * @param dataset                           DataSetRefDto for the dataset to create the CSV for.
     * @param facetConceptListHeaders           The headers for the Facet_Concept_List.csv file.
     * @param conceptModels                     The list of ConceptModels for the dataset.
     * @param facetNameToPosition               A map of facet name to position in the header.
     */
    private void generateFacetConceptListCSVForDataset(
            String fullFacetConceptListDatasetCSVDir, DataSetRefDto dataset, String[] facetConceptListHeaders, List<ConceptModel> conceptModels,
            Map<String, Integer> facetNameToPosition
    ) {

        String datasetCSVPath = fullFacetConceptListDatasetCSVDir + dataset.getRef() + ".csv";
        this.csvUtility.createCSVFile(datasetCSVPath, facetConceptListHeaders);

        try {
            // Get the facet-concept relationships for this dataset
            List<ConceptToFacetDTO> facetToConceptRelationships =
                    this.facetService.findFacetToConceptRelationshipsByDatasetID(dataset.getDatasetId());


            // Group relationships by concept node ID for efficient lookup
            Map<Long, List<ConceptToFacetDTO>> conceptToFacets =
                    facetToConceptRelationships.stream().filter(dto -> dto.getConceptNodeId() != null)
                            .collect(Collectors.groupingBy(ConceptToFacetDTO::getConceptNodeId));

            // Create a row mapper function for the CSV utility
            Function<ConceptModel, String[]> rowMapper = concept -> {
                String[] row = new String[facetConceptListHeaders.length];
                List<ConceptToFacetDTO> conceptToFacetDTOs = conceptToFacets.get(concept.getConceptNodeId());

                if (conceptToFacetDTOs != null) {
                    for (ConceptToFacetDTO dto : conceptToFacetDTOs) {
                        row[facetNameToPosition.get(dto.getFacetName())] = concept.getConceptPath().replace("\\", "\\\\");
                    }
                }

                return row;
            };

            // Write the data to the CSV file
            this.csvUtility.writeDataToCSV(datasetCSVPath, conceptModels, rowMapper);

            // Add the file to the queue for merging
            this.readyFacetConceptListCSVs.add(datasetCSVPath);
        } finally {
            this.datasetCounter.getAndIncrement();
        }
    }

    /**
     * Generates a CSV file containing facet data
     *
     * @param fullFacetPath         Path to the output CSV file
     * @param facetHeaders          Headers for the CSV file
     * @param facetMetadataKeyNames List of metadata key names
     * @param facetCategoryModels   List of facet category models
     * @param facetModels           List of facet models
     */
    private void generateFacetCSV(
            String fullFacetPath, String[] facetHeaders, List<String> facetMetadataKeyNames,
            List<FacetCategoryModel> facetCategoryModels, List<FacetModel> facetModels
    ) {

        // Create mapping from facet category ID to name
        Map<Long, String> facetCategoryIdToName = new HashMap<>();
        facetCategoryModels.forEach(
                facetCategoryModel -> facetCategoryIdToName.put(facetCategoryModel.getFacetCategoryId(), facetCategoryModel.getName()));


        // Create row mapper function
        Function<FacetModel, String[]> rowMapper = facet -> {
            String[] row = new String[facetHeaders.length];

            // Set basic facet information
            String facetCategoryName = facetCategoryIdToName.get(facet.getFacetCategoryId());
            row[0] = facetCategoryName;
            row[1] = facet.getName();
            row[2] = facet.getDisplay();
            row[3] = facet.getDescription();

            // Set parent facet information
            FacetModel parentFacet =
                    (facet.getParentId() != null) && (facetService.findByID(facet.getParentId()).isPresent()) ? facetService.findByID(
                            facet.getParentId()).get() : null;
            row[4] = parentFacet != null ? parentFacet.getName() : "";

            // Set metadata values
            int col = 5;
            for (String key : facetMetadataKeyNames) {
                Optional<FacetMetadataModel> facetMetaDataByKey =
                        this.facetService.findFacetMetadataByFacetIDAndKey(facet.getFacetId(), key);
                row[col] = facetMetaDataByKey.isPresent() ? facetMetaDataByKey.get().getValue() : "";
                col++;
            }

            return row;
        };

        // Write data to CSV file
        this.csvUtility.writeDataToCSV(fullFacetPath, facetModels, rowMapper);
    }

    /**
     * Generates a CSV file containing facet category data
     *
     * @param fullPath                      Path to the output CSV file
     * @param facetCategoriesHeaders        Headers for the CSV file
     * @param facetCategoryMetadataKeyNames List of metadata key names
     * @param facetCategoryModels           List of facet category models
     */
    private void generateFacetCategoriesCSV(
            String fullPath, String[] facetCategoriesHeaders, List<String> facetCategoryMetadataKeyNames,
            List<FacetCategoryModel> facetCategoryModels
    ) {

        // Get all metadata for the facet categories
        Long[] facetCategoryIDs = facetCategoryModels.stream().map(FacetCategoryModel::getFacetCategoryId).toArray(Long[]::new);
        List<FacetCategoryMeta> facetCategoryMetaData =
                this.facetCategoryService.findFacetCategoryMetaByFacetCategoriesID(facetCategoryIDs);

        // Create row mapper function
        Function<FacetCategoryModel, String[]> rowMapper = facetCategory -> {
            String[] row = new String[facetCategoriesHeaders.length];

            // Set basic facet category information
            row[0] = facetCategory.getName();
            row[1] = facetCategory.getDisplay();
            row[2] = facetCategory.getDescription();

            // Set metadata values
            int col = 3;
            for (String key : facetCategoryMetadataKeyNames) {
                FacetCategoryMeta metaData =
                        facetCategoryMetaData.stream().filter(meta -> meta.getKey().equals(key)).findFirst().orElse(null);

                row[col] = (metaData != null) ? metaData.getValue() : "";
                col++;
            }

            return row;
        };

        // Write data to CSV file
        this.csvUtility.writeDataToCSV(fullPath, facetCategoryModels, rowMapper);
    }

    /**
     * Creates a CSV file for consents with headers
     *
     * @param fullPath Path to the output CSV file
     * @return Array of headers
     */
    private String[] createConsentsCsvWithHeaders(String fullPath) {
        log.info("Creating Consents.csv");
        String[] consentCSVHeaders =
                new String[]{"dataset_ref", "consent_code", "description", "participant count", "variable count", "sample count", "authz"};
        this.csvUtility.createCSVFile(fullPath, consentCSVHeaders);
        log.info("Consents.csv created with initial headers");
        return consentCSVHeaders;
    }

    /**
     * Gets headers for facet categories CSV
     *
     * @param facetCategoryMetadataKeyNames List of metadata key names
     * @return Array of headers
     */
    private String[] getFacetCategoriesHeaders(List<String> facetCategoryMetadataKeyNames) {
        List<String> facetCategoriesHeaders =
                new ArrayList<>(List.of("name(unique)", "display name", "description"));
        facetCategoriesHeaders.addAll(facetCategoryMetadataKeyNames);
        return facetCategoriesHeaders.toArray(new String[0]);
    }

    /**
     * Gets headers for facet CSV
     *
     * @param facetMetadataKeyNames List of metadata key names
     * @return Array of headers
     */
    private String[] getFacetCSVHeaders(List<String> facetMetadataKeyNames) {
        ArrayList<String> facetCSVHeaders =
                new ArrayList<>(List.of(new String[]{"facet_category", "facet_name(unique)", "display_name", "description", "parent_name"}));
        Collections.sort(facetMetadataKeyNames);
        facetCSVHeaders.addAll(facetMetadataKeyNames);
        return facetCSVHeaders.toArray(String[]::new);
    }

    /**
     * Gets headers for concept CSV
     *
     * @param metadataKeys List of metadata keys
     * @return Array of headers
     */
    private String[] getConceptCSVHeaders(List<String> metadataKeys) {
        List<String> headers =
                new ArrayList<>(List.of("dataset_ref", "name", "display", "concept_type", "concept_path", "parent_concept_path", "values"));
        Collections.sort(metadataKeys);
        headers.addAll(metadataKeys);
        return headers.toArray(new String[0]);
    }

    /**
     * Gets headers for dataset CSV
     *
     * @param datasetMetadataKeys List of metadata keys
     * @return Array of headers
     */
    private String[] getDatasetCSVHeaders(List<String> datasetMetadataKeys) {
        List<String> headers = new ArrayList<>(List.of("ref", "full_name", "abbreviation", "description"));
        Collections.sort(datasetMetadataKeys);
        headers.addAll(datasetMetadataKeys);
        return headers.toArray(new String[0]);
    }

    /**
     * Generates a CSV file containing concept data for a dataset
     *
     * @param fullConceptPathDatasetCSVDir Directory for dataset-specific concept CSV files
     * @param conceptCSVHeaders            Headers for the CSV file
     * @param conceptMetaDataKeys          List of metadata keys
     * @param dataSetRefDto                Dataset reference
     * @param conceptModels                List of concept models
     */
    private void generateConceptsCSV(
            String fullConceptPathDatasetCSVDir, String[] conceptCSVHeaders, List<String> conceptMetaDataKeys,
            DataSetRefDto dataSetRefDto, List<ConceptModel> conceptModels
    ) {

        String filePath = fullConceptPathDatasetCSVDir + dataSetRefDto.getRef() + ".csv";
        this.csvUtility.createCSVFile(filePath, conceptCSVHeaders);

        try {
            // Get dataset information
            Optional<DatasetModel> datasetModel = this.datasetService.findByID(dataSetRefDto.getDatasetId());
            String datasetRef = datasetModel.isPresent() ? datasetModel.get().getRef() : "";

            // Create a map of concept IDs to concept models for efficient parent lookup
            Map<Long, ConceptModel> conceptMap =
                    conceptModels.stream().collect(Collectors.toMap(ConceptModel::getConceptNodeId, concept -> concept));

            // Create row mapper function
            Function<ConceptModel, String[]> rowMapper = concept -> {
                String[] row = new String[conceptCSVHeaders.length];

                // Get concept metadata
                List<ConceptMetadataModel> conceptsMetadata =
                        new ArrayList<>(this.conceptMetadataService.findByConceptID(concept.getConceptNodeId()));

                // Set basic concept information
                row[0] = datasetRef;
                row[1] = concept.getName();
                row[2] = concept.getDisplay();
                row[3] = concept.getConceptType();
                row[4] = concept.getConceptPath().replace("\\", "\\\\");

                // Set parent concept path
                String parentConceptPath = "";
                if (concept.getParentId() != null) {
                    ConceptModel parent = conceptMap.get(concept.getParentId());
                    if (parent != null) {
                        parentConceptPath = parent.getConceptPath();
                    }
                }
                row[5] = parentConceptPath.replace("\\", "\\\\");

                // Extract and process values metadata
                Optional<ConceptMetadataModel> values = Optional.empty();
                for (int i = 0; i < conceptsMetadata.size(); i++) {
                    ConceptMetadataModel metadataModel = conceptsMetadata.get(i);
                    if (metadataModel.getKey().equals("values")) {
                        values = Optional.of(metadataModel);
                        // Remove it from the list so we don't add it to metadata section
                        conceptsMetadata.remove(i);
                        break;
                    }
                }

                // Parse and set values
                String conceptType = concept.getConceptType();
                row[6] = convertConceptValuesToJsonString(values);

                // Set metadata values
                int col = 7;
                for (String key : conceptMetaDataKeys) {
                    Optional<ConceptMetadataModel> metaOpt =
                            conceptsMetadata.stream().filter(meta -> meta.getKey().equals(key)).findFirst();
                    row[col] = metaOpt.isPresent() ? metaOpt.get().getValue() : "";
                    col++;
                }

                return row;
            };

            // Write data to CSV file in batches
            this.csvUtility.writeDataToCSV(filePath, conceptModels, rowMapper);

            // Add the file to the queue for merging
            this.readyConceptCSVs.add(filePath);
        } finally {
            this.datasetCounter.getAndIncrement();
        }
    }

    private String convertConceptValuesToJsonString(Optional<ConceptMetadataModel> valuesMetaData) {
        if (valuesMetaData.isEmpty()) {
            return "";
        }
        ConceptMetadataModel conceptMetadataModel = valuesMetaData.get();
        String data = conceptMetadataModel.getValue();
        JSONArray valueArray = new JSONArray(data);
        return valueArray.toString();
    }

    /**
     * Generates a CSV file containing consent data for a dataset
     *
     * @param fullPath          Path to the output CSV file
     * @param dataSetRefDto     Dataset reference
     * @param consentCSVHeaders Headers for the CSV file
     */
    private void generateConsentsCSV(String fullPath, DataSetRefDto dataSetRefDto, String[] consentCSVHeaders) {
        List<ConsentModel> consents = this.consentService.findByDatasetID(dataSetRefDto.getDatasetId());

        // Create row mapper function
        Function<ConsentModel, String[]> rowMapper = consent -> {
            String[] row = new String[consentCSVHeaders.length];
            row[0] = dataSetRefDto.getRef();
            row[1] = consent.getConsentCode();
            row[2] = consent.getDescription();
            row[3] = consent.getParticipantCount().toString();
            row[4] = consent.getVariableCount().toString();
            row[5] = consent.getSampleCount().toString();
            row[6] = consent.getAuthz();
            return row;
        };

        // Write data to CSV file
        this.csvUtility.writeDataToCSV(fullPath, consents, rowMapper);
    }

    /**
     * Generates a CSV file containing dataset data
     *
     * @param fullPath       Path to the output CSV file
     * @param metadataKeys   List of metadata keys
     * @param dataSetRefDtos List of dataset references
     */
    public void generateDatasetsCSV(String fullPath, List<String> metadataKeys, List<DataSetRefDto> dataSetRefDtos) {
        log.info("Creating Datasets.csv");
        String[] datasetCSVHeaders = getDatasetCSVHeaders(metadataKeys);
        this.csvUtility.createCSVFile(fullPath, datasetCSVHeaders);

        try (CSVWriter writer = new CSVWriter(new FileWriter(fullPath, true))) {
            for (DataSetRefDto refDto : dataSetRefDtos) {
                Optional<DatasetModel> datasetOpt = this.datasetService.findByRef(refDto.getRef());
                if (datasetOpt.isPresent()) {
                    DatasetModel dataset = datasetOpt.get();
                    List<DatasetMetadataModel> metadata = this.datasetMetadataService.findByDatasetID(refDto.getDatasetId());

                    String[] row = new String[datasetCSVHeaders.length];

                    // Set basic dataset information
                    row[0] = dataset.getRef();
                    row[1] = dataset.getFullName();
                    row[2] = dataset.getAbbreviation();
                    row[3] = dataset.getDescription();

                    // Set metadata values
                    int col = 4;
                    for (String key : metadataKeys) {
                        Optional<DatasetMetadataModel> metaOpt = metadata.stream().filter(meta -> meta.getKey().equals(key)).findFirst();
                        row[col] = metaOpt.isPresent() ? metaOpt.get().getValue() : "";
                        col++;
                    }

                    writer.writeNext(row);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error writing datasets to CSV file: " + fullPath, e);
        }
    }

}
