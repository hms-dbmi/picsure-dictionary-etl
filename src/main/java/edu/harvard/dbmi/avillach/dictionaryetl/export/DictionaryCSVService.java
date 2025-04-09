package edu.harvard.dbmi.avillach.dictionaryetl.export;

import com.opencsv.*;
import com.opencsv.exceptions.CsvValidationException;
import edu.harvard.dbmi.avillach.dictionaryetl.Utility.CSVUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.Utility.ColumnMetaUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataService;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptService;
import edu.harvard.dbmi.avillach.dictionaryetl.consent.ConsentModel;
import edu.harvard.dbmi.avillach.dictionaryetl.consent.ConsentService;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.*;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetService;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.ConceptToFacetDTO;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryMeta;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
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
    public DictionaryCSVService(DatasetService datasetService,
                                DatasetMetadataService datasetMetadataService,
                                ConceptService conceptService,
                                ConceptMetadataService conceptMetadataService,
                                ColumnMetaUtility columnMetaUtility,
                                FacetService facetService,
                                FacetCategoryService facetCategoryService,
                                ConsentService consentService,
                                CSVUtility csvUtility,
                                DataSource dataSource) throws SQLException {
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
        String fullConceptPath = path + "/Concept.csv";
        String fullConceptPathDatasetCSVDir = path + "/Concept_Dataset_CSVs/";

        List<String> conceptMetadataKeys = this.conceptMetadataService.findMetadataKeysByDatasetID(datasetIDs);

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
        String fullFacetPath = path + "/Facet.csv";

        List<String> facetMetadataKeyNames = this.facetService.getFacetMetadataKeyNames();
        List<FacetModel> facetModels = this.facetService.findAllFacetsByDatasetIDs(datasetIDs);

        String[] facetCSVHeaders = getFacetCSVHeaders(facetMetadataKeyNames);
        this.csvUtility.createCSVFile(fullFacetPath, facetCSVHeaders);
        generateFacetCSV(fullFacetPath, facetCSVHeaders, facetMetadataKeyNames, facetCategoryModels, facetModels);
        // -----------------------------------------------------------------------------------------

        // Create Facet Concept List CSV
        // -----------------------------------------------------------------------------------------
        String fullFacetConceptListDatasetCSVDir = path + "/Facet_Concept_List_Dataset_CSVs/";
        String fullFacetConceptListPath = path + "/Facet_Concept_List.csv";

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
                this.fixedThreadPool.submit(() -> generateConceptsCSV(fullConceptPathDatasetCSVDir, conceptCSVHeaders, conceptMetadataKeys, dataset, conceptModels));
                this.fixedThreadPool.submit(() -> generateFacetConceptListCSVForDataset(fullFacetConceptListDatasetCSVDir, dataset, facetConceptListHeaders, conceptModels, facetNameToPosition));
            } else {
                generateConceptsCSV(fullConceptPathDatasetCSVDir, conceptCSVHeaders, conceptMetadataKeys, dataset, conceptModels);
                generateFacetConceptListCSVForDataset(fullFacetConceptListDatasetCSVDir, dataset, facetConceptListHeaders, conceptModels, facetNameToPosition);
            }
        }

        // When the datasetCounter reaches the number of datasets * number of parallel functions per iteration
        // we know that all the tasks have finished
        while (this.datasetCounter.get() < datasets.size() * numberOfParallelFunctionsPerIteration) {
            try {
                // busy wait for the tasks to finish
                Thread.sleep(5000); // 5 seconds
                log.info("Waiting for datasets to be populated, {} out of {} tasks completed", this.datasetCounter.get(), datasets.size() * numberOfParallelFunctionsPerIteration);
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

        this.removeDirectoryIfEmpty(fullConceptPathDatasetCSVDir);
        this.removeDirectoryIfEmpty(fullFacetConceptListDatasetCSVDir);

        Long endTime = System.currentTimeMillis();
        long minutes = (endTime - startTime) / 1000 / 60;
        long seconds = (endTime - startTime) / 1000 % 60;
        log.info("CSV generation took: {} minutes and {} seconds", minutes, seconds);
    }

    private void removeDirectoryIfEmpty(String fullConceptPathDatasetCSVDir) {
        File conceptCSVDir = new File(fullConceptPathDatasetCSVDir);
        if (conceptCSVDir.isDirectory()) {
            File[] files = conceptCSVDir.listFiles();
            if (files != null && files.length == 0) {
                boolean deleted = conceptCSVDir.delete();
                if (deleted) {
                    log.info("Deleted empty directory: {}", fullConceptPathDatasetCSVDir);
                } else {
                    log.warn("Failed to delete empty directory: {}", fullConceptPathDatasetCSVDir);
                }
            }
        }
    }

    private Thread startMergingFacetConceptListCSVs(String destinationFile) {
        // Create a pub-sub system to merge the CSVs because we are IO bound we will just use a single thread
        // to merge the CSVs. This will be a blocking call until all the CSVs are merged.
        Thread facetConceptListCSVMergeThread = new Thread(() -> {
            try {
                while (true) {
                    // get the value from the queue if it exists without removing it
                    // we will remove it after we are done merging
                    String csvFilePath = this.readyFacetConceptListCSVs.take();
                    if (csvFilePath.equals("STOP")) {
                        log.info("Stopping Facet-Concept-List-CSV-Merge-Thread");
                        break;
                    }
                    log.info("Merging Facet Concept List CSV file: {}", csvFilePath);
                    try {
                        mergeCSVFiles(csvFilePath, destinationFile);
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
                log.info("All facet concept list CSVs have been merged");
            }
        });

        facetConceptListCSVMergeThread.setDaemon(true);
        facetConceptListCSVMergeThread.setName("Facet-Concept-List-CSV-Merge-Thread");
        facetConceptListCSVMergeThread.start();
        return facetConceptListCSVMergeThread;
    }


    private Thread startMergingConceptCSVs(String destinationFile) {
        // Create a pub-sub system to merge the CSVs because we are IO bound we will just use a single thread
        // to merge the CSVs. This will be a blocking call until all the CSVs are merged.
        Thread conceptCSVMergeThread = new Thread(() -> {
            try {
                while (true) {
                    String csvFilePath = this.readyConceptCSVs.take();
                    if (csvFilePath.equals("STOP")) {
                        log.info("Stopping Concept-CSV-Merge-Thread");
                        break;
                    }

                    log.info("Merging Concept CSV file: {}", csvFilePath);
                    try {
                        mergeCSVFiles(csvFilePath, destinationFile);
                        // delete the CSV file after it has been merged
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
                log.info("All concept CSVs have been merged");
            }
        });

        conceptCSVMergeThread.setDaemon(true);
        conceptCSVMergeThread.setName("Concept-CSV-Merge-Thread");
        conceptCSVMergeThread.start();
        log.info("Started merging CSV files");
        return conceptCSVMergeThread;
    }

    private void mergeCSVFiles(String sourceFilePath, String destinationFilePath) {
        try (CSVReader reader = new CSVReader(new FileReader(sourceFilePath));
             CSVWriter writer = new CSVWriter(new FileWriter(destinationFilePath, true))) {
            // Skip the header row
            reader.readNext();
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                writer.writeNext(nextLine);
            }
        } catch (IOException | CsvValidationException e) {
            throw new RuntimeException(e);
        }
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
            String fullFacetConceptListDatasetCSVDir,
            DataSetRefDto dataset,
            String[] facetConceptListHeaders,
            List<ConceptModel> conceptModels,
            Map<String, Integer> facetNameToPosition) {

        String datasetCSVPath = fullFacetConceptListDatasetCSVDir + dataset.getRef() + ".csv";
        this.csvUtility.createCSVFile(datasetCSVPath, facetConceptListHeaders);
        try (CSVWriter writer = new CSVWriter(new FileWriter(datasetCSVPath, true))) {
            int batchSize = 1000;
            List<String[]> batch = new ArrayList<>(batchSize);

            // the list of facet_concept_node is lighter weight and should
            List<ConceptToFacetDTO> facetToConceptRelationshipsByDatasetID = this.facetService.findFacetToConceptRelationshipsByDatasetID(dataset.getDatasetId());

            Map<Long, List<ConceptToFacetDTO>> conceptToFacets = facetToConceptRelationshipsByDatasetID
                    .stream()
                    .filter(conceptToFacetDTO -> conceptToFacetDTO.getConceptNodeId() != null)
                    .collect(Collectors.groupingBy(ConceptToFacetDTO::getConceptNodeId));

            for (ConceptModel concept : conceptModels) {
                String[] row = new String[facetConceptListHeaders.length];
                List<ConceptToFacetDTO> conceptToFacetDTOs = conceptToFacets.get(concept.getConceptNodeId());
                if (conceptToFacetDTOs != null) {
                    for (ConceptToFacetDTO conceptToFacetDTO : conceptToFacetDTOs) {
                        row[facetNameToPosition.get(conceptToFacetDTO.getFacetName())] = concept.getConceptPath().replace("\\", "\\\\");
                    }
                }

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
                batch.clear();
            }

            this.readyFacetConceptListCSVs.add(datasetCSVPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            this.datasetCounter.getAndIncrement();
        }
    }

    private void generateFacetCSV(
            String fullFacetPath,
            String[] facetCategoriesHeaders,
            List<String> facetMetadataKeyNames,
            List<FacetCategoryModel> facetCategoryModels,
            List<FacetModel> facetModels) {
        try (CSVWriter writer = new CSVWriter(new FileWriter(fullFacetPath, true))) {
            Map<Long, String> facetCategoryIdToName = new HashMap<>();
            facetCategoryModels.forEach(facetCategoryModel -> {
                facetCategoryIdToName.put(facetCategoryModel.getFacetCategoryId(), facetCategoryModel.getName());
            });

            Map<Long, FacetModel> parentIdToFacetModel = new HashMap<>();
            // populate the parentIdToFacetModel map
            facetModels.stream()
                    .filter(parentModel -> parentModel != null && parentModel.getParentId() != null)
                    .map(facetModel -> this.facetService.findByID(facetModel.getParentId()))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(facetModel -> parentIdToFacetModel.put(facetModel.getParentId(), facetModel));


            facetModels.forEach(facet -> {
                String[] row = new String[facetCategoriesHeaders.length];
                String facetCategoryName = facetCategoryIdToName.get(facet.getFacetCategoryId());
                row[0] = facetCategoryName != null ? facetCategoryName : "";
                row[1] = facet.getName();
                row[2] = facet.getDisplay();
                row[3] = facet.getDescription();

                FacetModel parentFacet = parentIdToFacetModel.get(facet.getParentId());
                row[4] = parentFacet != null ? parentFacet.getName().replace("\\", "\\\\") : "";

                int col = 5;
                for (String key : facetMetadataKeyNames) {
                    Optional<FacetMetadataModel> facetMetaDataByKey =
                            this.facetService.findFacetMetadataByFacetIDAndKey(facet.getFacetId(), key);
                    row[col] = facetMetaDataByKey.isPresent() ? facetMetaDataByKey.get().getValue() : "";
                    col++;
                }

                writer.writeNext(row);
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private void generateFacetCategoriesCSV(String fullPath, String[] facetCategoriesHeaders, List<String> facetCategoryMetadataKeyNames, List<FacetCategoryModel> facetCategoryModels) {
        Long[] facetCategoryIDs = facetCategoryModels.stream().map(FacetCategoryModel::getFacetCategoryId).toArray(Long[]::new);
        List<FacetCategoryMeta> facetCategoryMetaData = this.facetCategoryService.findFacetCategoryMetaByFacetCategoriesID(facetCategoryIDs);
        try (CSVWriter writer = new CSVWriter(new FileWriter(fullPath, true))) {
            facetCategoryModels.forEach(facetCategory -> {
                String[] row = new String[facetCategoriesHeaders.length];
                row[0] = facetCategory.getName();
                row[1] = facetCategory.getDisplay();
                row[2] = facetCategory.getDescription();

                int col = 3;
                for (String key : facetCategoryMetadataKeyNames) {
                    FacetCategoryMeta facetCategoryMetaDataByKey =
                            facetCategoryMetaData.stream().filter(meta -> meta.getKey().equals(key)).findFirst().orElse(null);
                    if (facetCategoryMetaDataByKey != null) {
                        row[col] = facetCategoryMetaDataByKey.getValue();
                    } else {
                        row[col] = "";
                    }

                    col++;
                }

                writer.writeNext(row);
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private String[] createConsentsCsvWithHeaders(String fullPath) {
        log.info("Creating Concepts.csv");
        String[] consentCSVHeaders = new String[]{"dataset_ref", "consent_code", "description", "participant count", "variable count", "sample count", "authz"};
        this.csvUtility.createCSVFile(fullPath, consentCSVHeaders);
        log.info("Consents.csv created with initial headers");
        return consentCSVHeaders;
    }

    private String[] getFacetCategoriesHeaders(List<String> facetCategoryMetadataKeyNames) {
        List<String> facetCategoriesHeaders = new ArrayList<>(List.of("facet_category", "facet_name", "display_name", "description", "parent_name"));
        facetCategoriesHeaders.addAll(facetCategoryMetadataKeyNames);
        return facetCategoriesHeaders.toArray(new String[0]);
    }

    private String[] getFacetCSVHeaders(List<String> facetMetadataKeyNames) {
        String[] facetCSVHeaders = new String[]{"facet_category", "facet_name", "display_name", "description", "parent_name", "meta_1"};
        Collections.sort(facetMetadataKeyNames);
        facetCSVHeaders = facetMetadataKeyNames.toArray(facetCSVHeaders);
        return facetCSVHeaders;
    }

    private String[] getConceptCSVHeaders(List<String> metadataKeys) {
        List<String> headers = new ArrayList<>(List.of("dataset_ref", "concept name", "display name", "concept_type",
                "concept_path", "parent_concept_path", "values", "description"));
        Collections.sort(metadataKeys);
        headers.addAll(metadataKeys);
        return headers.toArray(new String[0]);
    }

    private String[] getDatasetCSVHeaders(List<String> datasetMetadataKeys) {
        List<String> headers = new ArrayList<>(List.of("ref", "full_name", "abbreviation", "description"));
        Collections.sort(datasetMetadataKeys);
        headers.addAll(datasetMetadataKeys);
        return headers.toArray(new String[0]);
    }

    private void generateConceptsCSV(String fullConceptPathDatasetCSVDir,
                                     String[] conceptCSVHeaders,
                                     List<String> conceptMetaDataKeys,
                                     DataSetRefDto dataSetRefDto,
                                     List<ConceptModel> conceptModels) {
        String filePath = fullConceptPathDatasetCSVDir + dataSetRefDto.getRef() + ".csv";
        this.csvUtility.createCSVFile(filePath, conceptCSVHeaders);
        Optional<DatasetModel> datasetModel = this.datasetService.findByID(dataSetRefDto.getDatasetId());
        Map<Long, ConceptModel> conceptMap = new HashMap<>();
        for (ConceptModel c : conceptModels) {
            conceptMap.put(c.getConceptNodeId(), c);
        }
        try (CSVWriter writer = new CSVWriter(new FileWriter(filePath, true))) {
            List<String[]> batch = new ArrayList<>();
            int batchSize = 1000;

            for (ConceptModel concept : conceptModels) {
                String[] row = new String[conceptCSVHeaders.length];
                List<ConceptMetadataModel> conceptsMetadata =
                        this.conceptMetadataService.findByConceptID(concept.getConceptNodeId());
                row[0] = datasetModel.isPresent() ? datasetModel.get().getRef() : "";
                row[1] = concept.getName();
                row[2] = concept.getDisplay();
                row[3] = concept.getConceptType();
                row[4] = concept.getConceptPath().replace("\\", "\\\\");

                String parentConceptPath = "";
                if (concept.getParentId() != null) {
                    ConceptModel parent = conceptMap.get(concept.getParentId());
                    if (parent != null) {
                        parentConceptPath = parent.getConceptPath();
                    }
                }
                row[5] = parentConceptPath.replace("\\", "\\\\");

                Optional<ConceptMetadataModel> values = Optional.empty();
                for (ConceptMetadataModel metadataModel : conceptsMetadata) {
                    if (metadataModel.getKey().equals("values")) {
                        values = Optional.of(metadataModel);
                        // remove it from the list so we don't add it to metadata section.
                        // The values need to be parsed differently.
                        conceptsMetadata.remove(metadataModel);
                        break;
                    }
                }
                String conceptType = concept.getConceptType();
                String parsedValues = convertConceptValuesToDelimitedString(values, conceptType);
                row[6] = parsedValues;

                int col = 7;
                for (String key : conceptMetaDataKeys) {
                    Optional<ConceptMetadataModel> conceptMetaDataByKey =
                            conceptsMetadata.stream().filter(meta -> meta.getKey().equals(key)).findFirst();
                    row[col] = conceptMetaDataByKey.isPresent() ? conceptMetaDataByKey.get().getValue() : "";
                    col++;
                }

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
                batch.clear();
            }
            this.readyConceptCSVs.add(filePath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            this.datasetCounter.getAndIncrement();
        }
    }

    private String convertConceptValuesToDelimitedString(Optional<ConceptMetadataModel> valuesMetaData, String conceptType) {
        if (valuesMetaData.isEmpty()) {
            return "";
        }
        ConceptMetadataModel conceptMetadataModel = valuesMetaData.get();
        String data = conceptMetadataModel.getValue();
        String values;
        if (conceptType.equalsIgnoreCase("categorical")) {
            List<String> strings = this.columnMetaUtility.parseValues(data);
            values = String.join("µ", strings);
        } else {
            Float max = this.columnMetaUtility.parseMax(data);
            Float min = this.columnMetaUtility.parseMin(data);
            values = min + "µ" + max;
        }

        return values;
    }

    private void generateConsentsCSV(String fullPath, DataSetRefDto dataSetRefDto, String[] consentCSVHeaders) {
        try (CSVWriter writer = new CSVWriter(new FileWriter(fullPath, true))) {
            List<ConsentModel> consents = this.consentService.findByDatasetID(dataSetRefDto.getDatasetId());
            consents.forEach(consent -> {
                String[] row = new String[consentCSVHeaders.length];
                row[0] = dataSetRefDto.getRef();
                row[1] = consent.getConsentCode();
                row[2] = consent.getDescription();
                row[3] = consent.getParticipantCount().toString();
                row[4] = consent.getVariableCount().toString();
                row[5] = consent.getSampleCount().toString();
                row[6] = consent.getAuthz();

                writer.writeNext(row);
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void generateDatasetsCSV(String fullPath, List<String> metadataKeys, List<DataSetRefDto> dataSetRefDtos) {
        log.info("Creating Datasets.csv");
        String[] datasetCSVHeaders = getDatasetCSVHeaders(metadataKeys);
        this.csvUtility.createCSVFile(fullPath, datasetCSVHeaders);

        try (CSVWriter writer = new CSVWriter(new FileWriter(fullPath, true))) {
            dataSetRefDtos.forEach(dataSetRefDto -> {
                Optional<DatasetModel> datasetByRef = this.datasetService.findByRef(dataSetRefDto.getRef());
                if (datasetByRef.isPresent()) {
                    DatasetModel dataset = datasetByRef.get();
                    List<DatasetMetadataModel> metadata = this.datasetMetadataService.findByDatasetID(dataSetRefDto.getDatasetId());
                    String[] row = new String[datasetCSVHeaders.length];
                    row[0] = dataset.getRef();
                    row[1] = dataset.getFullName();
                    row[2] = dataset.getAbbreviation();
                    row[3] = dataset.getDescription();
                    final int[] col = {4};
                    // Metadata keys is sorted so we should always end up with the keys as columns in sorted order.
                    metadataKeys.forEach(key -> {
                        Optional<DatasetMetadataModel> datasetMetaDataByKey =
                                metadata.stream().filter(meta -> meta.getKey().equals(key)).findFirst();
                        row[col[0]] = datasetMetaDataByKey.isPresent() ? datasetMetaDataByKey.get().getValue() : "";
                        col[0]++;
                    });

                    writer.writeNext(row);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
