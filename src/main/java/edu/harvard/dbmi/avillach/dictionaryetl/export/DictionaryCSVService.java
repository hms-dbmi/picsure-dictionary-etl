package edu.harvard.dbmi.avillach.dictionaryetl.export;

import com.opencsv.CSVWriter;
import edu.harvard.dbmi.avillach.dictionaryetl.Utility.CSVUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.Utility.ColumnMetaUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataService;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptService;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.*;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetService;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

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

    private final CSVUtility csvUtility;

    @Autowired
    public DictionaryCSVService(DatasetService datasetService, DatasetMetadataService datasetMetadataService, ConceptService conceptService, ConceptMetadataService conceptMetadataService, ColumnMetaUtility columnMetaUtility, FacetService facetService, FacetCategoryService facetCategoryService, CSVUtility csvUtility) {
        this.datasetService = datasetService;
        this.datasetMetadataService = datasetMetadataService;
        this.conceptService = conceptService;
        this.conceptMetadataService = conceptMetadataService;
        this.columnMetaUtility = columnMetaUtility;
        this.facetService = facetService;
        this.facetCategoryService = facetCategoryService;
        this.csvUtility = csvUtility;
    }

    public void generateFullIngestCSVs(String path) {
        File file = new File(path);
        if (!file.exists()) {
            boolean mkdir = file.mkdir();
            if (!mkdir) {
                throw new RuntimeException("Unable to create necessary directory. Directory: " + path + ". Please " +
                                           "create directory.");
            }
        }

        /*
        TODO: Outline for efficient database export to ideal ingest format.

         1. Load a set of the dataset_refs in order.
         2. Iterate over this list filling five FIFO queues each allowing 10 records at time. The primary thread should
         wait until there is space available in a queue to put more data into it. Data will be loaded in an order.
         3. 6 consumers are watching the FIFO queues and are writing data out to each file in order.

         */
        log.info("Creating Datasets.csv");
        List<String> datasetMetadataKeys = this.datasetMetadataService.getAllKeyNames();
        String[] datasetCSVHeaders = getDatasetCSVHeaders(datasetMetadataKeys);
        this.csvUtility.createCSVFile(path + "Datasets.csv", datasetCSVHeaders);
        log.info("Dataset.csv created with initial headers");

        log.info("Creating Concepts.csv");
        String[] consentCSVHeaders = new String[]{"dataset_ref", "consent_code", "description", "participant count", "variable count", "sample count", "authz"};
        this.csvUtility.createCSVFile(path + "Consents.csv", consentCSVHeaders);
        log.info("Consents.csv created with initial headers");

        log.info("Creating Concept.csv");
        List<String> metadataKeys = this.conceptMetadataService.allMetadataKeys();
        String[] conceptCSVHeaders = getConceptCSVHeaders(metadataKeys);
        this.csvUtility.createCSVFile(path + "Concept.csv", conceptCSVHeaders);
       log.info("Concept.csv created with initial headers");

        log.info("Creating Facet.csv");
        List<String> facetMetadataKeyNames = this.facetService.getFacetMetadataKeyNames();
        String[] facetCSVHeaders = getFacetCSVHeaders(facetMetadataKeyNames);
        this.csvUtility.createCSVFile(path + "Facet.csv", facetCSVHeaders);
        log.info("Facet.csv created with initial headers");

        log.info("Creating Facet_Categories.csv");
        List<String> facetCategoryMetadataKeyNames = this.facetCategoryService.getFacetCategoryMetadataKeyNames();
        String[] facetCategoriesHeaders = getFacetCategoriesHeaders(facetCategoryMetadataKeyNames);
        this.csvUtility.createCSVFile(path + "Facet_Categories.csv", facetCategoriesHeaders);
        log.info("Facet_Categories.csv created with initial headers");

        // load all datasets refs as a sorted list of strings that will be iterated over to load data in order.
        List<DataSetRefDto> datasetRefs = this.datasetService.getAllDatasetRefsSorted();
        List<Long> datasetIds = datasetRefs.stream().map(DataSetRefDto::dataset_id).toList();
        List<String> facetName = this.facetService.getFacetNames();

        log.info("Creating Facet_Concept_List.csv");
        String[] facetConceptListHeaders = getFacetConceptListHeaders(datasetMetadataKeys, facetName);
        this.csvUtility.createCSVFile(path + "Facet_Concept_List.csv", facetConceptListHeaders);
        log.info("Facet_Concept_List.csv created with initial headers");

    }

    private String[] getFacetCategoriesHeaders(List<String> facetCategoryMetadataKeyNames) {
        // name(unique)	display name	description	meta_key_1	meta_key_2
        List<String> facetCategoriesHeaders = new ArrayList<>(List.of("name", "display name", "description"));
        facetCategoriesHeaders.addAll(facetCategoryMetadataKeyNames);
        return facetCategoriesHeaders.toArray(new String[0]);
    }

    private String[] getFacetConceptListHeaders(List<String> datasetMetadataKeys, List<String> facetName) {
        //dataset_id_1	dataset_id_2	dataset_id_3	facet_name1	facet_name2	facet_name3

        return null;
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

    private void generateConceptsCSV(String path, List<String> headers) {
        String csvName = "Concept.csv";
        try (CSVWriter writer = new CSVWriter(new FileWriter(path + csvName))) {
            List<ConceptModel> conceptModels = this.conceptService.findAll();
            conceptModels.forEach(concept -> {
                String[] row = new String[headers.size()];
                Optional<DatasetModel> datasetModel = this.datasetService.findByID(concept.getDatasetId());
                List<ConceptMetadataModel> conceptMetadatas =
                        this.conceptMetadataService.findByConceptID(concept.getConceptNodeId());
                row[0] = datasetModel.isPresent() ? datasetModel.get().getRef() : "";
                row[1] = concept.getName();
                row[2] = concept.getDisplay();
                row[3] = concept.getConceptType();
                row[4] = concept.getConceptPath();

                String parentConceptPath = "";
                if (concept.getParentId() != null) {
                    ConceptModel parent = conceptModels.stream().filter(parentConcept -> parentConcept.getConceptNodeId().equals(concept.getParentId())).findFirst().get();
                    parentConceptPath = parent.getConceptPath();
                }
                row[5] = parentConceptPath;
                row[6] = convertConceptValuesToDelimitedString(conceptMetadatas.stream().filter(conceptMetadata -> conceptMetadata.getKey().equals("values")).findFirst(), concept.getConceptType());

                Optional<ConceptMetadataModel> description = conceptMetadatas.stream().filter(conceptMetadata -> conceptMetadata.getKey().equals("values")).findFirst();
                row[7] = description.isPresent() ? description.get().getValue() : "";

                // TODO: Metadata
                writer.writeNext(row);
            });

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String convertConceptValuesToDelimitedString(Optional<ConceptMetadataModel> valuesMetaData, String conceptType) {
        if (valuesMetaData.isEmpty()) {
            return "";
        }

        ConceptMetadataModel conceptMetadataModel = valuesMetaData.get();
        String data = conceptMetadataModel.getValue();
        String values = "";
        if (conceptType.equals("categorical")) {
            List<String> strings = this.columnMetaUtility.parseValues(data);
            values = String.join("µ", strings);
        } else {
            Float max = this.columnMetaUtility.parseMax(data);
            Float min = this.columnMetaUtility.parseMin(data);
            values = min + "µ" + max;
        }

        return values;
    }

    private void generateConsentsCSV(String path) {
        String csvName = "Dataset.csv";
        List<String> headers = new ArrayList<>(List.of("dataset_ref", "consent_code", "description", "participant count", "variable count", "sample count", "authz"));
        List<String> metadataKeys = this.datasetMetadataService.getAllKeyNames();
        Collections.sort(metadataKeys);
        headers.addAll(metadataKeys);
        try (CSVWriter writer = new CSVWriter(new FileWriter(path + csvName))) {
            List<DatasetModel> datasets = this.datasetService.findAll();
            datasets.forEach(datasetModel -> {
                List<DatasetMetadataModel> metadata = this.datasetMetadataService.findByDatasetID(datasetModel.getDatasetId());
                String[] row = new String[headers.size()];
                row[0] = datasetModel.getRef();
                row[1] = datasetModel.getFullName();
                row[2] = datasetModel.getAbbreviation();
                row[3] = datasetModel.getDescription();
                final int[] col = {4};
                // Metadata keys is sorted so we should always end up with the keys as columns in sorted order.
                metadataKeys.forEach(key -> {
                    Optional<DatasetMetadataModel> datasetMetaDataByKey =
                            metadata.stream().filter(meta -> meta.getKey().equals(key)).findFirst();
                    row[col[0]] = datasetMetaDataByKey.isPresent() ? datasetMetaDataByKey.get().getValue() : "";
                    col[0]++;
                });

                writer.writeNext(row);
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void generateDatasetsCSV(String path, List<String> headers) {
        String csvName = "Dataset.csv";

        try (CSVWriter writer = new CSVWriter(new FileWriter(path + csvName))) {
            List<DatasetModel> datasets = this.datasetService.findAll();
            datasets.forEach(datasetModel -> {
                List<DatasetMetadataModel> metadata = this.datasetMetadataService.findByDatasetID(datasetModel.getDatasetId());
                String[] row = new String[headers.size()];
                row[0] = datasetModel.getRef();
                row[1] = datasetModel.getFullName();
                row[2] = datasetModel.getAbbreviation();
                row[3] = datasetModel.getDescription();
                final int[] col = {4};
                // Metadata keys is sorted so we should always end up with the keys as columns in sorted order.
                metadataKeys.forEach(key -> {
                    Optional<DatasetMetadataModel> datasetMetaDataByKey =
                            metadata.stream().filter(meta -> meta.getKey().equals(key)).findFirst();
                    row[col[0]] = datasetMetaDataByKey.isPresent() ? datasetMetaDataByKey.get().getValue() : "";
                    col[0]++;
                });

                writer.writeNext(row);
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



}
