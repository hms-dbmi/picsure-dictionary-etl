package edu.harvard.dbmi.avillach.dictionaryetl.export;

import com.opencsv.CSVWriter;
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
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryMeta;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

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

    @Autowired
    public DictionaryCSVService(DatasetService datasetService, DatasetMetadataService datasetMetadataService, ConceptService conceptService, ConceptMetadataService conceptMetadataService, ColumnMetaUtility columnMetaUtility, FacetService facetService, FacetCategoryService facetCategoryService, ConsentService consentService, CSVUtility csvUtility) {
        this.datasetService = datasetService;
        this.datasetMetadataService = datasetMetadataService;
        this.conceptService = conceptService;
        this.conceptMetadataService = conceptMetadataService;
        this.columnMetaUtility = columnMetaUtility;
        this.facetService = facetService;
        this.facetCategoryService = facetCategoryService;
        this.consentService = consentService;
        this.csvUtility = csvUtility;
    }

    public void generateFullIngestCSVs(String path, String... datasetRefs) {
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
        List<String> conceptMetadataKeys = this.conceptMetadataService.findMetadataKeysByDatasetID(datasetIDs);
        String[] conceptCSVHeaders = getConceptCSVHeaders(conceptMetadataKeys);
        String fullConceptPath = path + "/Concept.csv";
        this.csvUtility.createCSVFile(fullConceptPath, conceptCSVHeaders);
        // -----------------------------------------------------------------------------------------

        // Create and Populate Facet Categories CSV
        // -----------------------------------------------------------------------------------------
        List<String> facetCategoryMetadataKeyNames = this.facetCategoryService.getFacetCategoryMetadataKeyNames();
        String[] facetCategoriesHeaders = getFacetCategoriesHeaders(facetCategoryMetadataKeyNames);
        String fullFacetCategoryPath = path + "/Facet_Categories.csv";
        this.csvUtility.createCSVFile(fullFacetCategoryPath, facetCategoriesHeaders);
        List<FacetCategoryModel> facetCategoryModels = this.facetCategoryService.findAll();
        generateFacetCategoriesCSV(fullFacetCategoryPath, facetCategoriesHeaders, facetCategoryMetadataKeyNames, facetCategoryModels);
        // -----------------------------------------------------------------------------------------

        log.info("Creating Facet.csv");
        List<String> facetMetadataKeyNames = this.facetService.getFacetMetadataKeyNames();
        String[] facetCSVHeaders = getFacetCSVHeaders(facetMetadataKeyNames);
        String fullFacetPath = path + "/Facet.csv";
        this.csvUtility.createCSVFile(fullFacetPath, facetCSVHeaders);
        log.info("Facet.csv created with initial headers");
        generateFacetCSV(fullFacetPath, facetCSVHeaders, facetMetadataKeyNames, facetCategoryModels, datasetIDs);

        /*
         * TODO: Make this faster
         * I think to push performance further we can make many of the concept CSVs in parallel.
         * We can just name them based on their dataset ref and then merge them later.
         *
         */
        for (DataSetRefDto dataset : datasets) {
            log.info("Populating CSVs for dataset: {}", dataset.getRef());
            generateConsentsCSV(fullConsentPath, dataset, consentsCsvWithHeaders);
//            generateConceptsCSV(fullConceptPath, conceptCSVHeaders, conceptMetadataKeys, dataset);
            log.info("Populated CSVs for dataset: {}", dataset.getRef());
        }

    }

    private void generateFacetCSV(String fullFacetPath, String[] facetCategoriesHeaders, List<String> facetMetadataKeyNames, List<FacetCategoryModel> facetCategoryModels, Long[] datasetIDs) {
        try (CSVWriter writer = new CSVWriter(new FileWriter(fullFacetPath, true))) {
            List<FacetModel> facetModels = this.facetService.findAllFacetsByDatasetIDs(datasetIDs);
            // create a map of facet category id to name
            Map<Long, String> facetCategoryIdToName = new HashMap<>();
            facetCategoryModels.forEach(facetCategoryModel -> {
                facetCategoryIdToName.put(facetCategoryModel.getFacetCategoryId(), facetCategoryModel.getName());
            });

            // make map of parent id to object
            Map<Long, FacetModel> parentIdToFacetModel = new HashMap<>();
            facetModels.stream()
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
                row[4] = parentFacet != null ? parentFacet.getName() : "";

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

    private String[] getFacetConceptListHeaders(List<String> datasetMetadataKeys, List<String> facetName) {
        //dataset_id_1	dataset_id_2	dataset_id_3	facet_name1	facet_name2	facet_name3
        // loop over the datasetMetadataKeys and place them in the header

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

    private void generateConceptsCSV(String fullPath,
                                     String[] conceptCSVHeaders,
                                     List<String> conceptMetaDataKeys,
                                     DataSetRefDto dataSetRefDto) {
        try (CSVWriter writer = new CSVWriter(new FileWriter(fullPath, true))) {
            List<ConceptModel> conceptModels = this.conceptService.findByDatasetID(dataSetRefDto.getDatasetId());
            conceptModels.forEach(concept -> {
                String[] row = new String[conceptCSVHeaders.length];
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

                Optional<ConceptMetadataModel> values = Optional.empty();
                for (ConceptMetadataModel metadataModel : conceptMetadatas) {
                    if (metadataModel.getKey().equals("values")) {
                        values = Optional.of(metadataModel);
                        break;
                    }
                }
                String conceptType = concept.getConceptType();
                row[6] = convertConceptValuesToDelimitedString(values, conceptType);
                Optional<ConceptMetadataModel> description = conceptMetadatas.stream().filter(conceptMetadata -> conceptMetadata.getKey().equals("values")).findFirst();
                row[7] = description.isPresent() ? description.get().getValue() : "";

                int col = 8;
                for (String key : conceptMetaDataKeys) {
                    Optional<ConceptMetadataModel> conceptMetaDataByKey =
                            conceptMetadatas.stream().filter(meta -> meta.getKey().equals(key)).findFirst();
                    row[col] = conceptMetaDataByKey.isPresent() ? conceptMetaDataByKey.get().getValue() : "";
                    col++;
                }

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
        try {
            if (conceptType.equals("categorical")) {
                List<String> strings = this.columnMetaUtility.parseValues(data);
                values = String.join("µ", strings);
            } else {
                Float max = this.columnMetaUtility.parseMax(data);
                Float min = this.columnMetaUtility.parseMin(data);
                values = min + "µ" + max;
            }
        } catch (NumberFormatException e) {
            log.warn("Error parsing values for concept: {} with values: {}", conceptMetadataModel.getConceptNodeId(), data, e);
            return data;
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
