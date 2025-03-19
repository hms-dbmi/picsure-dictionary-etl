package edu.harvard.dbmi.avillach.dictionaryetl.export;

import com.opencsv.CSVWriter;
import edu.harvard.dbmi.avillach.dictionaryetl.Utility.ColumnMetaUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataService;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptService;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetMetadataService;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetService;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class DictionaryCSVService {

    private static final Logger log = LoggerFactory.getLogger(DictionaryCSVService.class);
    private final DatasetService datasetService;
    private final DatasetMetadataService datasetMetadataService;
    private final ConceptService conceptService;
    private final ConceptMetadataService conceptMetadataService;
    private final ColumnMetaUtility columnMetaUtility;

    @Autowired
    public DictionaryCSVService(DatasetService datasetService, DatasetMetadataService datasetMetadataService, ConceptService conceptService, ConceptMetadataService conceptMetadataService, ColumnMetaUtility columnMetaUtility) {
        this.datasetService = datasetService;
        this.datasetMetadataService = datasetMetadataService;
        this.conceptService = conceptService;
        this.conceptMetadataService = conceptMetadataService;
        this.columnMetaUtility = columnMetaUtility;
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

        try (ExecutorService executorService = Executors.newFixedThreadPool(6)) {
            List<Callable<Void>> tasks = new ArrayList<>();
            tasks.add(() -> {
                generateDatasetsCSV(path);
                return null;
            });
            tasks.add(() -> {
                generateConsentsCSV(path);
                return null;
            });
            tasks.add(() -> {
                generateConceptsCSV(path);
                return null;
            });
            tasks.add(() -> {
                generateFacetCategories(path);
                return null;
            });
            tasks.add(() -> {
                generateFacetsCSV(path);
                return null;
            });
            tasks.add(() -> {
                generateFacetConceptLists(path);
                return null;
            });

            try {
                System.out.println("Generating all ingest csv files...");
                // Invoke all tasks and wait for completion
                executorService.invokeAll(tasks);
                System.out.println("All csv files are completed...");
            } catch (InterruptedException e) {
                log.error(e.getMessage());
            } finally {
                executorService.shutdown();
            }
        }
    }

    private void generateFacetConceptLists(String path) {

    }

    private void generateFacetsCSV(String path) {

    }

    private void generateFacetCategories(String path) {

    }

    private void generateConceptsCSV(String path) {
        String csvName = "Concept.csv";
        List<String> headers = new ArrayList<>(List.of("dataset_ref", "concept name", "display name", "concept_type",
                "concept_path", "parent_concept_path", "values", "description"));
        List<String> metadataKeys = this.conceptMetadataService.allMetadataKeys();
        Collections.sort(metadataKeys);
        headers.addAll(metadataKeys);

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
        List<String> metadataKeys = this.datasetMetadataService.allMetadataKeys();
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

    protected void generateDatasetsCSV(String path) {
        String csvName = "Dataset.csv";
        List<String> headers = new ArrayList<>(List.of("ref", "full_name", "abbreviation", "description"));
        List<String> metadataKeys = this.datasetMetadataService.allMetadataKeys();
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

}
