package edu.harvard.dbmi.avillach.dictionaryetl.dataset;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.consent.ConsentRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetController;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryController;
import jakarta.transaction.Transactional;

@CrossOrigin(origins = "http://localhost:8081")
@RestController
@RequestMapping("/api")
public class DatasetController {
    @Autowired
    DatasetRepository datasetRepository;

    @Autowired
    FacetRepository facetRepository;

    @Autowired
    ConceptRepository conceptRepository;

    @Autowired
    ConceptMetadataRepository conceptMetadataRepository;

    @Autowired
    FacetConceptRepository facetConceptRepository;

    @Autowired
    ConsentRepository consentRepository;

    @Autowired
    DatasetMetadataRepository datasetMetadataRepository;
    @Autowired
    DatasetHarmonizationRepository datasetHarmonizationRepository;

    @GetMapping("/dataset")
    public ResponseEntity<List<DatasetModel>> getAllDatasetModels() {
        try {
            List<DatasetModel> datasetModels = new ArrayList<DatasetModel>();
            datasetRepository.findAll().forEach(datasetModels::add);

            if (datasetModels.isEmpty()) {
                return new ResponseEntity<>(HttpStatus.NO_CONTENT);
            }
            return new ResponseEntity<>(datasetModels, HttpStatus.OK);
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PutMapping("/dataset")
    public ResponseEntity<DatasetModel> updateDataset(@RequestParam String datasetRef,
            @RequestParam String fullName, @RequestParam String abv, @RequestParam String desc) {

        Optional<DatasetModel> datasetData = datasetRepository.findByRef(datasetRef);

        if (datasetData.isPresent()) {
            // update already existing dataset
            DatasetModel existingDataset = datasetData.get();
            existingDataset.setAbbreviation(abv);
            existingDataset.setDescription(desc);
            existingDataset.setFullName(fullName);
            return new ResponseEntity<>(datasetRepository.save(existingDataset), HttpStatus.OK);
        } else {
            // add new dataset when dataset not present in data
            try {
                DatasetModel newDataset = datasetRepository
                        .save(new DatasetModel(datasetRef,
                                fullName, abv, desc));
                return new ResponseEntity<>(newDataset, HttpStatus.CREATED);
            } catch (Exception e) {
                System.out.println(e.getLocalizedMessage());
                return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
            }
        }

    }

    @Transactional
    @PutMapping("/dataset/csv")
    public ResponseEntity<Object> updateDatasetsFromCsv(@RequestBody String input) {
        DatasetService datasetService = new DatasetService(datasetRepository);
        Map<String, Integer> headerMap = new HashMap<String, Integer>();
        List<String> metaColumnNames = new ArrayList<>();
        List<String[]> datasets = new ArrayList<>();
        try (CSVReader reader = new CSVReader(new StringReader(input))) {
            String[] header = reader.readNext();
            headerMap = datasetService.buildCsvInputsHeaderMap(header);
            String[] coreDatasetHeaders = { "ref", "full_name", "abbreviation", "description" };
            if (!headerMap.keySet().containsAll(Arrays.asList(coreDatasetHeaders))) {
                return new ResponseEntity<>(
                        "Headers in dataset ingest file incorrect",
                        HttpStatus.BAD_REQUEST);
            } else {
                headerMap.keySet().forEach(k -> {
                    if (!Arrays.asList(coreDatasetHeaders).contains(k)) {
                        metaColumnNames.add(k);
                    }
                });
            }
            datasets = reader.readAll();
            datasets.remove(header);
            reader.close();
        } catch (IOException | CsvException e) {
            return new ResponseEntity<>(
                    "Error reading dataset ingestion csv. Error: \n" + e.getStackTrace(),
                    HttpStatus.BAD_REQUEST);
        }
        if (datasets.isEmpty()) {
            return new ResponseEntity<>(
                    "No csv records found in dataset input file.",
                    HttpStatus.BAD_REQUEST);
        }
        int datasetCount = datasets.size();
        int datasetUpdateCount = 0;
        int metaUpdateCount = 0;

        for (int i = 0; i < datasetCount; i++) {
            String[] dataset = datasets.get(i);
            String ref = dataset[headerMap.get("ref")];
            String fullName = dataset[headerMap.get("full_name")];
            String abbreviation = dataset[headerMap.get("abbreviation")];
            String description = dataset[headerMap.get("description")];
            Optional<DatasetModel> datasetData = datasetRepository.findByRef(ref);
            DatasetModel datasetModel;
            if (datasetData.isPresent()) {
                // update already existing dataset
                datasetModel = datasetData.get();
                datasetModel.setAbbreviation(abbreviation);
                datasetModel.setDescription(description);
                datasetModel.setFullName(fullName);
            } else {
                // add new dataset when dataset not present in data
                datasetModel = new DatasetModel(ref,
                        fullName, abbreviation, description);
            }
            datasetRepository.save(datasetModel);
            datasetUpdateCount++;
            for (int j = 0; j < metaColumnNames.size(); j++) {
                String key = metaColumnNames.get(j);
                String value = dataset[headerMap.get(key)];
                if (!value.isBlank() && value != null) {
                    DatasetMetadataModel dmModel = datasetMetadataRepository.findByDatasetIdAndKey(datasetModel.getDatasetId(), key).orElse(new DatasetMetadataModel(datasetModel.getDatasetId(), key, value));
                    dmModel.setValue(value);
                    datasetMetadataRepository.save(dmModel);
                    metaUpdateCount++;
                }
            }
        }
        return new ResponseEntity<>("Successfully created/updated " + datasetUpdateCount + " datasets and "
                + metaUpdateCount + " dataset metadata entries from input csv.", HttpStatus.CREATED);
    }

    @DeleteMapping("/dataset")
    public ResponseEntity<String> deleteDataset(@RequestParam String datasetRef) {

        Optional<DatasetModel> datasetData = datasetRepository.findByRef(datasetRef);

        if (datasetData.isPresent()) {
            Long datasetId = datasetData.get().getDatasetId();

            conceptRepository.findByDatasetId(datasetId).forEach(
                    concept -> {
                        Long conceptId = concept.getConceptNodeId();
                        // find all child concept nodes and null the parent ids to prevent dependency
                        // errors
                        // potentially would want to instead set the parent id to dataset or the
                        // parent's parent id - must do eval on use case of single var deletion
                        conceptRepository.findByParentId(conceptId).forEach(child -> {
                            child.setParentId(null);
                            conceptRepository.save(child);
                        });

                        facetConceptRepository.findByConceptNodeId(conceptId).get().forEach(fc -> {
                            facetConceptRepository.delete(fc);
                        });
                        conceptMetadataRepository.findByConceptNodeId(conceptId).forEach(cm -> {
                            conceptMetadataRepository.delete(cm);
                        });
                        conceptRepository.delete(concept);
                    });
            datasetMetadataRepository.findByDatasetId(datasetId).forEach(dm -> {
                datasetMetadataRepository.delete(dm);
            });
            if (facetRepository.findByName(datasetRef).isPresent()) {
                facetRepository.delete(facetRepository.findByName(datasetRef).get());
            }
            consentRepository.findByDatasetId(datasetId).forEach(consent -> {
                consentRepository.delete(consent);
            });
            datasetHarmonizationRepository.findBySourceDatasetId(datasetId).forEach(dh -> {
                datasetHarmonizationRepository.delete(dh);
            });
            datasetRepository.delete(datasetData.get());
            return new ResponseEntity<>("Dataset deleted", HttpStatus.OK);
        } else {
            return new ResponseEntity<>("No dataset found to delete", HttpStatus.NO_CONTENT);
        }
    }

    @GetMapping("/dataset/metadata")
    public ResponseEntity<List<DatasetMetadataModel>> getAllDatasetMetadataModels(
            @RequestParam Optional<String> datasetRef) {
        try {
            List<DatasetMetadataModel> datasetMetadataModels = new ArrayList<DatasetMetadataModel>();

            if (datasetRef == null || !datasetRef.isPresent()) {
                // get all dataset metadata in dictionary
                System.out.println("Hitting datasetMetadata");
                datasetMetadataRepository.findAll().forEach(datasetMetadataModels::add);
            } else {
                // get all dataset metadata in specific dataset
                Long datasetId = datasetRepository.findByRef(datasetRef.get()).get().getDatasetId();
                datasetMetadataRepository.findByDatasetId(datasetId).forEach(datasetMetadataModels::add);

            }
            if (datasetMetadataModels.isEmpty()) {
                return new ResponseEntity<>(HttpStatus.NO_CONTENT);
            }
            return new ResponseEntity<>(datasetMetadataModels, HttpStatus.OK);
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PutMapping("/dataset/metadata")
    public ResponseEntity<DatasetMetadataModel> updateDatasetMetadata(@RequestParam String datasetRef,
            @RequestParam String key, @RequestParam String values) {
        Long datasetId = datasetRepository.findByRef(datasetRef).get().getDatasetId();
        Optional<DatasetMetadataModel> datasetMetadataData = datasetMetadataRepository.findByDatasetIdAndKey(datasetId,
                key);

        try {
            if (datasetMetadataData.isPresent()) {
                // update already existing datasetMetadata
                DatasetMetadataModel existingDatasetMetadata = datasetMetadataData.get();
                existingDatasetMetadata.setValue(values);
                return new ResponseEntity<>(datasetMetadataRepository.save(existingDatasetMetadata), HttpStatus.OK);
            } else {
                // add new datasetMetadata when datasetMetadata not present in data
                try {
                    DatasetMetadataModel newDatasetMetadata = datasetMetadataRepository
                            .save(new DatasetMetadataModel(datasetId, key,
                                    values));
                    return new ResponseEntity<>(newDatasetMetadata, HttpStatus.CREATED);
                } catch (Exception e) {
                    System.out.println(e.getLocalizedMessage());
                    return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
                }
            }
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }

    }

    @DeleteMapping("/dataset/metadata")
    public ResponseEntity<DatasetMetadataModel> deleteDatasetMetadata(@RequestParam String datasetRef,
            @RequestParam String key) {

        Long datasetId = datasetRepository.findByRef(datasetRef).get().getDatasetId();
        Optional<DatasetMetadataModel> datasetMetadataData = datasetMetadataRepository.findByDatasetIdAndKey(datasetId,
                key);

        if (datasetMetadataData.isPresent()) {
            datasetMetadataRepository.delete(datasetMetadataData.get());
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        } else {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @PutMapping("/dataset/harmonization")
    public ResponseEntity<DatasetHarmonizationModel> updateDatasetHarmonization(
            @RequestParam String harmonizedDatasetRef,
            @RequestParam String sourceDatasetRef) {
        Long harmonizedDatasetId = datasetRepository.findByRef(harmonizedDatasetRef).get().getDatasetId();
        Long sourceDatasetId = datasetRepository.findByRef(sourceDatasetRef).get().getDatasetId();
        Optional<DatasetHarmonizationModel> existingHarmonization = datasetHarmonizationRepository
                .findBySourceDatasetIdAndHarmonizedDatasetId(sourceDatasetId, harmonizedDatasetId);
        if (!existingHarmonization.isPresent()) {
            try {
                DatasetHarmonizationModel newDatasetHarmonization = datasetHarmonizationRepository
                        .save(new DatasetHarmonizationModel(harmonizedDatasetId, sourceDatasetId));
                return new ResponseEntity<>(newDatasetHarmonization, HttpStatus.CREATED);
            } catch (Exception e) {
                System.out.println(e.getLocalizedMessage());
                return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
            }
        } else {
            return new ResponseEntity<>(existingHarmonization.get(), HttpStatus.CREATED);
        }

    }

    @DeleteMapping("/dataset/harmonization")
    public ResponseEntity<DatasetHarmonizationModel> deleteDatasetHarmonization(
            @RequestParam String harmonizedDatasetRef,
            @RequestParam String sourceDatasetRef) {

        Long harmonizedDatasetId = datasetRepository.findByRef(harmonizedDatasetRef).get().getDatasetId();
        Long sourceDatasetId = datasetRepository.findByRef(sourceDatasetRef).get().getDatasetId();
        Optional<DatasetHarmonizationModel> datasetHarmonizationData = datasetHarmonizationRepository
                .findBySourceDatasetIdAndHarmonizedDatasetId(
                        sourceDatasetId, harmonizedDatasetId);

        if (datasetHarmonizationData.isPresent()) {
            datasetHarmonizationRepository.delete(datasetHarmonizationData.get());
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        } else {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }
}
