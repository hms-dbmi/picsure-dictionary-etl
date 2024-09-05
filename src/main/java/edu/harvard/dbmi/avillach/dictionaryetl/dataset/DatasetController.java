package edu.harvard.dbmi.avillach.dictionaryetl.dataset;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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

import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptMetadataRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.consent.ConsentRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetController;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryController;

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

    @DeleteMapping("/dataset")
    public ResponseEntity<DatasetModel> deleteDataset(@RequestParam String datasetRef) {

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
            consentRepository.findByDatasetId(datasetId).forEach(consent -> {
                consentRepository.delete(consent);
            });
            datasetRepository.delete(datasetData.get());
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        } else {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
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
}
