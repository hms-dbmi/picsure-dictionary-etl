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

@CrossOrigin(origins = "http://localhost:8081")
@RestController
@RequestMapping("/api")
public class DatasetController {
    @Autowired
    DatasetRepository datasetRepository;

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

        Optional<DatasetModel> datasetData = datasetRepository.findByDatasetRef(datasetRef);

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

        Optional<DatasetModel> datasetData = datasetRepository.findByDatasetRef(datasetRef);

        if (datasetData.isPresent()) {
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

            if (datasetRef == null) {
                // get all dataset metadata in dictionary
                System.out.println("Hitting datasetMetadata");
                datasetMetadataRepository.findAll().forEach(datasetMetadataModels::add);
            } else {
                // get all dataset metadata in specific dataset
                Long datasetId = datasetRepository.findByDatasetRef(datasetRef.get()).get().getDatasetId();
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
    public ResponseEntity<DatasetMetadataModel> updateDataset(@RequestParam String datasetRef,
            @RequestParam String key, @RequestParam String values) {
        Long datasetId = datasetRepository.findByDatasetRef(datasetRef).get().getDatasetId();
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

        Long datasetId = datasetRepository.findByDatasetRef(datasetRef).get().getDatasetId();
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
