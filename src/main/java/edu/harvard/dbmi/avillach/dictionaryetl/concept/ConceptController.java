package edu.harvard.dbmi.avillach.dictionaryetl.concept;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;

@CrossOrigin(origins = "http://localhost:8081")
@RestController
@RequestMapping("/api")
public class ConceptController {
    @Autowired
    ConceptRepository conceptRepository;
    @Autowired
    ConceptMetadataRepository conceptMetadataRepository;
    @Autowired
    DatasetRepository datasetRepository;

    @GetMapping("/concept")
    public ResponseEntity<List<ConceptModel>> getAllConceptModels(@RequestParam(required = false) String datasetRef) {
        try {
            List<ConceptModel> conceptModels = new ArrayList<ConceptModel>();

            if (datasetRef == null) {
                // get all concepts in dictionary
                System.out.println("Hitting concept");
                conceptRepository.findAll().forEach(conceptModels::add);
            } else {
                // get all concepts in specific dataset
                Long datasetId = datasetRepository.findByDatasetRef(datasetRef).get().getDatasetId();
                conceptRepository.findByDatasetId(datasetId).forEach(conceptModels::add);

            }
            if (conceptModels.isEmpty()) {
                return new ResponseEntity<>(HttpStatus.NO_CONTENT);
            }
            return new ResponseEntity<>(conceptModels, HttpStatus.OK);
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PutMapping("/concept")
    public ResponseEntity<ConceptModel> updateConcept(@RequestParam String conceptPath, @RequestParam String datasetRef,
            @RequestParam String conceptType, @RequestParam String display, @RequestParam String name,
            @RequestParam String parentPath) {

        Optional<ConceptModel> conceptData = conceptRepository.findByConceptPath(conceptPath);
        Long conceptParentId = conceptRepository.findByConceptPath(parentPath).get().getConceptNodeId();
        Long datasetId = datasetRepository.findByDatasetRef(datasetRef).get().getDatasetId();

        if (conceptData.isPresent()) {
            // update already existing concept
            ConceptModel existingConcept = conceptData.get();
            existingConcept.setConceptType(conceptType);
            existingConcept.setDatasetId(datasetId);
            existingConcept.setDisplay(display);
            existingConcept.setParentId(conceptParentId);
            existingConcept.setName(name);
            return new ResponseEntity<>(conceptRepository.save(existingConcept), HttpStatus.OK);
        } else {
            // add new concept when concept not present in data
            try {
                ConceptModel newConcept = conceptRepository
                        .save(new ConceptModel(datasetId, name,
                                display, conceptType, conceptPath,
                                conceptParentId));
                return new ResponseEntity<>(newConcept, HttpStatus.CREATED);
            } catch (Exception e) {
                System.out.println(e.getLocalizedMessage());
                return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
            }
        }

    }

    @DeleteMapping("/concept")
    public ResponseEntity<ConceptModel> deleteConcept(@RequestParam String conceptPath) {

        Optional<ConceptModel> conceptData = conceptRepository.findByConceptPath(conceptPath);

        if (conceptData.isPresent()) {
            conceptRepository.delete(conceptData.get());
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        } else {
            // add new concept when concept not present in data
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @GetMapping("/concept/metadata")
    public ResponseEntity<List<ConceptMetadataModel>> getAllConceptMetadataModels(
            @RequestParam Optional<String> conceptPath) {
        try {
            List<ConceptMetadataModel> conceptMetadataModels = new ArrayList<ConceptMetadataModel>();

            if (conceptPath == null) {
                // get all conceptMetadatas in dictionary
                System.out.println("Hitting conceptMetadata");
                conceptMetadataRepository.findAll().forEach(conceptMetadataModels::add);
            } else {
                Long conceptId = conceptRepository.findByConceptPath(conceptPath.get()).get().getConceptNodeId();
                conceptMetadataRepository.findByConceptNodeId(conceptId).forEach(conceptMetadataModels::add);

            }
            if (conceptMetadataModels.isEmpty()) {
                return new ResponseEntity<>(HttpStatus.NO_CONTENT);
            }
            return new ResponseEntity<>(conceptMetadataModels, HttpStatus.OK);
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PutMapping("/concept/metadata")
    public ResponseEntity<ConceptMetadataModel> updateConceptMetadata(@RequestParam String conceptPath,
            @RequestParam String key, @RequestParam String values) {
        ConceptModel concept = conceptRepository.findByConceptPath(conceptPath).get();
        Optional<ConceptMetadataModel> conceptMetadataData = conceptMetadataRepository
                .findByConceptNodeIdAndKey(concept.getConceptNodeId(), key);

        try {
            if (conceptMetadataData.isPresent()) {
                // update already existing conceptMetadata
                ConceptMetadataModel existingConceptMetadata = conceptMetadataData.get();
                existingConceptMetadata.setValue(values);
                return new ResponseEntity<>(conceptMetadataRepository.save(existingConceptMetadata), HttpStatus.OK);
            } else {
                // add new conceptMetadata when conceptMetadata not present in data
                try {
                    ConceptMetadataModel newConceptMetadata = conceptMetadataRepository
                            .save(new ConceptMetadataModel(concept.getConceptNodeId(), key,
                                    values));
                    return new ResponseEntity<>(newConceptMetadata, HttpStatus.CREATED);
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

    @DeleteMapping("/concept/metadata")
    public ResponseEntity<ConceptMetadataModel> deleteConceptMetadata(@RequestParam String conceptPath,
            @RequestParam String key) {

        Long conceptId = conceptRepository.findByConceptPath(conceptPath).get().getConceptNodeId();
        Optional<ConceptMetadataModel> conceptMetadataData = conceptMetadataRepository
                .findByConceptNodeIdAndKey(conceptId, key);

        if (conceptMetadataData.isPresent()) {
            conceptMetadataRepository.delete(conceptMetadataData.get());
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        } else {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }
}
