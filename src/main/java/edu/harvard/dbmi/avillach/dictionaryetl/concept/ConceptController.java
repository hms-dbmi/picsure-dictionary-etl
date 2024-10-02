package edu.harvard.dbmi.avillach.dictionaryetl.concept;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.hibernate.mapping.Map;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
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
import org.springframework.data.jpa.*;

import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetConceptRepository;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;

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
    @Autowired
    FacetConceptRepository facetConceptRepository;
    @PersistenceContext
    private EntityManager entityManager;

    @GetMapping("/concept")
    public ResponseEntity<List<ConceptModel>> getAllConceptModels(@RequestParam(required = false) String datasetRef) {
        try {
            List<ConceptModel> conceptModels = new ArrayList<ConceptModel>();

            if (datasetRef == null) {
                // get all concepts in dictionary
                conceptRepository.findAll().forEach(conceptModels::add);
            } else {
                // get all concepts in specific dataset
                Long datasetId = datasetRepository.findByRef(datasetRef).get().getDatasetId();
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
        Optional<ConceptModel> parentData = conceptRepository.findByConceptPath(parentPath);
        Optional<DatasetModel> datasetData = datasetRepository.findByRef(datasetRef);
        Long datasetId;
        if (datasetData.isPresent()) {
            datasetId = datasetData.get().getDatasetId();
        } else {
            System.out.println("Dataset not found: " + datasetRef + ". Failed to create/update concept " + conceptPath);
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }

        Long parentId;
        if (parentData.isPresent()) {
            parentId = parentData.get().getConceptNodeId();
        } else {
            parentId = null;
        }
        if (conceptData.isPresent()) {
            // update already existing concept
            ConceptModel existingConcept = conceptData.get();
            existingConcept.setConceptType(conceptType);
            existingConcept.setDatasetId(datasetId);
            existingConcept.setDisplay(display);
            existingConcept.setParentId(parentId);
            existingConcept.setName(name);
            return new ResponseEntity<>(conceptRepository.save(existingConcept), HttpStatus.OK);
        } else {
            // add new concept when concept not present in data
            try {
                ConceptModel newConcept = conceptRepository
                        .save(new ConceptModel(datasetId, name,
                                display, conceptType, conceptPath,
                                parentId));
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

            Long conceptId = conceptData.get().getConceptNodeId();
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
            conceptRepository.delete(conceptData.get());
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        } else {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @GetMapping("/concept/metadata")
    public ResponseEntity<List<ConceptMetadataModel>> getAllConceptMetadataModels(
            @RequestParam Optional<String> conceptPath) {
        try {
            List<ConceptMetadataModel> conceptMetadataModels = new ArrayList<ConceptMetadataModel>();

            if (conceptPath == null || !conceptPath.isPresent()) {
                // get all conceptMetadatas in dictionary
                System.out.println("Hitting conceptMetadata null");
                conceptMetadataRepository.findAll().forEach(conceptMetadataModels::add);
            } else {
                System.out.println("Hitting conceptMetadata");
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
            @RequestParam String key, @RequestBody String values) {
        Optional<ConceptModel> concept = conceptRepository.findByConceptPath(conceptPath);
        Long conceptNodeId;
        if (concept.isPresent()) {
            conceptNodeId = concept.get().getConceptNodeId();
        } else {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
        Optional<ConceptMetadataModel> conceptMetadataData = conceptMetadataRepository
                .findByConceptNodeIdAndKey(conceptNodeId, key);

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
                            .save(new ConceptMetadataModel(conceptNodeId, key,
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

    // Specifically for stigvar updates
    @PutMapping("/concept/metadata/stigvars")
    public ResponseEntity<ConceptMetadataModel> updateStigvars(@RequestBody String conceptsToRemove) {

        try {
            String[] concepts = conceptsToRemove.split("\n");
            List<String> conceptList = Arrays.asList(concepts);
            List<String> queryEntries = new ArrayList<String>();
            conceptList.forEach(path -> {
                Long conceptId;
                try {
                    Optional<ConceptModel> concept = conceptRepository.findByConceptPath(path);
                    conceptId = concept.get().getConceptNodeId();
                    queryEntries.add("(" + conceptId + "," + "'stigmatized', 'true')");

                } catch (Exception e) {
                    System.out.println("Concept path not found in database " + path);
                }

                // updateConceptMetadata(path, "stigmatized", "true");
            });
            try {
                String queryInset = queryEntries.stream().collect(Collectors.joining(", "));
                System.out.println(queryInset);
                Query fullQuery = entityManager.createNativeQuery(
                        "insert into concept_node_meta (concept_node_id, key, value) VALUES " + queryInset
                                + "ON CONFLICT (key, concept_node_id) "
                                + "DO UPDATE SET value='true'");
                fullQuery.executeUpdate();
                // conceptMetadataRepository.insertOrUpdateConceptMeta(fullQuery, "true");
            } catch (Exception e) {
                System.out.print(e.getMessage());
            }
        } catch (JSONException e) {
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return new ResponseEntity<>(null, HttpStatus.CREATED);

    }

    // Specifically for mass value updates
    @PutMapping("/concept/metadata/values")
    public ResponseEntity<ConceptMetadataModel> updateManyValues(@RequestBody String values) {

        try {
            JSONArray valArray = new JSONArray(values);
            for (int i = 0; i < valArray.length(); i++) {
                JSONObject obj = valArray.getJSONObject(i);
                String path = obj.getString("concept_path");
                JSONArray vals = obj.getJSONArray("values");
                updateConceptMetadata(path, "values", vals.toString());
            }
        } catch (JSONException e) {
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return new ResponseEntity<>(null, HttpStatus.CREATED);

    }

    @DeleteMapping("/concept/metadata")
    public ResponseEntity<ConceptMetadataModel> deleteConceptMetadata(@RequestParam Optional<String> conceptPath,
            @RequestParam String key) {
        if (conceptPath.isPresent()) {
            Long conceptId = conceptRepository.findByConceptPath(conceptPath.get()).get().getConceptNodeId();
            Optional<ConceptMetadataModel> conceptMetadataData = conceptMetadataRepository
                    .findByConceptNodeIdAndKey(conceptId, key);

            if (conceptMetadataData.isPresent()) {
                conceptMetadataRepository.delete(conceptMetadataData.get());
                return new ResponseEntity<>(HttpStatus.NO_CONTENT);
            } else {
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            }
        } else if (key.length() > 0) {
            conceptMetadataRepository.findByKey(key).forEach(cm -> {
                conceptMetadataRepository.delete(cm);
            });
        }

        return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);

    }
}
