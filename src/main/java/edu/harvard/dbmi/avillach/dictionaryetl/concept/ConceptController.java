package edu.harvard.dbmi.avillach.dictionaryetl.concept;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.Transaction;
import org.hibernate.transform.AliasToEntityMapResultTransformer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.jpa.repository.QueryHints;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;

import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetConceptRepository;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import jakarta.persistence.Tuple;
import jakarta.persistence.TupleElement;
import jakarta.transaction.Transactional;
import org.springframework.transaction.support.*;
import org.springframework.util.StringUtils;

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
    @Autowired
    SessionFactory sessionFactory;
    @PersistenceContext
    private EntityManager entityManager;
    private int BATCH_SIZE = 100;

    @GetMapping("/concept")
    public ResponseEntity<Object> getAllConceptModels(@RequestParam(required = false) String datasetRef) {
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
                return new ResponseEntity<>("No concepts found for " + datasetRef, HttpStatus.NO_CONTENT);
            }
            return new ResponseEntity<>(conceptModels, HttpStatus.OK);
        } catch (Exception e) {
            return new ResponseEntity<>(e.getLocalizedMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PutMapping("/concept")
    public ResponseEntity<Object> updateConcept(@RequestParam String conceptPath, @RequestParam String datasetRef,
            @RequestParam String conceptType, @RequestParam String display, @RequestParam String name,
            @RequestParam String parentPath) {

        Optional<ConceptModel> conceptData = conceptRepository.findByConceptPath(conceptPath);
        Optional<ConceptModel> parentData = conceptRepository.findByConceptPath(parentPath);
        Optional<DatasetModel> datasetData = datasetRepository.findByRef(datasetRef);
        Long datasetId;
        if (datasetData.isPresent()) {
            datasetId = datasetData.get().getDatasetId();
        } else {
            return new ResponseEntity<>(
                    "Dataset not found: " + datasetRef + ". Failed to create/update concept " + conceptPath,
                    HttpStatus.NOT_FOUND);
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
            if (conceptType != null)
                existingConcept.setConceptType(conceptType);
            existingConcept.setDatasetId(datasetId);
            existingConcept.setDisplay(display);
            if (parentId != null) {
                existingConcept.setParentId(parentId);
            }
            existingConcept.setName(name);
            return new ResponseEntity<>(conceptRepository.save(existingConcept), HttpStatus.OK);
        } else {
            // add new concept when concept not present in data
            try {
                if (conceptType == null) {
                    conceptType = "categorical";
                }
                ConceptModel newConcept = conceptRepository
                        .save(new ConceptModel(datasetId, name,
                                display, conceptType, conceptPath,
                                parentId));
                return new ResponseEntity<>(newConcept, HttpStatus.CREATED);
            } catch (Exception e) {
                return new ResponseEntity<>(e.getLocalizedMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
            }
        }

    }

    @DeleteMapping("/concept")
    public ResponseEntity<Object> deleteConcept(@RequestParam String conceptPath) {

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
            return new ResponseEntity<>(HttpStatus.OK);
        } else {
            return new ResponseEntity<>(conceptPath + "not found", HttpStatus.NOT_FOUND);
        }
    }

    // fetches all concepts still in the dictionary which arent currently in the
    // loader files
    @GetMapping("/concept/obsolete")
    public ResponseEntity<Object> getObsoleteConcepts(@RequestParam String datasetRef,
            @RequestBody String conceptNodeIds) {
        String[] inputArray = conceptNodeIds.split("\n");
        List<ConceptModel> validConcepts = new ArrayList<>();
        for (int i = 0; i < inputArray.length; i++) {
            try {
                System.out.println(inputArray[i]);
                System.out.println(Long.parseLong(inputArray[i]));
                validConcepts.add(conceptRepository.getReferenceById(Long.parseLong(inputArray[i])));
            } catch (NumberFormatException e) {
                return new ResponseEntity<>(
                        "Unable to parse conceptNodeIds as numeric. Please check your input and try again",
                        HttpStatus.BAD_REQUEST);
            }
        }
        Optional<DatasetModel> datasetData = datasetRepository.findByRef(datasetRef);
        Long datasetId;
        if (datasetData.isPresent()) {
            datasetId = datasetData.get().getDatasetId();
        } else {
            return new ResponseEntity<>("Dataset not found: " + datasetRef + ".", HttpStatus.NOT_FOUND);
        }
        List<ConceptModel> obsoleteConcepts = new ArrayList<>();
        conceptRepository.findByDatasetId(datasetId).forEach(concept -> {
            obsoleteConcepts.add(concept);
        });
        obsoleteConcepts.removeAll(validConcepts);
        return new ResponseEntity<>(obsoleteConcepts, HttpStatus.OK);
    }

    // removes all obsolete concepts from dictionary
    @DeleteMapping("/concept/obsolete")
    public ResponseEntity<Object> deleteObsoleteConcepts(@RequestParam String datasetRef,
            @RequestBody String conceptNodeIds) {
        String[] inputArray = conceptNodeIds.split("\n");
        List<ConceptModel> validConcepts = new ArrayList<>();
        for (int i = 0; i < inputArray.length; i++) {
            try {
                System.out.println(inputArray[i]);
                System.out.println(Long.parseLong(inputArray[i]));
                validConcepts.add(conceptRepository.getReferenceById(Long.parseLong(inputArray[i])));
            } catch (NumberFormatException e) {
                return new ResponseEntity<Object>(
                        "Unable to parse conceptNodeIds as numeric. Please check your input and try again",
                        HttpStatus.BAD_REQUEST);
            }
        }
        Optional<DatasetModel> datasetData = datasetRepository.findByRef(datasetRef);
        Long datasetId;
        if (datasetData.isPresent()) {
            datasetId = datasetData.get().getDatasetId();
        } else {
            return new ResponseEntity<>("Dataset not found: " + datasetRef + ".", HttpStatus.NOT_FOUND);
        }
        List<ConceptModel> obsoleteConcepts = new ArrayList<>();
        conceptRepository.findByDatasetId(datasetId).forEach(concept -> {
            obsoleteConcepts.add(concept);
        });
        obsoleteConcepts.removeAll(validConcepts);
        obsoleteConcepts.forEach(concept -> {
            // uses the local delete method in order to propery cascade
            deleteConcept(concept.getConceptPath());
        });
        return new ResponseEntity<>("removed " + obsoleteConcepts.size() + " obsolete concepts", HttpStatus.OK);
    }

    // Bulk insert/update from "ideal ingest" csv.
    // Expected CSV Header:
    // dataset_ref concept name display name concept_type concept_path
    // parent_concept_path values {addt metakeys}
    @Transactional
    @PutMapping("/concept/csv")
    public ResponseEntity<Object> updateConceptsFromCSV(@RequestParam String datasetRef, @RequestBody String input)
            throws IOException, CsvException {
        ConceptService service = new ConceptService(conceptRepository);
        Optional<DatasetModel> datasetData = datasetRepository.findByRef(datasetRef);
        Long datasetId;
        if (datasetData.isPresent()) {
            datasetId = datasetData.get().getDatasetId();
        } else {
            return new ResponseEntity<>("Dataset not found: " + datasetRef + ".", HttpStatus.NOT_FOUND);
        }
        List<String[]> concepts = new ArrayList<>();
        Map<String, Integer> headerMap = new HashMap<String, Integer>();
        List<String> metaColumnNames = new ArrayList<>();
        try (CSVReader reader = new CSVReader(new StringReader(input))) {
            ConceptService conceptService = new ConceptService(conceptRepository);
            String[] header = reader.readNext();
            headerMap = conceptService.buildCsvInputsHeaderMap(header);
            String[] coreConceptHeaders = { "dataset_ref", "name", "display", "concept_type", "concept_path",
                    "parent_concept_path" };
            if (!headerMap.keySet().containsAll(Arrays.asList(coreConceptHeaders))) {
                return new ResponseEntity<>(
                        "Headers in concept ingest file incorrect for " + datasetRef,
                        HttpStatus.BAD_REQUEST);
            } else {
                headerMap.keySet().forEach(k -> {
                    if (!Arrays.asList(coreConceptHeaders).contains(k)) {
                        metaColumnNames.add(k);
                    }
                });
            }
            concepts = reader.readAll();
            concepts.remove(header);
            reader.close();
        } catch (IOException | CsvException e) {
            return new ResponseEntity<>(
                    "Error reading ingestion csv for " + datasetRef + ". Error: \n" + e.getStackTrace(),
                    HttpStatus.BAD_REQUEST);
        }
        if (concepts.isEmpty()) {
            return new ResponseEntity<>(
                    "No csv records found in input file.",
                    HttpStatus.BAD_REQUEST);
        }

        int varcount = concepts.size();
        System.out.println("varcount: " + varcount);
        int conceptUpdateCount = 0;
        int metaUpdateCount = 0;
        List<ConceptModel> conceptModels = new ArrayList<>();
        // map of concept path -> key/value map
        Map<String, Map<String, String>> metaMap = new HashMap<>();
        Map<String, String> parentMap = new HashMap<>();
        for (int i = 0; i < varcount; i++) {
            String[] var = concepts.get(i);
            if (var.length < headerMap.size())
                continue;
            String name = var[headerMap.get("name")];
            String display = var[headerMap.get("display")];
            String conceptType = var[headerMap.get("concept_type")];
            String conceptPath = var[headerMap.get("concept_path")].replaceAll("'",
                    "''");
            String parentConceptPath = var[headerMap.get("parent_concept_path")].replaceAll("'",
                    "''");
            if (!parentConceptPath.isEmpty() && parentConceptPath != null)
                parentMap.put(conceptPath, parentConceptPath);
            ConceptModel newConceptModel;
            newConceptModel = new ConceptModel(conceptPath);
            newConceptModel.setConceptType(conceptType);
            newConceptModel.setDatasetId(datasetId);
            newConceptModel.setDisplay(display);
            newConceptModel.setName(name);
            conceptModels.add(newConceptModel);
            Map<String, String> metaVals = new HashMap<>();
            for (int j = 0; j < metaColumnNames.size(); j++) {
                String key = metaColumnNames.get(j);
                String value = var[headerMap.get(key)];
                if (!value.isBlank() && value != null) {
                    System.out.println("Value for key " + key + " and path " + conceptPath + " is " + value);
                    metaVals.put(key, value);
                } else
                    System.out.println("No value for key " + key + " and path " + conceptPath);
            }
            metaMap.put(conceptPath, metaVals);

            if ((i % BATCH_SIZE == 0 && i != 0) || i == varcount - 1) {

                // bulk update concept_node
                Query conceptQuery = entityManager.createNativeQuery(service.getUpsertConceptBatchQuery(conceptModels));

                conceptUpdateCount += conceptQuery.executeUpdate();

                // fetch updated concept node ids corresponding to concept paths
                Map<String, Long> pathIdMap = new HashMap<>();

                List<Object[]> refList = entityManager
                        .createNativeQuery(service.getIdsFromPathsQuery(metaMap.keySet())).getResultList();
                refList.forEach(entry -> {
                    String path = entry[0].toString().replaceAll("'", "''");
                    Long conceptId = Long.parseLong(entry[1].toString());
                    pathIdMap.put(path, conceptId);
                });

                // bulk update to add parent ids
                try {
                    Map<Long, String> idParentMap = parentMap.entrySet().stream()
                            .collect(
                                    Collectors.toMap(e -> (pathIdMap.get(e.getKey())),
                                            e -> (e.getValue())));
                    Query parentQuery = entityManager.createNativeQuery(service.getUpdateParentIdsQuery(idParentMap));
                    parentQuery.executeUpdate();
                } catch (NullPointerException e) {
                    return new ResponseEntity<>(
                            "Caught null pointer. \n Path map: \n"
                                    + StringUtils.collectionToDelimitedString(parentMap.entrySet(), "\n")
                                    + "\n\n Id map: \n" + StringUtils
                                            .collectionToDelimitedString(pathIdMap.entrySet(),
                                                    "\n"),
                            HttpStatus.BAD_REQUEST);

                }

                // bulk update concept_node_meta
                Map<Long, Map<String, String>> idMetaMap = metaMap.entrySet().stream()
                        .collect(Collectors.toMap(e -> (pathIdMap.get(e.getKey())), e -> (e.getValue())));

                List<ConceptMetadataModel> metaList = new ArrayList<ConceptMetadataModel>();
                idMetaMap.entrySet().forEach(entry -> {
                    Long id = entry.getKey();
                    System.out.println("Id: " + id);
                    Map<String, String> metaEntries = entry.getValue();
                    metaEntries.keySet().forEach(metaKey -> {
                        ConceptMetadataModel conceptMeta = new ConceptMetadataModel();
                        conceptMeta.setConceptNodeId(id);
                        conceptMeta.setKey(metaKey);
                        conceptMeta.setValue(metaEntries.get(metaKey).toString());
                        metaList.add(conceptMeta);
                    });
                });
                System.out.println("list size " + metaList.size());
                Query metaQuery = entityManager.createNativeQuery(service.getUpsertConceptMetaBatchQuery(metaList));
                metaUpdateCount += metaQuery.executeUpdate();

                // clear all dataobjects for next batch
                System.out.println("batch complete, resetting");
                conceptModels = new ArrayList<>();
                parentMap.clear();
                metaMap.clear();
                entityManager.flush();
            }
        }
        return new ResponseEntity<>("Successfully updated " + conceptUpdateCount + " concepts and " + metaUpdateCount
                + " concept meta entries from CSV. \n", HttpStatus.OK);
    }

    // Used for curated json from noncompliant studies
    /*
     * expected JSONArray element format
     * {
     * String dataset_ref
     * String name
     * String display
     * String concept_path
     * String parent_concept_path
     * JSONObject metadata {String description, JSONArray drs_uri, String or
     * JSONArray ~other metadata fields as needed~}
     * }
     * 
     * 
     */
    @Transactional
    @PutMapping("/concept/curated")
    public ResponseEntity<Object> updateConceptsFromJSON(@RequestParam String datasetRef, @RequestBody String input) {
        ConceptService service = new ConceptService(conceptRepository);
        Optional<DatasetModel> datasetData = datasetRepository.findByRef(datasetRef);
        Long datasetId;
        if (datasetData.isPresent()) {
            datasetId = datasetData.get().getDatasetId();
        } else {
            return new ResponseEntity<>("Dataset not found: " + datasetRef + ".", HttpStatus.NOT_FOUND);
        }
        JSONArray dictionaryJSON = new JSONArray(input);
        int varcount = dictionaryJSON.length();
        System.out.println("varcount: " + varcount);
        int conceptUpdateCount = 0;
        int metaUpdateCount = 0;
        final Map<String, JSONObject> conceptMetaMap = new HashMap<String, JSONObject>();
        List<ConceptModel> conceptModels = new ArrayList<>();

        for (int i = 0; i < varcount; i++) {
            JSONObject var = dictionaryJSON.getJSONObject(i);
            String name = var.getString("name").replaceAll("'", "''").replaceAll("\n", " ");

            String conceptType = var.getString("concept_type");
            if (!conceptType.equalsIgnoreCase("Categorical") && !conceptType.equalsIgnoreCase("Continuous")
                    && !conceptType.isEmpty()) {
                return new ResponseEntity<Object>(
                        "Bad input for concept_type (" + conceptType + ") for var " + name,
                        HttpStatus.BAD_REQUEST);
            }
            ;
            String conceptPath = var.getString("concept_path").replaceAll("'", "''").replaceAll("\n", " ");

            String display = name;

            try {
                display = var.getString("display").replaceAll("'", "''").replaceAll("\n", " ");
            } catch (JSONException e) {
                System.out.println("Using name as display");
            }
            ConceptModel newConceptModel;
            newConceptModel = new ConceptModel(conceptPath);
            newConceptModel.setConceptType(conceptType);
            newConceptModel.setDatasetId(datasetId);
            newConceptModel.setDisplay(display);
            newConceptModel.setName(name);
            conceptModels.add(newConceptModel);
            conceptMetaMap.put(conceptPath, var.getJSONObject("metadata"));
            if ((i % BATCH_SIZE == 0 && i != 0) || i == varcount - 1) {
                // bulk update concept_node

                Query conceptQuery = entityManager.createNativeQuery(service.getUpsertConceptBatchQuery(conceptModels));

                conceptUpdateCount += conceptQuery.executeUpdate();

                conceptMetaMap.forEach((key, value) -> {
                    System.out.println("Path key" + key);
                    System.out.println("meta value" + value);
                });
                // fetch updated concept node ids corresponding to concept paths
                Map<Long, JSONObject> idMetaMap = new HashMap<Long, JSONObject>();
                List<Object[]> refList = entityManager
                        .createNativeQuery(service.getIdsFromPathsQuery(conceptMetaMap.keySet())).getResultList();
                refList.forEach(entry -> {
                    Long id = Long.parseLong(entry[1].toString());
                    JSONObject metaJson = conceptMetaMap.get(entry[0].toString().replaceAll("'", "''"));
                    idMetaMap.put(id, metaJson);
                });

                // bulk update concept_node_meta
                List<ConceptMetadataModel> metaList = new ArrayList<ConceptMetadataModel>();
                idMetaMap.entrySet().forEach(entry -> {
                    Long id = entry.getKey();
                    System.out.println("Id:" + id);
                    JSONObject metaJson = entry.getValue();
                    metaJson.keySet().forEach(metaKey -> {
                        ConceptMetadataModel conceptMeta = new ConceptMetadataModel();
                        conceptMeta.setConceptNodeId(id);
                        conceptMeta.setKey(metaKey);
                        conceptMeta.setValue(metaJson.get(metaKey).toString());
                        metaList.add(conceptMeta);
                    });
                });
                Query metaQuery = entityManager.createNativeQuery(service.getUpsertConceptMetaBatchQuery(metaList));
                metaUpdateCount += metaQuery.executeUpdate();

                // clear all dataobjects for next batch
                conceptModels = new ArrayList<>();
                conceptMetaMap.clear();
                entityManager.flush();
            }
        }
        return new ResponseEntity<>("Successfully updated " + conceptUpdateCount + " concepts and " + metaUpdateCount
                + " concept meta entries from JSON. \n", HttpStatus.OK);
    }

    @GetMapping("/concept/metadata")
    public ResponseEntity<Object> getAllConceptMetadataModels(
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
    public ResponseEntity<Object> updateConceptMetadata(@RequestParam String conceptPath,
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
    public ResponseEntity<Object> updateStigvars(@RequestBody String conceptsToUpdate,
            @RequestParam String value) {
        String[] concepts = conceptsToUpdate.split("\n");
        System.out.println("concept size:" + concepts.length);
        int upsertCount = conceptMetadataRepository.updateStigvarsFromConceptPaths(concepts, value);
        System.out.println("count:" + upsertCount);
        return new ResponseEntity<>("Successfully updated stigvar status for " + upsertCount + " concepts. \n",
                HttpStatus.CREATED);
    }

    // gets all fields needed for stigvar identification
    @GetMapping("/concept/metadata/stigvars")
    public ResponseEntity<Object> getInfoForStigvarIdentification(@RequestParam String ref) {
        List<ConceptStigvarIdentificationModel> info = conceptMetadataRepository.getInfoForStigvars(ref);
        StringBuilder csvString = new StringBuilder();
        info.forEach(model -> {
            csvString.append(model.toString());
        });
        return new ResponseEntity<>(csvString.toString(), HttpStatus.OK);
    }

    // Specifically for mass value updates
    @PutMapping("/concept/metadata/values")
    public ResponseEntity<Object> updateManyValues(@RequestBody String valuesInput) {
        // TODO convert this to use getUpsertConceptMetaBatchQuery
        try {
            JSONArray valArray = new JSONArray(valuesInput);
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
    public ResponseEntity<Object> deleteConceptMetadata(@RequestParam Optional<String> conceptPath,
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

class SortbyId implements Comparator<ConceptModel> {

    @Override
    public int compare(ConceptModel o1, ConceptModel o2) {
        return (int) (o1.getConceptNodeId() - o2.getConceptNodeId());
    }
}
