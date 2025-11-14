package edu.harvard.dbmi.avillach.dictionaryetl.concept;

import com.opencsv.*;
import com.opencsv.exceptions.CsvException;
import edu.harvard.dbmi.avillach.dictionaryetl.Utility.CSVUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetConceptRepository;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import jakarta.transaction.Transactional;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;

@CrossOrigin(origins = "http://localhost:8081")
@RestController
@RequestMapping("/api")
public class ConceptController {
    private static final Logger log = LoggerFactory.getLogger(ConceptController.class);
    private final int BATCH_SIZE = 100;
    @Autowired
    ConceptRepository conceptRepository;
    @Autowired
    ConceptMetadataRepository conceptMetadataRepository;
    @Autowired
    DatasetRepository datasetRepository;
    @Autowired
    FacetConceptRepository facetConceptRepository;
    @Autowired
    ConceptService conceptService;
    String[] coreConceptHeaders = {"dataset_ref", "name", "display", "concept_type", "concept_path", "parent_concept_path"};
    @PersistenceContext
    private EntityManager entityManager;

    @GetMapping("/concept")
    public ResponseEntity<Object> getAllConceptModels(@RequestParam(required = false) String datasetRef) {
        try {
            List<ConceptModel> conceptModels = new ArrayList<>();

            if (datasetRef == null) {
                // get all concepts in dictionary
                conceptModels.addAll(conceptRepository.findAll());
            } else {
                // get all concepts in specific dataset
                Long datasetId = datasetRepository.findByRef(datasetRef).get().getDatasetId();
                conceptModels.addAll(conceptRepository.findByDatasetId(datasetId));

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
    public ResponseEntity<Object> updateConcept(
        @RequestParam String conceptPath, @RequestParam String datasetRef, @RequestParam String conceptType, @RequestParam String display,
        @RequestParam String name, @RequestParam String parentPath
    ) {

        Optional<ConceptModel> conceptData = conceptRepository.findByConceptPath(conceptPath);
        Optional<ConceptModel> parentData = conceptRepository.findByConceptPath(parentPath);
        Optional<DatasetModel> datasetData = datasetRepository.findByRef(datasetRef);
        Long datasetId;
        if (datasetData.isPresent()) {
            datasetId = datasetData.get().getDatasetId();
        } else {
            return new ResponseEntity<>(
                "Dataset not found: " + datasetRef + ". Failed to create/update concept " + conceptPath,
                HttpStatus.NOT_FOUND
            );
        }

        Long parentId;
        parentId = parentData.map(ConceptModel::getConceptNodeId).orElse(null);
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
                ConceptModel newConcept =
                    conceptRepository.save(new ConceptModel(datasetId, name, display, conceptType, conceptPath, parentId));
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

            facetConceptRepository.deleteAll(facetConceptRepository.findByConceptNodeId(conceptId).get());
            conceptMetadataRepository.deleteAll(conceptMetadataRepository.findByConceptNodeId(conceptId));
            conceptRepository.delete(conceptData.get());
            return new ResponseEntity<>(HttpStatus.OK);
        } else {
            return new ResponseEntity<>(conceptPath + "not found", HttpStatus.NOT_FOUND);
        }
    }

    // fetches all concepts still in the dictionary which arent currently in the
    // loader files
    @GetMapping("/concept/obsolete")
    public ResponseEntity<Object> getObsoleteConcepts(@RequestParam String datasetRef, @RequestBody String conceptNodeIds) {
        String[] inputArray = conceptNodeIds.split("\n");
        List<ConceptModel> validConcepts = new ArrayList<>();
        for (String s : inputArray) {
            try {
                validConcepts.add(conceptRepository.getReferenceById(Long.parseLong(s)));
            } catch (NumberFormatException e) {
                return new ResponseEntity<>(
                    "Unable to parse conceptNodeIds as numeric. Please check your input and try again",
                    HttpStatus.BAD_REQUEST
                );
            }
        }
        Optional<DatasetModel> datasetData = datasetRepository.findByRef(datasetRef);
        Long datasetId;
        if (datasetData.isPresent()) {
            datasetId = datasetData.get().getDatasetId();
        } else {
            return new ResponseEntity<>("Dataset not found: " + datasetRef + ".", HttpStatus.NOT_FOUND);
        }
        List<ConceptModel> obsoleteConcepts = new ArrayList<>(conceptRepository.findByDatasetId(datasetId));
        obsoleteConcepts.removeAll(validConcepts);
        return new ResponseEntity<>(obsoleteConcepts, HttpStatus.OK);
    }

    // removes all obsolete concepts from dictionary
    @DeleteMapping("/concept/obsolete")
    public ResponseEntity<Object> deleteObsoleteConcepts(@RequestParam String datasetRef, @RequestBody String conceptNodeIds) {
        String[] inputArray = conceptNodeIds.split("\n");
        List<ConceptModel> validConcepts = new ArrayList<>();
        for (String s : inputArray) {
            try {
                validConcepts.add(conceptRepository.getReferenceById(Long.parseLong(s)));
            } catch (NumberFormatException e) {
                return new ResponseEntity<>(
                    "Unable to parse conceptNodeIds as numeric. Please check your input and try again",
                    HttpStatus.BAD_REQUEST
                );
            }
        }
        Optional<DatasetModel> datasetData = datasetRepository.findByRef(datasetRef);
        Long datasetId;
        if (datasetData.isPresent()) {
            datasetId = datasetData.get().getDatasetId();
        } else {
            return new ResponseEntity<>("Dataset not found: " + datasetRef + ".", HttpStatus.NOT_FOUND);
        }
        List<ConceptModel> obsoleteConcepts = new ArrayList<>(conceptRepository.findByDatasetId(datasetId));
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
    public ResponseEntity<Object> updateConceptsFromCSV(@RequestBody String input) {

        List<String[]> concepts;
        Map<String, Integer> headerMap;
        List<String> metaColumnNames;
        RFC4180Parser csvParser = new RFC4180Parser();
        try (CSVReader reader = new CSVReaderBuilder(new StringReader(input)).withCSVParser(csvParser).build()) {
            String[] header = reader.readNext();
            headerMap = CSVUtility.buildCsvInputsHeaderMap(header);
            metaColumnNames = CSVUtility.getExtraColumns(coreConceptHeaders, headerMap);
            if (metaColumnNames == null) {
                return new ResponseEntity<>(
                    "ERROR: Input headers are not as expected \n"
                        + "Verify that the following headers are present in the input csv file: " + String.join(", ", coreConceptHeaders),
                    HttpStatus.BAD_REQUEST
                );
            }

            concepts = reader.readAll();
            concepts.remove(header);
        } catch (IOException | CsvException e) {
            log.error(e.toString());
            log.error(Arrays.toString(e.getStackTrace()));
            return new ResponseEntity<>(
                "Error reading ingestion csv. See logs for details.",
                HttpStatus.BAD_REQUEST
            );
        }
        if (concepts.isEmpty()) {
            return new ResponseEntity<>("No csv records found in input file:", HttpStatus.BAD_REQUEST);
        }
        String updateConceptsResponse = conceptService.updateConceptsFromCSV(concepts, headerMap, metaColumnNames, BATCH_SIZE);
        if (updateConceptsResponse.startsWith("Success")) {
            return new ResponseEntity<>(updateConceptsResponse, HttpStatus.OK);
        }
        return new ResponseEntity<>(
            "An error occurred while trying to update concepts from CSV. Check logs for further information",
            HttpStatus.INTERNAL_SERVER_ERROR
        );


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
        ConceptService service = new ConceptService(conceptRepository, datasetRepository);
        Optional<DatasetModel> datasetData = datasetRepository.findByRef(datasetRef);
        Long datasetId;
        if (datasetData.isPresent()) {
            datasetId = datasetData.get().getDatasetId();
        } else {
            return new ResponseEntity<>("Dataset not found: " + datasetRef + ".", HttpStatus.NOT_FOUND);
        }
        JSONArray dictionaryJSON = new JSONArray(input);
        int varcount = dictionaryJSON.length();
        log.info("varcount: " + varcount);
        int conceptUpdateCount = 0;
        int metaUpdateCount = 0;
        final Map<String, JSONObject> conceptMetaMap = new HashMap<>();
        List<ConceptModel> conceptModels = new ArrayList<>();

        for (int i = 0; i < varcount; i++) {
            JSONObject var = dictionaryJSON.getJSONObject(i);
            String name = var.getString("name").replaceAll("'", "''").replaceAll("\n", " ");

            String conceptType = "Categorical";

            String conceptPath = var.getString("concept_path").replaceAll("'", "''").replaceAll("\n", " ");

            String display = name;

            try {
                display = var.getString("display").replaceAll("'", "''").replaceAll("\n", " ");
            } catch (JSONException e) {
                log.info("Using name as display");
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
                    log.info("Path key" + key);
                    log.info("meta value" + value);
                });
                // fetch updated concept node ids corresponding to concept paths
                Map<Long, JSONObject> idMetaMap = new HashMap<>();
                List<Object[]> refList =
                    entityManager.createNativeQuery(service.getIdsFromPathsQuery(conceptMetaMap.keySet())).getResultList();
                refList.forEach(entry -> {
                    Long id = Long.parseLong(entry[1].toString());
                    JSONObject metaJson = conceptMetaMap.get(entry[0].toString().replaceAll("'", "''"));
                    idMetaMap.put(id, metaJson);
                });

                // bulk update concept_node_meta
                List<ConceptMetadataModel> metaList = new ArrayList<>();
                idMetaMap.forEach((id, metaJson) -> {
                    log.info("Id:{}", id);
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
        return new ResponseEntity<>(
            "Successfully updated " + conceptUpdateCount + " concepts and " + metaUpdateCount + " concept meta entries from JSON. \n",
            HttpStatus.OK
        );
    }

    @GetMapping("/concept/metadata")
    public ResponseEntity<Object> getAllConceptMetadataModels(
        @RequestParam Optional<String> conceptPath
    ) {
        try {
            List<ConceptMetadataModel> conceptMetadataModels = new ArrayList<>();

            if (conceptPath.isEmpty()) {
                // get all conceptMetadatas in dictionary
                log.info("Hitting conceptMetadata null");
                conceptMetadataModels.addAll(conceptMetadataRepository.findAll());
            } else {
                log.info("Hitting conceptMetadata");
                Long conceptId = conceptRepository.findByConceptPath(conceptPath.get()).get().getConceptNodeId();
                conceptMetadataModels.addAll(conceptMetadataRepository.findByConceptNodeId(conceptId));

            }
            if (conceptMetadataModels.isEmpty()) {
                return new ResponseEntity<>(HttpStatus.NO_CONTENT);
            }
            return new ResponseEntity<>(conceptMetadataModels, HttpStatus.OK);
        } catch (Exception e) {
            log.info(e.getLocalizedMessage());
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PutMapping("/concept/metadata")
    public ResponseEntity<Object> updateConceptMetadata(
        @RequestParam String conceptPath, @RequestParam String key,
        @RequestBody String values
    ) {
        Optional<ConceptModel> concept = conceptRepository.findByConceptPath(conceptPath);
        Long conceptNodeId;
        if (concept.isPresent()) {
            conceptNodeId = concept.get().getConceptNodeId();
        } else {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
        Optional<ConceptMetadataModel> conceptMetadataData = conceptMetadataRepository.findByConceptNodeIdAndKey(conceptNodeId, key);

        try {
            if (conceptMetadataData.isPresent()) {
                // update already existing conceptMetadata
                ConceptMetadataModel existingConceptMetadata = conceptMetadataData.get();
                existingConceptMetadata.setValue(values);
                return new ResponseEntity<>(conceptMetadataRepository.save(existingConceptMetadata), HttpStatus.OK);
            } else {
                // add new conceptMetadata when conceptMetadata not present in data
                try {
                    ConceptMetadataModel newConceptMetadata =
                        conceptMetadataRepository.save(new ConceptMetadataModel(conceptNodeId, key, values));
                    return new ResponseEntity<>(newConceptMetadata, HttpStatus.CREATED);
                } catch (Exception e) {
                    log.info(e.getLocalizedMessage());
                    return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
                }
            }
        } catch (Exception e) {
            log.info(e.getLocalizedMessage());
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }

    }

    // Specifically for stigvar updates
    @PutMapping("/concept/metadata/stigvars")
    public ResponseEntity<Object> updateStigvars(@RequestBody String conceptsToUpdate, @RequestParam String value) {
        try {
            CSVParser csvParser = new CSVParserBuilder().withEscapeChar('φ').build();
            List<String> conceptList =
                new CSVReaderBuilder(new StringReader(conceptsToUpdate)).withCSVParser(csvParser).build().readAll().stream()
                    .map(line -> line[0]).toList();

            String[] concepts = conceptList.toArray(new String[0]);
            int upsertCount = conceptMetadataRepository.updateStigvarsFromConceptPaths(concepts, value);
            log.info("count:" + upsertCount);
            return new ResponseEntity<>("Successfully updated stigvar status for " + upsertCount + " concepts. \n", HttpStatus.CREATED);

        } catch (IOException | CsvException e) {
            return new ResponseEntity<>(
                "Error reading conceptstoupdate csv. Error: \n" + Arrays.toString(e.getStackTrace()),
                HttpStatus.BAD_REQUEST
            );
        }

    }

    // gets all fields needed for stigvar identification
    @GetMapping("/concept/metadata/stigvars")
    public ResponseEntity<Object> getInfoForStigvarIdentification(@RequestParam String ref) {
        List<ConceptStigvarIdentificationModel> info = conceptMetadataRepository.getInfoForStigvars(ref);
        StringBuilder csvString = new StringBuilder();
        info.forEach(model -> csvString.append(model.toString()));
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
    public ResponseEntity<Object> deleteConceptMetadata(@RequestParam Optional<String> conceptPath, @RequestParam String key) {
        if (conceptPath.isPresent()) {
            Long conceptId = conceptRepository.findByConceptPath(conceptPath.get()).get().getConceptNodeId();
            Optional<ConceptMetadataModel> conceptMetadataData = conceptMetadataRepository.findByConceptNodeIdAndKey(conceptId, key);

            if (conceptMetadataData.isPresent()) {
                conceptMetadataRepository.delete(conceptMetadataData.get());
                return new ResponseEntity<>(HttpStatus.NO_CONTENT);
            } else {
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            }
        } else if (!key.isEmpty()) {
            conceptMetadataRepository.deleteAll(conceptMetadataRepository.findByKey(key));
        }

        return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);

    }
}
