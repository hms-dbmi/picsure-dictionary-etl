package edu.harvard.dbmi.avillach.dictionaryetl.facet;

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

import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryRepository;

@CrossOrigin(origins = "http://localhost:8081")
@RestController
@RequestMapping("/api")
public class FacetController {
    @Autowired
    FacetRepository facetRepository;

    @Autowired
    FacetConceptRepository facetConceptRepository;

    @Autowired
    FacetCategoryRepository facetCategoryRepository;

    @Autowired
    FacetMetadataRepository facetMetadataRepository;

    @Autowired
    ConceptRepository conceptRepository;

    @Autowired
    DatasetRepository datasetRepository;

    @GetMapping("/facet")
    public ResponseEntity<List<FacetModel>> getAllFacetModels() {
        try {
            List<FacetModel> facetModels = new ArrayList<FacetModel>();
            facetRepository.findAll().forEach(facetModels::add);

            if (facetModels.isEmpty()) {
                return new ResponseEntity<>(HttpStatus.NO_CONTENT);
            }
            return new ResponseEntity<>(facetModels, HttpStatus.OK);
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PutMapping("/facet")
    public ResponseEntity<FacetModel> updateFacet(@RequestParam String categoryName,
            @RequestParam String name, @RequestParam String display, @RequestParam String desc,
            @RequestParam String parentName) {

        Optional<FacetCategoryModel> facetCategoryModel = facetCategoryRepository.findByName(categoryName);
        Long categoryId;
        if (facetCategoryModel.isPresent()) {
            categoryId = facetCategoryModel.get().getFacetCategoryId();
        } else {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
        Optional<FacetModel> parentModel = facetRepository.findByName(parentName);
        Long parentId;
        if (parentModel.isPresent()) {
            parentId = parentModel.get().getFacetId();
        } else {
            parentId = null;
        }
        Optional<FacetModel> facetData = facetRepository.findByName(name);

        if (facetData.isPresent()) {
            // update already existing facet
            FacetModel existingFacet = facetData.get();
            existingFacet.setFacetCategoryId(categoryId);
            existingFacet.setName(name);
            existingFacet.setDisplay(display);
            existingFacet.setDescription(desc);
            existingFacet.setParentId(parentId);
            return new ResponseEntity<>(facetRepository.save(existingFacet), HttpStatus.OK);
        } else {
            // add new facet when facet not present in data
            try {
                FacetModel newFacet = facetRepository
                        .save(new FacetModel(categoryId,
                                name, display, desc, parentId));
                return new ResponseEntity<>(newFacet, HttpStatus.CREATED);
            } catch (Exception e) {
                System.out.println(e.getLocalizedMessage());
                return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
            }
        }

    }

    @DeleteMapping("/facet")
    public ResponseEntity<FacetModel> deleteFacet(@RequestParam String name) {

        Optional<FacetModel> facetData = facetRepository.findByName(name);

        if (facetData.isPresent()) {
            Long facetId = facetData.get().getFacetId();
            facetConceptRepository.findByFacetId(facetId).get().forEach(fc -> {
                facetConceptRepository.delete(fc);
            });
            facetRepository.delete(facetData.get());
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        } else {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @GetMapping("/facet/concept")
    public ResponseEntity<List<FacetConceptModel>> getAllFacetConceptModels(Optional<String> facetName) {
        try {

            List<FacetConceptModel> facetConceptModels = new ArrayList<FacetConceptModel>();
            facetConceptRepository.findAll().forEach(facetConceptModels::add);

            if (facetConceptModels.isEmpty()) {
                return new ResponseEntity<>(HttpStatus.NO_CONTENT);
            }
            return new ResponseEntity<>(facetConceptModels, HttpStatus.OK);
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PutMapping("/facet/concept")
    public ResponseEntity<FacetConceptModel> addConceptFacet(@RequestParam String facetName,
            @RequestParam String conceptPath) {
        // add new facet concept relationship

        try {

            Optional<FacetModel> facet = facetRepository.findByName(facetName);
            Optional<ConceptModel> concept = conceptRepository.findByConceptPath(conceptPath);
            Boolean hasParent = false;

            Long facetId;
            Long conceptNodeId;
            Long parentFacetId;
            if (facet.isPresent() && concept.isPresent()) {
                facetId = facet.get().getFacetId();
                conceptNodeId = concept.get().getConceptNodeId();
                if (facet.get().getParentId() != null) {
                    hasParent = true;
                }

            } else {
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            }
            Optional<FacetConceptModel> conceptFacet = facetConceptRepository.findByFacetIdAndConceptNodeId(facetId,
                    conceptNodeId);
            if (!conceptFacet.isPresent()) {
                FacetConceptModel newConceptFacet = facetConceptRepository
                        .save(new FacetConceptModel(facetId,
                                conceptNodeId));

                // check if parent facet a) exists and b) has the concept relationship
                // established. If not, create relationship between parent facet and concept
                if (hasParent) {
                    Optional<FacetModel> parentFacet = facetRepository.findById(facet.get().getParentId());
                    parentFacetId = parentFacet.get().getFacetId();
                    Optional<FacetConceptModel> parentConceptFacet = facetConceptRepository
                            .findByFacetIdAndConceptNodeId(parentFacetId,
                                    conceptNodeId);
                    if (!parentConceptFacet.isPresent()) {
                        FacetConceptModel newConceptParentFacet = facetConceptRepository
                                .save(new FacetConceptModel(parentFacetId,
                                        conceptNodeId));
                    }
                }
                return new ResponseEntity<>(newConceptFacet, HttpStatus.CREATED);
            } else
                return new ResponseEntity<>(conceptFacet.get(), HttpStatus.CREATED);

        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }

    }

    @PutMapping("/facet/dataset")
    public ResponseEntity<FacetConceptModel> addDatasetFacet(@RequestParam String facetName,
            @RequestParam String datasetRef) {
        // add relationship between facet and all concepts in dataset

        try {

            Optional<FacetModel> facet = facetRepository.findByName(facetName);
            Optional<DatasetModel> dataset = datasetRepository.findByRef(datasetRef);

            Long datasetID;
            Long facetId;
            if (dataset.isPresent() && facet.isPresent()) {
                datasetID = dataset.get().getDatasetId();
                facetId = facet.get().getFacetId();

            } else {
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            }
            List<ConceptModel> concepts = conceptRepository.findByDatasetId(datasetID);

            concepts.forEach(concept -> {
                Long conceptNodeId = concept.getConceptNodeId();
                Optional<FacetConceptModel> conceptFacet = facetConceptRepository.findByFacetIdAndConceptNodeId(facetId,
                        conceptNodeId);
                if (!conceptFacet.isPresent()) {
                    FacetConceptModel newConceptFacet = facetConceptRepository
                            .save(new FacetConceptModel(facetId,
                                    conceptNodeId));

                    // check if parent facet a) exists and b) has the concept relationship
                    // established. If not, create relationship between parent facet and concept
                    Boolean hasParent = false;
                    if (facet.get().getParentId() != null) {
                        hasParent = true;
                    }
                    if (hasParent) {
                        Optional<FacetModel> parentFacet = facetRepository.findById(facet.get().getParentId());
                        Long parentFacetId = parentFacet.get().getFacetId();
                        Optional<FacetConceptModel> parentConceptFacet = facetConceptRepository
                                .findByFacetIdAndConceptNodeId(parentFacetId,
                                        conceptNodeId);
                        if (!parentConceptFacet.isPresent()) {
                            FacetConceptModel newConceptParentFacet = facetConceptRepository
                                    .save(new FacetConceptModel(parentFacetId,
                                            conceptNodeId));
                        }
                    }
                }

            });
            return new ResponseEntity<>(null, HttpStatus.OK);

        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }

    }

    @DeleteMapping("/facet/concept")
    public ResponseEntity<FacetConceptModel> deleteConceptFacet(@RequestParam String facetName,
            @RequestParam String conceptPath) {
        Optional<FacetModel> facet = facetRepository.findByName(facetName);
        Optional<ConceptModel> concept = conceptRepository.findByConceptPath(conceptPath);
        Long facetId;
        Long conceptNodeId;
        if (facet.isPresent() && concept.isPresent()) {
            facetId = facet.get().getFacetId();
            conceptNodeId = concept.get().getConceptNodeId();
        } else {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
        Optional<FacetConceptModel> facetConceptData = facetConceptRepository.findByFacetIdAndConceptNodeId(facetId,
                conceptNodeId);

        if (facetConceptData.isPresent()) {
            facetConceptRepository.delete(facetConceptData.get());
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        } else {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @GetMapping("/facet/metadata")
    public ResponseEntity<List<FacetMetadataModel>> getAllFacetMetadataModels(
            @RequestParam Optional<String> name) {
        try {
            List<FacetMetadataModel> facetMetadataModels = new ArrayList<FacetMetadataModel>();

            if (name == null || !name.isPresent()) {
                // get all facetMetadatas in dictionary
                System.out.println("Hitting facetMetadata null");
                facetMetadataRepository.findAll().forEach(facetMetadataModels::add);
            } else {
                System.out.println("Hitting facetMetadata");
                Long facetId = facetRepository.findByName(name.get()).get().getFacetId();
                facetMetadataRepository.findByFacetId(facetId).forEach(facetMetadataModels::add);

            }
            if (facetMetadataModels.isEmpty()) {
                return new ResponseEntity<>(HttpStatus.NO_CONTENT);
            }
            return new ResponseEntity<>(facetMetadataModels, HttpStatus.OK);
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PutMapping("/facet/metadata")
    public ResponseEntity<FacetMetadataModel> updateFacetMetadata(@RequestParam String name,
            @RequestParam String key, @RequestBody String values) {
        Optional<FacetModel> facet = facetRepository.findByName(name);
        Long facetId;
        if (facet.isPresent()) {
            facetId = facet.get().getFacetId();
        } else {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
        Optional<FacetMetadataModel> facetMetadataData = facetMetadataRepository
                .findByFacetIdAndKey(facetId, key);

        try {
            if (facetMetadataData.isPresent()) {
                // update already existing facetMetadata
                FacetMetadataModel existingFacetMetadata = facetMetadataData.get();
                existingFacetMetadata.setValue(values);
                return new ResponseEntity<>(facetMetadataRepository.save(existingFacetMetadata), HttpStatus.OK);
            } else {
                // add new facetMetadata when facetMetadata not present in data
                try {
                    FacetMetadataModel newFacetMetadata = facetMetadataRepository
                            .save(new FacetMetadataModel(facetId, key,
                                    values));
                    return new ResponseEntity<>(newFacetMetadata, HttpStatus.CREATED);
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

    @DeleteMapping("/facet/metadata")
    public ResponseEntity<FacetMetadataModel> deleteFacetMetadata(@RequestParam String name,
            @RequestParam String key) {

        Long facetId = facetRepository.findByName(name).get().getFacetId();
        Optional<FacetMetadataModel> facetMetadataData = facetMetadataRepository
                .findByFacetIdAndKey(facetId, key);

        if (facetMetadataData.isPresent()) {
            facetMetadataRepository.delete(facetMetadataData.get());
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        } else {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }
}
