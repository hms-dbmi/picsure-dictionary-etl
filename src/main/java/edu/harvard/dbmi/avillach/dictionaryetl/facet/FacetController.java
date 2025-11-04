package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import java.util.*;


import edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.*;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.model.FacetConceptModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.model.FacetMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.model.FacetModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import jakarta.transaction.Transactional;

@CrossOrigin(origins = "http://localhost:8081")
@RestController
@RequestMapping("/api")
public class FacetController {

    private final FacetService facetService;
    private final FacetConceptService facetConceptService;
    private final FacetLoaderService facetLoaderService;
    private final RecoverMonthsFacetGeneratorService generator;

    @Autowired
    public FacetController(FacetService facetService,
                           FacetConceptService facetConceptService,
                           FacetLoaderService facetLoaderService,
                           RecoverMonthsFacetGeneratorService generator) {
        this.facetService = facetService;
        this.facetConceptService = facetConceptService;
        this.facetLoaderService = facetLoaderService;
        this.generator = generator;
    }

    @GetMapping("/facet")
    public ResponseEntity<List<FacetModel>> getAllFacetModels() {
        return facetService.listAllResponse();
    }

    @PutMapping("/facet")
    public ResponseEntity<FacetModel> updateFacet(@RequestParam String categoryName,
                                                  @RequestParam String name, @RequestParam String display, @RequestParam String desc,
                                                  @RequestParam String parentName) {
        return facetService.updateFacet(categoryName, name, display, desc, parentName);
    }

    @DeleteMapping("/facet")
    public ResponseEntity<FacetModel> deleteFacet(@RequestParam String name) {
        return facetService.deleteFacetByName(name);
    }

    @GetMapping("/facet/concept")
    public ResponseEntity<List<FacetConceptModel>> getAllFacetConceptModels() {
        return facetConceptService.listAllResponse();
    }

    @PutMapping("/facet/concept")
    public ResponseEntity<FacetConceptModel> addConceptFacet(@RequestParam String facetName,
                                                             @RequestParam String conceptPath) {
        return facetConceptService.addConceptFacet(facetName, conceptPath);
    }

    //TODO - update this endpoint to be /facet/refresh/bdc - calls need to be updated in bdc pipeline jobs
    @Transactional
    @PutMapping("/facet/general/refresh/")
    public ResponseEntity<String> refreshBDCFacets() {
        return facetService.refreshBDCFacets();
    }

    //TODO extract this method to service and add test
    @Transactional
    @PutMapping("/facet/refresh/data_type")
    public ResponseEntity<String> refreshDataTypeFacet() {
        return facetService.refreshDataTypeFacet();
    }

    /*Ingest facets from "Ideal Ingest" csv format
    Expected CSV headers:
    facet_category facet_name(unique) display_name description parent_name

    */
    @Transactional
    @PutMapping("/facet/csv")
    public ResponseEntity<String> updateFacetsFromCSVs(@RequestBody String input) {
        return facetService.updateFacetsFromCSVs(input);
    }

    /*Ingest facet/concept mappings from "Ideal Ingest" csv format
    Expected CSV headers:
    concept_path	[1+ columns of facetCategoryName header]
    values are concept_path, then the exact facet in each facetCategory that the concept path belongs to.
        If that facet has a parent facet, the concept should also be assigned to the parent
    */
    @Transactional
    @PutMapping("/facet/concept/csv")
    public ResponseEntity<String> updateFacetConceptMappingsFromCSVs(@RequestBody String input) {
        return facetConceptService.updateFacetConceptMappingsFromCSVs(input);
    }


    @PutMapping("/facet/dataset")
    public ResponseEntity<FacetConceptModel> addFacetToFullDataset(@RequestParam String facetName,
                                                                   @RequestParam String datasetRef) {
        return facetService.addFacetToFullDataset(facetName, datasetRef);
    }

    @DeleteMapping("/facet/concept")
    public ResponseEntity<FacetConceptModel> deleteConceptFacet(@RequestParam String facetName,
                                                                @RequestParam String conceptPath) {
        return facetConceptService.deleteConceptFacet(facetName, conceptPath);
    }

    @GetMapping("/facet/metadata")
    public ResponseEntity<List<FacetMetadataModel>> getAllFacetMetadataModels(
            @RequestParam Optional<String> name) {
        return facetService.getAllFacetMetadataModels(name);
    }

    @PutMapping("/facet/metadata")
    public ResponseEntity<FacetMetadataModel> updateFacetMetadata(@RequestParam String name,
                                                                  @RequestParam String key, @RequestBody String values) {
        return facetService.updateFacetMetadata(name, key, values);
    }

    @DeleteMapping("/facet/metadata")
    public ResponseEntity<FacetMetadataModel> deleteFacetMetadata(@RequestParam String name,
                                                                  @RequestParam String key) {
        return facetService.deleteFacetMetadata(name, key);
    }

    @PostMapping("/facet/loader/load")
    public ResponseEntity<Result> load(@RequestBody List<FacetCategoryWrapper> payload) {
        Result result = facetLoaderService.load(payload);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @PostMapping("/facet/loader/clear")
    public ResponseEntity<ClearResult> clear(@RequestBody FacetClearRequest request) {
        ClearResult result = facetLoaderService.clear(request);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @PostMapping("/facet/loader/recover/months/generate")
    public ResponseEntity<GenerateRecoverMonthsResponse> generate(@RequestBody GenerateRecoverMonthsRequest request) {
        GenerateRecoverMonthsResponse resp = generator.generate(request);
        return new ResponseEntity<>(resp, HttpStatus.OK);
    }

}
