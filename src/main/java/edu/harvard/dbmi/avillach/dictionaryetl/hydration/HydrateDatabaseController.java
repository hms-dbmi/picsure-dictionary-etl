package edu.harvard.dbmi.avillach.dictionaryetl.hydration;

import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller(value = "/hydrate")
public class HydrateDatabaseController {

    private final HydrateDatabaseService hydrateDatabaseService;
    private final FacetService facetService;

    @Autowired
    public HydrateDatabaseController(HydrateDatabaseService hydrateDatabaseService, FacetService facetService) {
        this.hydrateDatabaseService = hydrateDatabaseService;
        this.facetService = facetService;
    }

    @GetMapping(value = "/initialize")
    public ResponseEntity<String> initialDatabaseHydration(
            @RequestParam(required = false) String datasetName,
            @RequestParam(required = false) String csvPath,
            @RequestParam(required = false) String errorDirectory,
            @RequestParam(required = false) boolean includeDefaultFacets
            ) {
        this.hydrateDatabaseService.processColumnMetaCSV(csvPath, datasetName, errorDirectory);
        if (includeDefaultFacets) {
            this.facetService.createDefaultFacets();
        }

        return new ResponseEntity<>(HttpStatus.OK);
    }
}
