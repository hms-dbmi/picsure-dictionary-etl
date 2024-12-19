package edu.harvard.dbmi.avillach.dictionaryetl.hydration;

import edu.harvard.dbmi.avillach.dictionaryetl.Utility.DatabaseCleanupUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.concurrent.locks.ReentrantLock;

@CrossOrigin(origins = "http://localhost:8081")
@Controller
@RequestMapping("hydrate")
public class HydrateDatabaseController {

    private final static Logger log = LoggerFactory.getLogger(HydrateDatabaseController.class);

    private final HydrateDatabaseService hydrateDatabaseService;
    private final FacetService facetService;
    private final DatabaseCleanupUtility databaseCleanupUtility;

    private final ReentrantLock reentrantLock = new ReentrantLock();

    @Autowired
    public HydrateDatabaseController(HydrateDatabaseService hydrateDatabaseService, FacetService facetService, DatabaseCleanupUtility databaseCleanupUtility) {
        this.hydrateDatabaseService = hydrateDatabaseService;
        this.facetService = facetService;
        this.databaseCleanupUtility = databaseCleanupUtility;
    }

    /**
     *
     * @param datasetName
     * @param csvPath
     * @param errorDirectory
     * @param includeDefaultFacets
     * @return
     */
    @GetMapping(value = "/initialize")
    public ResponseEntity<String> initialDatabaseHydration(
            @RequestParam(required = false) String datasetName,
            @RequestParam(required = false) String csvPath,
            @RequestParam(required = false) String errorDirectory,
            @RequestParam(required = false, defaultValue = "true") boolean includeDefaultFacets
            ) {
        log.info("initialDatabaseHydration __ datasetName: {}, csvPath: {}, errorDictionary: {}, includeDefaultFacets: {}",
                datasetName,
                csvPath,
                errorDirectory,
                includeDefaultFacets);

        String response;
        if (this.reentrantLock.tryLock()) {
            try {
                databaseCleanupUtility.truncateTablesAllTables();
                response = this.hydrateDatabaseService.processColumnMetaCSV(csvPath, datasetName, errorDirectory);
                if (includeDefaultFacets) {
                    this.facetService.createDefaultFacets();
                }
            } finally {
                reentrantLock.unlock();
            }
        } else {
            response = "This task is already running. Skipping execution.";
        }

        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}