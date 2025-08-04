package edu.harvard.dbmi.avillach.dictionaryetl.loading;

import edu.harvard.dbmi.avillach.dictionaryetl.Utility.DatabaseCleanupUtility;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetFacetRefreshService;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.locks.ReentrantLock;

@CrossOrigin(origins = "http://localhost:8081")
@Controller
@RequestMapping("load")
public class DictionaryLoaderController {

    private final static Logger log = LoggerFactory.getLogger(DictionaryLoaderController.class);

    private final DictionaryLoaderService dictionaryLoaderService;
    private final FacetService facetService;
    private final DatabaseCleanupUtility databaseCleanupUtility;
    private final ReentrantLock reentrantLock = new ReentrantLock();
    private final DatasetFacetRefreshService datasetFacetRefreshService;

    @Autowired
    public DictionaryLoaderController(DictionaryLoaderService dictionaryLoaderService, FacetService facetService, DatabaseCleanupUtility databaseCleanupUtility, DatasetFacetRefreshService datasetFacetRefreshService) {
        this.dictionaryLoaderService = dictionaryLoaderService;
        this.facetService = facetService;
        this.databaseCleanupUtility = databaseCleanupUtility;
        this.datasetFacetRefreshService = datasetFacetRefreshService;
    }

    @GetMapping("/clear")
    public ResponseEntity<String> clearDatabase() {
        if (this.reentrantLock.tryLock()) {
            log.info("Clearing all tables");
            databaseCleanupUtility.truncateTablesAllTables();
            reentrantLock.unlock();
            return ResponseEntity.ok().build();
        } else {
            log.info("ETL already running");
            return ResponseEntity.status(423).body("ETL locked, something else is running");
        }
    }

    /**
     * This method is responsible for loading all data from a provided columnMeta.csv into the data dictionary
     * SQL database. It will return a response based on the results. If there are errors during the transform process
     * the rows will be printed to a separate error file.
     *
     * @param request InitializeRequest
     * @return Returns a string response based on the results of attempting to load the initial data.
     */
    @PostMapping(value = "/initialize")
    public ResponseEntity<String> initialDatabaseHydration(
            @RequestBody InitializeRequest request
    ) {
        log.info("initialDatabaseHydration __ csvPath: {}, errorDictionary: {}, includeDefaultFacets: {}, clearDatabase: {}",
                request.csvPath(),
                request.errorDirectory(),
                request.includeDefaultFacets(),
                request.clearDatabase());

        boolean includeDefaultFacets = (request.includeDefaultFacets() != null) ? request.includeDefaultFacets() : true;
        boolean clearDatabase = (request.clearDatabase() != null) ? request.clearDatabase() : false;
        String response;
        if (this.reentrantLock.tryLock()) {
            try {
                if (clearDatabase) {
                    databaseCleanupUtility.truncateTablesAllTables();
                }
                response = this.dictionaryLoaderService.processColumnMetaCSV(request.csvPath(), request.errorDirectory());
                if (includeDefaultFacets) {
                    this.facetService.createDefaultFacets();
                }
                datasetFacetRefreshService.refreshDatasetFacet();
            } finally {
                reentrantLock.unlock();
            }
        } else {
            response = "This task is already running. Skipping execution.";
        }

        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}
