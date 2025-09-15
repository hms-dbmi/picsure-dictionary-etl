package edu.harvard.dbmi.avillach.dictionaryetl.clear;

import edu.harvard.dbmi.avillach.dictionaryetl.Utility.DatabaseCleanupUtility;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@Controller
@RequestMapping("clear")
public class DictionaryClearController {

    private final static Logger log = LoggerFactory.getLogger(DictionaryClearController.class);

    @Autowired
    private DatabaseCleanupUtility databaseCleanupUtility;

    /**
     * Responsible for clearing out database of all existing entries. Iterates over all tables (except update_info) and deletes all contents
     */
    @Transactional
    @DeleteMapping(value = "/all")
    public ResponseEntity<String> clearAllTables() {
        databaseCleanupUtility.truncateTablesAllTables();

        return new ResponseEntity<>("All dataset/concept/facet tables cleared", HttpStatus.OK);
    }

    /**
     * Responsible for clearing out database of all existing entries. Iterates over all tables (except update_info) and deletes all contents
     */
    @Transactional
    @DeleteMapping(value = "/concepts")
    public ResponseEntity<String> clearDatasetAndConceptTables() {
        databaseCleanupUtility.truncateTables();
        return new ResponseEntity<>("All dataset/concept tables cleared", HttpStatus.OK);
    }
}
