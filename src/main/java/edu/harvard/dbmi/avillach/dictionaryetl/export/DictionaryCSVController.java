package edu.harvard.dbmi.avillach.dictionaryetl.export;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@CrossOrigin(origins = "http://localhost:8081")
@RestController
@RequestMapping("export")
public class DictionaryCSVController {

    private static final Logger logger = LoggerFactory.getLogger(DictionaryCSVController.class);

    private final DictionaryCSVService dictionaryCSVService;

    @Autowired
    public DictionaryCSVController(DictionaryCSVService dictionaryCSVService) {
        this.dictionaryCSVService = dictionaryCSVService;
    }

    @GetMapping("/fullIngest")
    public void generateFullIngestCSVs(@RequestParam String downloadPath) {
        logger.info("Generating Full Ingest CSVs.");
        dictionaryCSVService.generateFullIngestCSVs(downloadPath);
        logger.info("Full Ingest CSVs generated successfully.");
    }

    @GetMapping("/fullIngestWithPHS")
    public void generateFullIngestCSVsWithPHS(@RequestBody FullIngestRequest fullIngestRequest) {
        logger.info("Generating Full Ingest CSVs with PHS IDs.");
        String[] phsVals = fullIngestRequest.phsIds().toArray(String[]::new);
        dictionaryCSVService.generateFullIngestCSVs(fullIngestRequest.downloadPath(), phsVals);
        logger.info("Full Ingest CSVs with PHS IDs generated successfully.");
    }


}
