package edu.harvard.dbmi.avillach.dictionaryetl.fhir;

import edu.harvard.dbmi.avillach.dictionaryetl.fhir.model.ResearchStudy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("api/fhir")
public class FhirController {

    private static final Logger logger = LoggerFactory.getLogger(FhirController.class);

    private final FhirService fhirService;

    @Autowired
    public FhirController(FhirService fhirService) {
        this.fhirService = fhirService;
    }

    @PatchMapping("/load/metadata/refresh")
    public ResponseEntity<String> datasetsMetadataRefresh() {
        try {
            fhirService.updateDatasetMetadata();
            return ResponseEntity.ok("Metadata update successful");
        } catch (IOException e) {
            logger.error("Error updating dataset metadata", e);
            return ResponseEntity.status(500).body("Metadata update failed");
        }
    }

    @PatchMapping("/config/extensions/load-mappings")
    public ResponseEntity<String> updateUrlToKeyMap(@RequestBody String urlToKeyMapJson) {
        fhirService.setUrlToKeyMap(urlToKeyMapJson);
        return ResponseEntity.ok("URL to Key Map updated successfully");
    }

    @GetMapping("/research-studies/")
    public ResponseEntity<List<ResearchStudy>> findAll() {
        try {
            List<ResearchStudy> researchStudies = fhirService.getResearchStudies();
            return ResponseEntity.ok(researchStudies);
        } catch (IOException e) {
            logger.error("Error retrieving research studies", e);
            return ResponseEntity.status(500).build();
        }
    }

    @GetMapping("/research-studies/find-dbgap")
    public ResponseEntity<List<String>> getDistinctPhsValues() {
        try {
            List<String> distinctPhsValues = fhirService.getDistinctPhsValues();
            return ResponseEntity.ok(distinctPhsValues);
        } catch (IOException e) {
            logger.error("Error retrieving distinct PHS values", e);
            return ResponseEntity.status(500).build();
        }
    }
}