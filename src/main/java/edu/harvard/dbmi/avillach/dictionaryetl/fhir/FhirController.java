
package edu.harvard.dbmi.avillach.dictionaryetl.fhir;

import edu.harvard.dbmi.avillach.dictionaryetl.fhir.model.ResearchStudy;
import org.springframework.beans.factory.annotation.Autowired;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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

@PatchMapping("/datasets/metadata/refresh")
public ResponseEntity<String> datasetsMetadataRefresh() {
    try {
        fhirService.updateDatasetMetadata();
        return ResponseEntity.ok("Metadata update successful");
    } catch (IOException e) {
        logger.error("Error updating dataset metadata", e);
        return ResponseEntity.status(500).body("Metadata update failed");
    }
}

@GetMapping("/research-studies/findAll")
public ResponseEntity<List<ResearchStudy>> getResearchStudies() {
    try {
        List<ResearchStudy> researchStudies = fhirService.getResearchStudies();
        return ResponseEntity.ok(researchStudies);
    } catch (IOException e) {
        logger.error("Error retrieving research studies", e);
        return ResponseEntity.status(500).build();
    }
}

@GetMapping("/listByDbgapAccessions")
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
