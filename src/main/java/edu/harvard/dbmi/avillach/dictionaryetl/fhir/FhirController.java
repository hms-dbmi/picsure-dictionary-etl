package edu.harvard.dbmi.avillach.dictionaryetl.fhir;

import edu.harvard.dbmi.avillach.dictionaryetl.fhir.model.ResearchStudy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

@RestController
@RequestMapping("/api/fhir")
public class FhirController {

    private final FhirService fhirService;

    @Autowired
    public FhirController(FhirService fhirService) {
        this.fhirService = fhirService;
    }

    @PostMapping("/datasets/metadata/update")
    public String datasetsMetadataRefresh() {
        try {
            fhirService.updateDatasetMetadata();
            return "Dataset Loaded Successfully!";
        } catch (IOException e) {
            return "Error Loading Dataset: " + e.getMessage();
        }
    }

    @GetMapping("/research-studies")
    public List<ResearchStudy> getResearchStudies() throws IOException {
        return fhirService.getResearchStudies();
    }

    @GetMapping("/distinct-phs-values")
    public ResponseEntity<List<String>> getDistinctPhsValues() {
        try {
            List<String> distinctPhsValues = fhirService.getDistinctPhsValues();
            return ResponseEntity.ok(distinctPhsValues);
        } catch (IOException e) {
            return ResponseEntity.status(500).body(Collections.emptyList());
        }
    }
}
