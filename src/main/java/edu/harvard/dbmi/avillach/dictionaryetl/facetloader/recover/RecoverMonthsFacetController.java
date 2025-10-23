package edu.harvard.dbmi.avillach.dictionaryetl.facetloader.recover;

import edu.harvard.dbmi.avillach.dictionaryetl.facetloader.recover.RecoverMonthsFacetGeneratorService.GenerateRecoverMonthsRequest;
import edu.harvard.dbmi.avillach.dictionaryetl.facetloader.recover.RecoverMonthsFacetGeneratorService.GenerateRecoverMonthsResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@CrossOrigin(origins = "http://localhost:8081")
@RequestMapping("/api/facet-loader/recover/months")
public class RecoverMonthsFacetController {

    private final RecoverMonthsFacetGeneratorService generator;

    public RecoverMonthsFacetController(RecoverMonthsFacetGeneratorService generator) {
        this.generator = generator;
    }

    @PostMapping("/generate")
    public ResponseEntity<GenerateRecoverMonthsResponse> generate(@RequestBody GenerateRecoverMonthsRequest request) {
        GenerateRecoverMonthsResponse resp = generator.generate(request);
        return new ResponseEntity<>(resp, HttpStatus.OK);
    }
}
