package edu.harvard.dbmi.avillach.dictionaryetl.facetloader;

import edu.harvard.dbmi.avillach.dictionaryetl.facetloader.dto.ClearResult;
import edu.harvard.dbmi.avillach.dictionaryetl.facetloader.dto.FacetCategoryWrapper;
import edu.harvard.dbmi.avillach.dictionaryetl.facetloader.dto.FacetClearRequest;
import edu.harvard.dbmi.avillach.dictionaryetl.facetloader.dto.Result;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@CrossOrigin(origins = "http://localhost:8081")
@RequestMapping("/api/facet-loader")
public class FacetLoaderController {

    private final FacetLoaderService service;

    public FacetLoaderController(FacetLoaderService service) {
        this.service = service;
    }

    @PostMapping("/load")
    public ResponseEntity<Result> load(@RequestBody List<FacetCategoryWrapper> payload) {
        Result result = service.load(payload);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @PostMapping("/clear")
    public ResponseEntity<ClearResult> clear(@RequestBody FacetClearRequest request) {
        ClearResult result = service.clear(request);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }
}
