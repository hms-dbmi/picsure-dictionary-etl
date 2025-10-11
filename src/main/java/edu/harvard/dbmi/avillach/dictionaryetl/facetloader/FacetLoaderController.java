package edu.harvard.dbmi.avillach.dictionaryetl.facetloader;

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
    public ResponseEntity<FacetLoaderService.Result> load(@RequestBody List<FacetCategoryWrapper> payload) {
        FacetLoaderService.Result result = service.load(payload);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }
}
