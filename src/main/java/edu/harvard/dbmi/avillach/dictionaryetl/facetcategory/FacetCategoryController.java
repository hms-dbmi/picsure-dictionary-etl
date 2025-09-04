package edu.harvard.dbmi.avillach.dictionaryetl.facetcategory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import jakarta.transaction.Transactional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@CrossOrigin(origins = "http://localhost:8081")
@RestController
@RequestMapping("/api")
public class FacetCategoryController {
    @Autowired
    FacetCategoryRepository facetCategoryRepository;
    @Autowired
    FacetCategoryService categoryService;
    FacetCategoryMetaRepository facetCategoryMetaRepository;

    @GetMapping("/facetCategory")
    public ResponseEntity<List<FacetCategoryModel>> getAllFacetCategoryModels() {
        try {
            List<FacetCategoryModel> facetCategoryModels = new ArrayList<FacetCategoryModel>();
            facetCategoryRepository.findAll().forEach(facetCategoryModels::add);

            if (facetCategoryModels.isEmpty()) {
                return new ResponseEntity<>(HttpStatus.NO_CONTENT);
            }
            return new ResponseEntity<>(facetCategoryModels, HttpStatus.OK);
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PutMapping("/facetCategory")
    public ResponseEntity<FacetCategoryModel> updateFacetCategory(@RequestParam String name,
            @RequestParam String display, @RequestParam String description) {

        Optional<FacetCategoryModel> facetCategoryData = facetCategoryRepository
                .findByName(name);

        if (facetCategoryData.isPresent()) {
            // update already existing facetCategory
            FacetCategoryModel existingFacetCategory = facetCategoryData.get();
            existingFacetCategory.setDescription(description);
            existingFacetCategory.setDisplay(display);
            return new ResponseEntity<>(facetCategoryRepository.save(existingFacetCategory), HttpStatus.OK);
        } else {
            // add new facetCategory when facetCategory not present in data
            try {
                FacetCategoryModel newFacetCategory = facetCategoryRepository
                        .save(new FacetCategoryModel(name,
                                display, description));
                return new ResponseEntity<>(newFacetCategory, HttpStatus.CREATED);
            } catch (Exception e) {
                System.out.println(e.getLocalizedMessage());
                return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
            }
        }

    }

    @DeleteMapping("/facetCategory")
    public ResponseEntity<FacetCategoryModel> deleteFacetCategory(@RequestParam String name) {

        Optional<FacetCategoryModel> facetCategoryData = facetCategoryRepository
                .findByName(name);

        if (facetCategoryData.isPresent()) {
            facetCategoryRepository.delete(facetCategoryData.get());
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        } else {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }


    /*Ingest facet categories from "Ideal Ingest" csv format
        Expected CSV headers:
        name(unique)	display name	description
        */

        @Transactional
        @PutMapping("/facet/category/csv")
        public ResponseEntity<String> updateFacetCategoriesFromCSVs(@RequestBody String input) {
            return categoryService.updateFacetCategoriesFromCSVs(input);
        }

}
