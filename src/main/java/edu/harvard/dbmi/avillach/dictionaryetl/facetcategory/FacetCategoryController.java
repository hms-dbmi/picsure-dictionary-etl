package edu.harvard.dbmi.avillach.dictionaryetl.facetcategory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryRepository;

@CrossOrigin(origins = "http://localhost:8081")
@RestController
@RequestMapping("/api")
public class FacetCategoryController {
    @Autowired
    FacetCategoryRepository facetCategoryRepository;

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
    public ResponseEntity<FacetCategoryModel> updateFacetCategory(@RequestBody FacetCategoryModel facetCategory) {

        Optional<FacetCategoryModel> facetCategoryData = facetCategoryRepository
                .findByName(facetCategory.getName());

        if (facetCategoryData.isPresent()) {
            // update already existing facetCategory
            FacetCategoryModel existingFacetCategory = facetCategoryData.get();
            existingFacetCategory.setDescription(facetCategory.getDescription());
            existingFacetCategory.setDisplay(facetCategory.getDisplay());
            return new ResponseEntity<>(facetCategoryRepository.save(existingFacetCategory), HttpStatus.OK);
        } else {
            // add new facetCategory when facetCategory not present in data
            try {
                FacetCategoryModel newFacetCategory = facetCategoryRepository
                        .save(new FacetCategoryModel(facetCategory.getName(),
                                facetCategory.getDisplay(), facetCategory.getDescription()));
                return new ResponseEntity<>(newFacetCategory, HttpStatus.CREATED);
            } catch (Exception e) {
                System.out.println(e.getLocalizedMessage());
                return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
            }
        }

    }

    @DeleteMapping("/facetCategory")
    public ResponseEntity<FacetCategoryModel> deleteFacetCategory(@RequestParam String facetCategoryName) {

        Optional<FacetCategoryModel> facetCategoryData = facetCategoryRepository
                .findByName(facetCategoryName);

        if (facetCategoryData.isPresent()) {
            facetCategoryRepository.delete(facetCategoryData.get());
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        } else {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }
}
