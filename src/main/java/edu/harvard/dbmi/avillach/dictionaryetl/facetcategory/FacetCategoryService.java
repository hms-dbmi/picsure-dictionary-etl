package edu.harvard.dbmi.avillach.dictionaryetl.facetcategory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class FacetCategoryService {

    private final FacetCategoryRepository facetCategoryRepository;

    @Autowired
    public FacetCategoryService(FacetCategoryRepository facetCategoryRepository) {
        this.facetCategoryRepository = facetCategoryRepository;
    }

    public FacetCategoryModel save(FacetCategoryModel facetCategoryModel) {
        return this.facetCategoryRepository.save(facetCategoryModel);
    }

    public Optional<FacetCategoryModel> findByName(String name) {
        return this.facetCategoryRepository.findByName(name);
    }


}
