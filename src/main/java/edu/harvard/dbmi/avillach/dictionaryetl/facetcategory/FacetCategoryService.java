package edu.harvard.dbmi.avillach.dictionaryetl.facetcategory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class FacetCategoryService {

    private final FacetCategoryRepository facetCategoryRepository;
    private final FacetCategoryMetaRepository facetCategoryMetaRepository;

    @Autowired
    public FacetCategoryService(FacetCategoryRepository facetCategoryRepository, FacetCategoryMetaRepository facetCategoryMetaRepository) {
        this.facetCategoryRepository = facetCategoryRepository;
        this.facetCategoryMetaRepository = facetCategoryMetaRepository;
    }

    public FacetCategoryModel save(FacetCategoryModel facetCategoryModel) {
        return this.facetCategoryRepository.save(facetCategoryModel);
    }

    public Optional<FacetCategoryModel> findByName(String name) {
        return this.facetCategoryRepository.findByName(name);
    }

    public List<String> getFacetCategoryMetadataKeyNames() {
        return this.facetCategoryMetaRepository.getFacetCategoryMetadataKeyNames();
    }

    public List<FacetCategoryModel> findAll() {
        return this.facetCategoryRepository.findAll();
    }

    public List<FacetCategoryMeta> findFacetCategoryMetaByFacetCategoriesID(Long[] facetCategoryID) {
        return this.facetCategoryMetaRepository.findByFacetCategoryID(facetCategoryID);
    }

}
