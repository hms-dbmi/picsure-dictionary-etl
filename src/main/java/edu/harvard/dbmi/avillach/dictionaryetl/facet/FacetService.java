package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class FacetService {

    private final FacetRepository facetRepository;
    private final FacetCategoryService facetCategoryService;
    private final FacetConceptService facetConceptService;

    @Autowired
    public FacetService(FacetRepository facetRepository, FacetCategoryService facetCategoryService, FacetConceptService facetConceptService) {
        this.facetRepository = facetRepository;
        this.facetCategoryService = facetCategoryService;
        this.facetConceptService = facetConceptService;
    }

    public FacetModel save(FacetModel facetModel) {
        return this.facetRepository.save(facetModel);
    }

    public Optional<FacetModel> findByName(String name) {
        return this.facetRepository.findByName(name);
    }

    /**
     * This method will create the data_type FacetCategory, continuous & categorical Facet, and map all continuous and
     * categorical Concepts to the respective Facet.
     */
    public void createDefaultFacets() {
        FacetCategoryModel dataType = this.facetCategoryService.findByName("data_type").orElse(
                this.facetCategoryService.save(new FacetCategoryModel("data_type", "Type of Variable", "Continuous or categorical"))
        );

        FacetModel categorical = this.findByName("categorical").orElse(
                this.save(new FacetModel(dataType.getFacetCategoryId(), "categorical", "Categorical", "", null))
        );

        FacetModel continuous = this.findByName("continuous").orElse(
                this.save(new FacetModel(dataType.getFacetCategoryId(), "continuous", "Continuous", "", null))
        );

        this.facetConceptService.mapConceptConceptTypeToFacet(categorical.getFacetId(), "categorical");
        this.facetConceptService.mapConceptConceptTypeToFacet(continuous.getFacetId(), "continuous");
    }


}
