package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class FacetService {

    private final FacetRepository facetRepository;
    private final FacetCategoryService facetCategoryService;
    private final FacetConceptService facetConceptService;
    private final FacetMetadataRepository facetMetadataRepository;

    @Autowired
    public FacetService(FacetRepository facetRepository, FacetCategoryService facetCategoryService, FacetConceptService facetConceptService, FacetMetadataRepository facetMetadataRepository) {
        this.facetRepository = facetRepository;
        this.facetCategoryService = facetCategoryService;
        this.facetConceptService = facetConceptService;
        this.facetMetadataRepository = facetMetadataRepository;
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

    public List<String> getFacetMetadataKeyNames() {
        return this.facetMetadataRepository.getFacetMetadataKeyNames();
    }

    public List<String> getFacetNames() {
        return this.facetRepository.getAllFacetNames();
    }

    public List<FacetModel> findAllFacetsByDatasetIDs(Long[] datasetIDs) {
        return this.facetRepository.findAllFacetsByDatasetIDs(datasetIDs);
    }

    public List<FacetModel> findAll() {
        return this.facetRepository.findAll();
    }

    public Optional<FacetModel> findByID(Long parentId) {
        return this.facetRepository.findById(parentId);
    }

    public Optional<FacetMetadataModel> findFacetMetadataByFacetIDAndKey(Long facetId, String key) {
        return this.facetMetadataRepository.findByFacetIdAndKey(facetId, key);
    }

    public List<ConceptToFacetDTO> findFacetToConceptRelationshipsByDatasetID(Long datasetID) {
        return this.facetRepository.findFacetToConceptRelationshipsByDatasetID(datasetID);
    }
}
