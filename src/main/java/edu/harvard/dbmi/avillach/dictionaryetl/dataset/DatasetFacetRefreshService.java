package edu.harvard.dbmi.avillach.dictionaryetl.dataset;

import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetConceptRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.FacetRepository;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryModel;
import edu.harvard.dbmi.avillach.dictionaryetl.facetcategory.FacetCategoryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class DatasetFacetRefreshService {
    private static final Logger log = LoggerFactory.getLogger(DatasetFacetRefreshService.class);
    private final DatasetRepository datasetRepository;
    private final FacetRepository facetRepository;
    private final FacetCategoryRepository facetCategoryRepository;
    private final FacetConceptRepository facetConceptRepository;

    @Autowired
    public DatasetFacetRefreshService(
        DatasetRepository datasetRepository,
        FacetRepository facetRepository,
        FacetCategoryRepository facetCategoryRepository,
        FacetConceptRepository facetConceptRepository
    ) {
        this.datasetRepository = datasetRepository;
        this.facetRepository = facetRepository;
        this.facetCategoryRepository = facetCategoryRepository;
        this.facetConceptRepository = facetConceptRepository;
    }

    public void refreshDatasetFacet() {
        if (datasetRepository.findAll().size() < 2) {
            log.info("Less than 2 datasets detected. Not altering facets");
            return;
        }

        Optional<FacetCategoryModel> maybeCategory = facetCategoryRepository.findByName("dataset_id");
        FacetCategoryModel category;
        if (maybeCategory.isPresent()) {
            category = maybeCategory.get();
            log.info("Found a dataset_id facet category. Deleting all associated facets and facet pairs");
            facetConceptRepository.deleteAllForCategory(category.getFacetCategoryId());
            facetRepository.deleteAllForCategory(category.getFacetCategoryId());
        } else {
            log.info("Did not find a dataset_id facet category. Creating...");
            category = new FacetCategoryModel("dataset_id", "Dataset", "The dataset this concept belongs to");
            facetCategoryRepository.save(category);
        }

        log.info("Adding facets for each dataset");
        facetRepository.createFacetForEachDatasetForCategory(category.getFacetCategoryId());
        log.info("Adding facet/concept pairs for each leaf concept node");
        facetConceptRepository.createDatasetPairForEachLeafConcept(category.getFacetCategoryId());
    }
}
