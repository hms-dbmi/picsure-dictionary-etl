package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class FacetConceptService {

    private final FacetConceptRepository facetConceptRepository;

    @Autowired
    public FacetConceptService(FacetConceptRepository facetConceptRepository) {
        this.facetConceptRepository = facetConceptRepository;
    }

    public void mapConceptConceptTypeToFacet(Long facetID, String conceptType) {
        this.facetConceptRepository.mapConceptConceptTypeToFacet(facetID, conceptType);
    }
}
