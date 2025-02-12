package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;


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

    public Optional<FacetConceptModel> findByFacetAndConcept(Long facetID, Long conceptID) {
        return this.facetConceptRepository.findByFacetIdAndConceptNodeId(facetID, conceptID);
    }

    public void mapConceptsToSpecificFacet(Long facetId, List<Long> conceptNodes){
        StringBuilder qBuilder = new StringBuilder();
        qBuilder.append("insert into facet__concept_node (facet_id, concept_node_id)\n" + //
                        "(select ");
                        qBuilder.append(facetId);
                        qBuilder.append(",\n" + //
                        "concept_node.concept_node_id from dict.concept_node where display = 'SAMPLE_ID')\n" + //
                        "on conflict do nothing");
        
    }

}
