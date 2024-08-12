package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface FacetConceptRepository extends JpaRepository<FacetConceptModel, Long> {
    List<FacetConceptModel> findByFacetConceptId(Long facetConceptId);

    Optional<List<FacetConceptModel>> findByConceptNodeId(Long conceptNodeId);

    Optional<List<FacetConceptModel>> findByFacetId(Long facetId);

    Optional<FacetConceptModel> findByFacetIdAndConceptNodeId(Long facetId, Long conceptNodeId);

}
