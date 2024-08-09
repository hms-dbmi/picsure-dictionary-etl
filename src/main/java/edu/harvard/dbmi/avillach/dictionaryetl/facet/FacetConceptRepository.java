package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface FacetConceptRepository extends JpaRepository<FacetConceptModel, Long> {
    List<FacetConceptModel> findByFacetConceptNodeId(long facetConceptNodeId);

    Optional<FacetConceptModel> findByConceptNodeId(long conceptNodeId);

    Optional<FacetConceptModel> findByFacetId(long facetId);

    Optional<FacetConceptModel> findByFacetIdAndConceptNodeId(long facetId, long conceptNodeId);

}
