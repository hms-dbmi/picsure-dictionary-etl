package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface FacetMetadataRepository extends JpaRepository<FacetMetadataModel, Long> {
    List<FacetMetadataModel> findByFacetId(long facetId);

    Optional<FacetMetadataModel> findByFacetIdAndKey(long facetId, String key);

}
