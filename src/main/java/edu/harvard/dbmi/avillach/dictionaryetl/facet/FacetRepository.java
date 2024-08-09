package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface FacetRepository extends JpaRepository<FacetModel, Long> {
    List<FacetModel> findByFacetId(long facetId);

    Optional<FacetModel> findByName(String name);

}
