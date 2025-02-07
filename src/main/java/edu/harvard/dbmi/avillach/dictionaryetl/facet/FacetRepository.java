package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface FacetRepository extends JpaRepository<FacetModel, Long> {
    List<FacetModel> findByFacetId(long facetId);

    Optional<FacetModel> findByName(String name);

    @Modifying
    @Transactional
    @Query(value = """
                delete from dict.facet where facet_id not in
                    (select distinct facet_id from dict.facet__concept_node) and facet_category_id = :facetCategoryId
            """, nativeQuery = true)
    void deleteUnusedFacetsFromCategory(@Param("facetCategoryId") Long facetCategoryId);

}
