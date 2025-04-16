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

    @Query(value = "select f.name from FacetModel f order by f.name")
    List<String> getAllFacetNames();

    @Query(value = """
                SELECT f.*
            FROM dict.facet f
                     JOIN dict.facet__concept_node fcn ON f.facet_id = fcn.facet_id
                     JOIN dict.concept_node cn ON fcn.concept_node_id = cn.concept_node_id
            WHERE cn.dataset_id in (:datasetIDs)
            group by f.facet_id, facet_category_id, f.name, f.display, description, f.parent_id
            """, nativeQuery = true)
    List<FacetModel> findAllFacetsByDatasetIDs(Long[] datasetIDs);

    @Query(value = """
        SELECT f.facet_id AS facetId,
               cn.concept_node_id AS conceptNodeId,
               f.name AS facetName
        FROM dict.facet f
                 JOIN dict.facet__concept_node fcn ON f.facet_id = fcn.facet_id
                 JOIN dict.concept_node cn ON fcn.concept_node_id = cn.concept_node_id
        WHERE cn.dataset_id = :datasetID
        """, nativeQuery = true)
    List<ConceptToFacetDTO> findFacetToConceptRelationshipsByDatasetID(@Param("datasetID") Long datasetID);
}
