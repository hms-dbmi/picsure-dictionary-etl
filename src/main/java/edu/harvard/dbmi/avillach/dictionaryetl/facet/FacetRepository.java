package edu.harvard.dbmi.avillach.dictionaryetl.facet;

import java.util.List;
import java.util.Optional;

import edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.ConceptToFacetDTO;
import edu.harvard.dbmi.avillach.dictionaryetl.facet.model.FacetModel;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface FacetRepository extends JpaRepository<FacetModel, Long> {

    FacetModel findByFacetId(long facetId);

    Optional<FacetModel> findByName(String name);

    Optional<FacetModel> findByNameAndFacetCategoryId(String name, Long facetCategoryId);

    List<FacetModel> findAllByParentId(Long parentId);

    @Query(value = "select f.name from FacetModel f order by f.name")
    List<String> getAllFacetNames();

    @Query(value = """
                SELECT f.*
            FROM dict.facet f
                     JOIN dict.facet__concept_node fcn ON f.facet_id = fcn.facet_id
                     JOIN dict.concept_node cn ON fcn.concept_node_id = cn.concept_node_id
            WHERE cn.dataset_id in (:datasetIDs)
            """, nativeQuery = true)
    List<FacetModel> findAllFacetsByDatasetIDs(Long[] datasetIDs);

    @Query(value = """
            SELECT f.facet_id AS facetId,
                   cn.concept_node_id AS conceptNodeId,
                   f.name AS facetName,
                   cn.concept_path AS conceptPath
            FROM dict.facet f
                     JOIN dict.facet__concept_node fcn ON f.facet_id = fcn.facet_id
                     JOIN dict.concept_node cn ON fcn.concept_node_id = cn.concept_node_id
            WHERE cn.dataset_id = :datasetID
            """, nativeQuery = true)
    List<ConceptToFacetDTO> findFacetToConceptRelationshipsByDatasetID(@Param("datasetID") Long datasetID);

    @Modifying
    @Transactional
    @Query(value = "DELETE FROM dict.facet WHERE facet_category_id = :catId", nativeQuery = true)
    int deleteAllForCategory(@Param("catId") Long catId);

    @Modifying
    @Transactional
    @Query(value = """
            INSERT INTO dict.facet (FACET_CATEGORY_ID, NAME, DISPLAY, DESCRIPTION)
            SELECT DISTINCT :catId, REF, COALESCE(ABBREVIATION, REF), FULL_NAME
            FROM dict.dataset
            ON CONFLICT (name, facet_category_id) DO NOTHING
            """, nativeQuery = true)
    void createFacetForEachDatasetForCategory(@Param("catId") Long catId);

    @Modifying
    @Transactional
    @Query(value = "DELETE FROM dict.facet WHERE name = :name", nativeQuery = true)
    int deleteByName(@Param("name") String name);

    @Modifying
    @Transactional
    @Query(value = "DELETE FROM dict.facet WHERE facet_id IN (:ids)", nativeQuery = true)
    int deleteByIds(@Param("ids") List<Long> ids);
}
