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
public interface FacetConceptRepository extends JpaRepository<FacetConceptModel, Long> {
  List<FacetConceptModel> findByFacetConceptId(Long facetConceptId);

  Optional<List<FacetConceptModel>> findByConceptNodeId(Long conceptNodeId);

  Optional<List<FacetConceptModel>> findByFacetId(Long facetId);

  Optional<FacetConceptModel> findByFacetIdAndConceptNodeId(Long facetId, Long conceptNodeId);

  @Modifying
  @Transactional
  @Query(value = """
      INSERT INTO dict.facet__concept_node (facet_id, concept_node_id)
      SELECT :facetID, cn.concept_node_id
      FROM dict.concept_node cn
      WHERE LOWER(cn.concept_type) = LOWER(:conceptType)
      ON CONFLICT DO NOTHING;
      """, nativeQuery = true)
  void mapConceptConceptTypeToFacet(@Param("facetID") Long facetID, @Param("conceptType") String conceptType);

  @Modifying
  @Transactional
  @Query(value = """
      INSERT INTO dict.facet__concept_node (facet_id, concept_node_id)
      SELECT :facetID, cn.concept_node_id
      FROM dict.concept_node cn
      WHERE cn.dataset_id = :datasetId
      ON CONFLICT DO NOTHING;
      """, nativeQuery = true)
  void mapConceptDatasetIdToFacet(@Param("facetID") Long facetID, @Param("datasetId") Long datasetId);

  @Modifying
  @Transactional
  @Query(value = """
      INSERT INTO dict.facet__concept_node (facet_id, concept_node_id)
      SELECT :facetID, cn.concept_node_id
      FROM dict.concept_node cn
      WHERE cn.display = :display
      ON CONFLICT DO NOTHING;
      """, nativeQuery = true)
  void mapConceptDisplayToFacet(@Param("facetID") Long facetID, @Param("display") String display);

  @Modifying
  @Transactional
  @Query(value = """
      DELETE FROM dict.facet__concept_node WHERE facet_id IN (
          SELECT facet_id FROM dict.facet WHERE facet.facet_category_id = :facetCategoryId
      );
      """, nativeQuery = true)
  void deleteAllForCategory(@Param("facetCategoryId") Long facetCategoryId);

  @Modifying
  @Transactional
  @Query(value = """
      INSERT INTO dict.facet__concept_node (concept_node_id, facet_id)
      SELECT concept_node.concept_node_id, facet.facet_id
      FROM dict.concept_node
          JOIN dict.dataset ON concept_node.dataset_id = dataset.dataset_id
          JOIN dict.facet ON dataset.REF = facet.NAME
          LEFT JOIN dict.concept_node_meta AS categorical_values ON dict.concept_node.concept_node_id = categorical_values.concept_node_id AND categorical_values.KEY = 'values'
      WHERE (continuous_min.value <> '' OR continuous_max.value <> '' OR categorical_values.value <> '')
      """, nativeQuery = true)
  void createDatasetPairForEachLeafConcept(@Param("facetCategoryId") Long facetCategoryId);

  @Modifying
  @Transactional
  @Query(value = """
      INSERT INTO dict.facet__concept_node (concept_node_id, facet_id)
      SELECT concept_node.concept_node_id, :facetID
      FROM dict.concept_node
         LEFT JOIN dict.concept_node_meta AS categorical_values ON dict.concept_node.concept_node_id = categorical_values.concept_node_id AND categorical_values.KEY = 'values'
      WHERE concept_node.concept_path_md5 = md5(:conceptPath)
  """, nativeQuery = true)
  void createFacetConceptForFacetAndConceptWithPath(@Param("facetID") Long facetID, @Param("conceptPath") String conceptPath);
}
