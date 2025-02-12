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


}
