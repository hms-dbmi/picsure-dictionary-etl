package edu.harvard.dbmi.avillach.dictionaryetl.concept;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface ConceptMetadataRepository extends JpaRepository<ConceptMetadataModel, Long> {
    List<ConceptMetadataModel> findByConceptNodeId(long conceptNodeId);

    List<ConceptMetadataModel> findByKey(String key);

    Optional<ConceptMetadataModel> findByConceptNodeIdAndKey(long conceptNodeId, String key);

    @Modifying
    @Transactional
    @Query(value = """
            WITH paths AS (
                SELECT unnest(:paths) AS path
            )
                update concept_node_meta set value = :val where concept_node_meta_id in (
                        select cnm.concept_node_meta_id from paths
                        left join concept_node cn on paths.path = cn.concept_path
                        left join concept_node_meta cnm on cn.concept_node_id=cnm.concept_node_id and key = 'stigmatized')
            """, nativeQuery = true)
    void updateStigvarsFromConceptPaths(@Param(value = "paths") String[] paths, String val);

}
