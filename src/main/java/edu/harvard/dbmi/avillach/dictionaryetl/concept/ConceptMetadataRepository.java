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
            insert into dict.concept_node_meta
                (concept_node_id, key, value)
                    select concept_node_id, 'stigmatized' as key, :val as new_value
                        from dict.concept_node as cn
                            where concept_path in (select unnest(:paths))
            on conflict (concept_node_id, key)
            do update set value = EXCLUDED.value
            """, nativeQuery = true)
    int updateStigvarsFromConceptPaths(@Param(value = "paths") String[] paths, String val);

    @Query(value = """
            select new edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptStigvarIdentificationModel(
                concept_node.name,
                concept_node.display,
                metadesc.value,
                concept_node.conceptPath,
                metavals.value,
                dataset.ref
            )
            from ConceptModel concept_node
                join DatasetModel dataset on dataset.datasetId = concept_node.datasetId
                left join ConceptMetadataModel metavals on metavals.conceptNodeId = concept_node.conceptNodeId and metavals.key = 'values'
                left join ConceptMetadataModel metadesc on metadesc.conceptNodeId = concept_node.conceptNodeId and metadesc.key = 'description'
            where dataset.ref = :ref
            """)
    List<ConceptStigvarIdentificationModel> getInfoForStigvars(@Param(value = "ref") String ref);

    @Query(value = "select key FROM dict.concept_node_meta group by key;", nativeQuery = true)
    List<String> findAllKeyValues();

    @Query(value = """
            select concept_node_meta.key from dict.concept_node_meta
            where concept_node_id in (
                select concept_node_id from dict.concept_node
                where concept_node.dataset_id in (:datasetIDs)
                )
            group by key;
    """, nativeQuery = true)
    List<String> findByDatasetID(Long[] datasetIDs);
}
