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
public interface ConceptRepository extends JpaRepository<ConceptModel, Long> {
    List<ConceptModel> findByDatasetId(long datasetId);

    List<ConceptModel> findByParentId(long datasetId);

    Optional<ConceptModel> findByConceptPath(String conceptPath);

    Optional<List<ConceptModel>> findByName(String name);

    @Modifying
    @Transactional
    @Query(value = """
            UPDATE dict.concept_node ch
            SET parent_id = NULL
            WHERE ch.parent_id IN (
                SELECT concept_node_id FROM dict.concept_node WHERE dataset_id = :datasetId
            )
            """, nativeQuery = true)
    int nullChildrenByDataset(@Param("datasetId") long datasetId);

    @Modifying
    @Transactional
    @Query(value = "DELETE FROM dict.concept_node WHERE dataset_id = :datasetId", nativeQuery = true)
    int deleteByDatasetId(@Param("datasetId") long datasetId);
}
