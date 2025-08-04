package edu.harvard.dbmi.avillach.dictionaryetl.concept;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.NativeQuery;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface ConceptRepository extends JpaRepository<ConceptModel, Long> {
    List<ConceptModel> findByDatasetId(long datasetId);

    List<ConceptModel> findByParentId(long datasetId);

    Optional<ConceptModel> findByConceptPath(String conceptPath);

    @Modifying
    @Transactional
    @Query(value = """
            UPDATE dict.concept_node
            SET parent_id = parent.concept_node_id
            FROM dict.concept_node parent
            WHERE concept_node.concept_path_md5 = md5(:childPath)
                AND parent.concept_path_md5 = md5(:parentPath)
            """, nativeQuery = true)
    void updateConceptParentIds(@Param("childPath") String childPath, @Param("parentPath") String parentPath);

}
