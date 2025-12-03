package edu.harvard.dbmi.avillach.dictionaryetl.concept;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import edu.harvard.dbmi.avillach.dictionaryetl.facet.dto.ConceptPathRow;
import jakarta.persistence.QueryHint;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface ConceptRepository extends JpaRepository<ConceptModel, Long> {
    List<ConceptModel> findByDatasetId(long datasetId);

    List<ConceptModel> findByParentId(long datasetId);

    Optional<ConceptModel> findByConceptPath(String conceptPath);

    Optional<List<ConceptModel>> findByName(String name);

    List<ConceptModel> findByConceptType(String conceptType);

    @Query(value = """
        SELECT concept_node_id AS conceptNodeId, concept_path AS conceptPath
        FROM dict.concept_node
        """, nativeQuery = true)
    @QueryHints({
            @QueryHint(name = "hibernate.jdbc.fetch_size", value = "1000"),
            @QueryHint(name = "hibernate.query.readOnly", value = "true"),
            @QueryHint(name = "hibernate.query.cacheable", value = "false")
    })
    @Transactional(readOnly = true)
    Stream<ConceptPathRow> streamNodeIdAndPath();

    // Streams only end (leaf) concept nodes — those with no children.
    @Query(value = """
    SELECT cn.concept_node_id AS conceptNodeId, cn.concept_path AS conceptPath
    FROM dict.concept_node cn
    WHERE NOT EXISTS (
        SELECT 1
        FROM dict.concept_node child
        WHERE child.parent_id = cn.concept_node_id
    )
    """, nativeQuery = true)
    @QueryHints({
            @QueryHint(name = "hibernate.jdbc.fetch_size", value = "1000"),
            @QueryHint(name = "hibernate.query.readOnly", value = "true"),
            @QueryHint(name = "hibernate.query.cacheable", value = "false")
    })
    @Transactional(readOnly = true)
    Stream<ConceptPathRow> streamLeafNodeIdAndPath();

    @Query(value = """
        SELECT concept_node_id AS conceptNodeId, concept_path AS conceptPath
        FROM dict.concept_node
        LEFT JOIN dict.dataset d on concept_node.dataset_id = d.dataset_id
        WHERE d.ref = :ref
        """, nativeQuery = true)
    @QueryHints({
            @QueryHint(name = "hibernate.jdbc.fetch_size", value = "1000"),
            @QueryHint(name = "hibernate.query.readOnly", value = "true"),
            @QueryHint(name = "hibernate.query.cacheable", value = "false")
    })
    @Transactional(readOnly = true)
    Stream<ConceptPathRow> streamDatasetNodeIdAndPath(String ref);

}
