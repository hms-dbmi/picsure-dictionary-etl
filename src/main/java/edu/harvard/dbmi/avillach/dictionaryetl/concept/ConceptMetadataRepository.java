package edu.harvard.dbmi.avillach.dictionaryetl.concept;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.transaction.annotation.Transactional;

import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface ConceptMetadataRepository extends JpaRepository<ConceptMetadataModel, Long> {
    List<ConceptMetadataModel> findByConceptNodeId(long conceptNodeId);

    List<ConceptMetadataModel> findByKey(String key);

    Optional<ConceptMetadataModel> findByConceptNodeIdAndKey(long conceptNodeId, String key);

/*     @Transactional
    @Modifying
    @Query(value = fullQuery, nativeQuery = true)
    void insertOrUpdateConceptMeta(Query fullQuery, String value); */

}
