package edu.harvard.dbmi.avillach.dictionaryetl.concept;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ConceptMetadataRepository extends JpaRepository<ConceptMetadataModel, Long> {
    List<ConceptMetadataModel> findByConceptNodeId(long conceptNodeId);

    Optional<ConceptMetadataModel> findByConceptNodeIdAndKey(long conceptNodeId, String key);

}
