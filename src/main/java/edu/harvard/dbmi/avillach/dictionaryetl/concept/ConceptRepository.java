package edu.harvard.dbmi.avillach.dictionaryetl.concept;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import edu.harvard.dbmi.avillach.dictionaryetl.concept.ConceptModel;

@Repository
public interface ConceptRepository extends JpaRepository<ConceptModel, Long> {
    List<ConceptModel> findByDatasetId(long datasetId);

    Optional<ConceptModel> findByConceptPath(String conceptPath);

}
