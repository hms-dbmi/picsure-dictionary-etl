package edu.harvard.dbmi.avillach.dictionaryetl.concept;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ConceptRepository extends JpaRepository<ConceptModel, Long> {
    List<ConceptModel> findByDatasetId(long datasetId);

    List<ConceptModel> findByParentId(long datasetId);

    Optional<ConceptModel> findByConceptPath(String conceptPath);

    Optional<List<ConceptModel>> findByName(String name);

    List<ConceptModel> findByConceptType(String conceptType);
}
