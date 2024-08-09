package edu.harvard.dbmi.avillach.dictionaryetl.consent;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ConsentRepository extends JpaRepository<ConsentModel, Long> {
    List<ConsentModel> findByDatasetId(long datasetId);

    Optional<ConsentModel> findByConsentCodeAndDatasetId(String consentCode, Long datasetId);

}
