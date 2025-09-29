package edu.harvard.dbmi.avillach.dictionaryetl.consent;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface ConsentRepository extends JpaRepository<ConsentModel, Long> {
    List<ConsentModel> findByDatasetId(long datasetId);

    Optional<ConsentModel> findByConsentCodeAndDatasetId(String consentCode, Long datasetId);

    @Modifying
    @Transactional
    @Query(value = "DELETE FROM dict.consent WHERE dataset_id = :datasetId", nativeQuery = true)
    int deleteByDatasetId(@Param("datasetId") Long datasetId);
}
