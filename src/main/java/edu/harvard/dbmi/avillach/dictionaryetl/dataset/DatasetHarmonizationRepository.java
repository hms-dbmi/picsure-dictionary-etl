package edu.harvard.dbmi.avillach.dictionaryetl.dataset;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import java.util.Optional;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface DatasetHarmonizationRepository extends JpaRepository<DatasetHarmonizationModel, Long> {
    List<DatasetHarmonizationModel> findBySourceDatasetId(long sourceDatasetId);

    List<DatasetHarmonizationModel> findByHarmonizedDatasetId(long harmonizedDatasetId);

    Optional<DatasetHarmonizationModel> findBySourceDatasetIdAndHarmonizedDatasetId(long sourceDatasetId,
            long harmonizedDatasetId);

    @Modifying
    @Transactional
    @Query(value = "DELETE FROM dict.dataset_harmonization WHERE source_dataset_id = :datasetId", nativeQuery = true)
    int deleteBySourceDatasetId(@Param("datasetId") Long datasetId);
}
