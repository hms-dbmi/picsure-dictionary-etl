package edu.harvard.dbmi.avillach.dictionaryetl.dataset;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.Optional;

@Repository
public interface DatasetHarmonizationRepository extends JpaRepository<DatasetHarmonizationModel, Long> {
    List<DatasetHarmonizationModel> findBySourceDatasetId(long sourceDatasetId);

    List<DatasetHarmonizationModel> findByHarmonizedDatasetId(long harmonizedDatasetId);

    Optional<DatasetHarmonizationModel> findBySourceDatasetIdAndHarmonizedDatasetId(long sourceDatasetId,
            long harmonizedDatasetId);
}
