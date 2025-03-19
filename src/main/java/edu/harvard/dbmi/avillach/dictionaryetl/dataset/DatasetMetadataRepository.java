package edu.harvard.dbmi.avillach.dictionaryetl.dataset;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface DatasetMetadataRepository extends JpaRepository<DatasetMetadataModel, Long> {
    List<DatasetMetadataModel> findByDatasetId(long datasetId);

    Optional<DatasetMetadataModel> findByDatasetIdAndKey(long datasetId, String key);

    @Query(value = "select key FROM dict.dataset_meta group by key;", nativeQuery = true)
    List<String> findAllKeyValues();
}
