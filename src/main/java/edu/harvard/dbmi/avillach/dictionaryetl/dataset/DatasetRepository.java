package edu.harvard.dbmi.avillach.dictionaryetl.dataset;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;

@Repository
public interface DatasetRepository extends JpaRepository<DatasetModel, Long> {
    List<DatasetModel> findByDatasetId(long datasetId);

    Optional<DatasetModel> findByDatasetRef(String datasetRef);

}
