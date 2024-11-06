package edu.harvard.dbmi.avillach.dictionaryetl.dataset;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;

@Repository
public interface DatasetRepository extends JpaRepository<DatasetModel, Long> {
    List<DatasetModel> findByDatasetId(long datasetId);

    Optional<DatasetModel> findByRef(String datasetRef);

    /**
     * This query will return a list of refs that are NOT IN the dataset table
     * @param refs A list of refs
     * @return The List of refs that don't exist in the database
     */
    @Query("SELECT :refs FROM DatasetModel where ref NOT IN :refs")
    List<String> findValuesNotInRef(@Param("refs") List<String> refs);

}
