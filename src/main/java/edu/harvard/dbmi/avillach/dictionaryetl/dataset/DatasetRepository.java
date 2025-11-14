package edu.harvard.dbmi.avillach.dictionaryetl.dataset;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface DatasetRepository extends JpaRepository<DatasetModel, Long> {
    List<DatasetModel> findByDatasetId(long datasetId);

    Optional<DatasetModel> findByRef(String datasetRef);

    /**
     * This query will return a list of refs that are NOT IN the dataset table
     * @param refs A list of refs
     * @return The List of refs that don't exist in the database
     */
    @Query(value = """
WITH refs AS (
        SELECT unnest(:refs) AS ref
    )
    SELECT refs.ref
    FROM refs
    LEFT JOIN dict.dataset d ON refs.ref = d.ref
    WHERE d.ref IS NULL
""", nativeQuery = true)
    List<String> findValuesNotInRef(@Param("refs") String[] refs);

    @Query(value = "SELECT dataset.dataset_id AS datasetId, dataset.ref AS ref FROM dict.dataset ORDER BY ref", nativeQuery = true)
    List<DataSetRefDto> getAllDatasetRefsSorted();

    @Query(value = "SELECT dataset.dataset_id AS datasetId, dataset.ref AS ref FROM dict.dataset WHERE ref IN (:datasetRefs) ORDER BY ref", nativeQuery = true)
    List<DataSetRefDto> getDatasetRefsSorted(String[] datasetRefs);

    @Modifying
    @Transactional
    @Query(value = "DELETE FROM dict.dataset WHERE ref = :ref", nativeQuery = true)
    int deleteByRef(@Param("ref") String ref);
}

