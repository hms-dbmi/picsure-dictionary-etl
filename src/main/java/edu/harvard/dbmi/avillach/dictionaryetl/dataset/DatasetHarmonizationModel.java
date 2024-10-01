package edu.harvard.dbmi.avillach.dictionaryetl.dataset;

import jakarta.persistence.*;

@Entity
@Table(name = "dataset_harmonization")
public class DatasetHarmonizationModel {
    @Id
    @SequenceGenerator(name = "datasetHarmonizationSeq", sequenceName = "dataset_harmonization_dataset_harmonization_id_seq", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "datasetHarmonizationSeq")
    @Column(name = "dataset_harmonization_id")
    private Long datasetHarmonizationId;

    @Column(name = "harmonized_dataset_id")
    private Long harmonizedDatasetId;

    @Column(name = "source_dataset_id")
    private Long sourceDatasetId;

    public DatasetHarmonizationModel(Long harmonizedDatasetId, Long sourceDatasetId) {
        this.harmonizedDatasetId = harmonizedDatasetId;
        this.sourceDatasetId = sourceDatasetId;
    }

    public Long getDatasetHarmonizationId() {
        return this.datasetHarmonizationId;
    }

    public void setDatasetHarmonizationId(Long datasetHarmonizationId) {
        this.datasetHarmonizationId = datasetHarmonizationId;
    }

    public Long getHarmonizedDatasetId() {
        return this.harmonizedDatasetId;
    }

    public void setHarmonizedDatasetId(Long harmonizedDatasetId) {
        this.harmonizedDatasetId = harmonizedDatasetId;
    }

    public Long getSourceDatasetId() {
        return this.sourceDatasetId;
    }

    public void setSourceDatasetId(Long sourceDatasetId) {
        this.sourceDatasetId = sourceDatasetId;
    }

}
