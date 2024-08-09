package edu.harvard.dbmi.avillach.dictionaryetl.dataset;

import jakarta.persistence.*;

@Entity
@Table(name = "dataset_meta")
public class DatasetMetadataModel {
    @Id
    @SequenceGenerator(name = "datasetMetaSeq", sequenceName = "dataset_meta_dataset_meta_id_seq", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "datasetMetaSeq")
    @Column(name = "dataset_meta_id")
    private Long datasetMetaId;

    @Column(name = "dataset_id")
    private Long datasetId;

    @Column(name = "key")
    private String key;

    @Column(name = "value")
    private String value;

    public DatasetMetadataModel() {

    }

    public DatasetMetadataModel(Long datasetId, String key, String value) {
        this.datasetId = datasetId;
        this.key = key;
        this.value = value;
    }

    public Long getDatasetMetaId() {
        return this.datasetMetaId;
    }

    public void setDatasetMetaId(Long datasetMetaId) {
        this.datasetMetaId = datasetMetaId;
    }

    public Long getDatasetId() {
        return this.datasetId;
    }

    public void setDatasetId(Long datasetId) {
        this.datasetId = datasetId;
    }

    public String getKey() {
        return this.key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return this.value;
    }

    public void setValue(String value) {
        this.value = value;
    }

}
