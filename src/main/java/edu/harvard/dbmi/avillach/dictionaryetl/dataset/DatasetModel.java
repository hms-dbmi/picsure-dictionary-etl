package edu.harvard.dbmi.avillach.dictionaryetl.dataset;

import jakarta.persistence.*;

@Entity
@Table(name = "dataset")
public class DatasetModel {
    @Id
    @SequenceGenerator(name = "datasetSeq", sequenceName = "dataset_dataset_id_seq", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "datasetSeq")
    @Column(name = "dataset_id")
    private Long datasetId;

    @Column(name = "ref")
    private String ref;

    @Column(name = "full_name")
    private String fullName;

    @Column(name = "abbreviation")
    private String abbreviation;

    @Column(name = "description")
    private String description;

    public DatasetModel() {

    }

    public DatasetModel(String ref, String fullName, String abbreviation, String description) {
        this.ref = ref;
        this.fullName = fullName;
        this.abbreviation = abbreviation;
        this.description = description;
    }

    public Long getDatasetId() {
        return this.datasetId;
    }

    public void setDatasetId(Long dataset_id) {
        this.datasetId = dataset_id;
    }

    public String getRef() {
        return this.ref;
    }

    public void setRef(String ref) {
        this.ref = ref;
    }

    public String getFullName() {
        return this.fullName;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
    }

    public String getAbbreviation() {
        return this.abbreviation;
    }

    public void setAbbreviation(String abbreviation) {
        this.abbreviation = abbreviation;
    }

    public String getDescription() {
        return this.description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return "{" +
                " dataset_id='" + getDatasetId() + "'" +
                ", ref='" + getRef() + "'" +
                ", full_name='" + getFullName() + "'" +
                ", abbreviation='" + getAbbreviation() + "'" +
                ", description='" + getDescription() + "'" +
                "}";
    }

}
