package edu.harvard.dbmi.avillach.dictionaryetl.concept;

import jakarta.persistence.*;

@Entity
@Table(name = "concept_node")
public class ConceptModel {
    @Id
    @SequenceGenerator(name = "conceptNodeSeq", sequenceName = "concept_node_concept_node_id_seq", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "conceptNodeSeq")
    @Column(name = "concept_node_id")
    private Long conceptNodeId;

    @Column(name = "dataset_id")
    private Long datasetId;

    @Column(name = "name")
    private String name;

    @Column(name = "display")
    private String display;

    @Column(name = "concept_type")
    private String conceptType;

    @Column(name = "concept_path")
    private String conceptPath;

    @Column(name = "parent_id")
    private Long parentId;

    public ConceptModel() {

    }

    public ConceptModel(Long dataset_id, String name, String display, String concept_type, String concept_path,
            Long parent_id) {
        this.datasetId = dataset_id;
        this.name = name;
        this.display = display;
        this.conceptType = concept_type;
        this.conceptPath = concept_path;
        this.parentId = parent_id;
    }

    public Long getConceptNodeId() {
        return this.conceptNodeId;
    }

    public void setConceptNodeId(Long concept_node_id) {
        this.conceptNodeId = concept_node_id;
    }

    public Long getDatasetId() {
        return this.datasetId;
    }

    public void setDatasetId(Long dataset_id) {
        this.datasetId = dataset_id;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDisplay() {
        return this.display;
    }

    public void setDisplay(String display) {
        this.display = display;
    }

    public String getConceptType() {
        return this.conceptType;
    }

    public void setConceptType(String concept_type) {
        this.conceptType = concept_type;
    }

    public String getConceptPath() {
        return this.conceptPath;
    }

    public void setConceptPath(String concept_path) {
        this.conceptPath = concept_path;
    }

    public Long getParentId() {
        return this.parentId;
    }

    public void setParentId(Long parent_id) {
        this.parentId = parent_id;
    }

    @Override
    public String toString() {
        return "{" +
                " concept_node_id='" + getConceptNodeId() + "'" +
                ", dataset_id='" + getDatasetId() + "'" +
                ", name='" + getName() + "'" +
                ", display='" + getDisplay() + "'" +
                ", concept_type='" + getConceptType() + "'" +
                ", concept_path='" + getConceptPath() + "'" +
                ", parent_id='" + getParentId() + "'" +
                "}";
    }

}
