package edu.harvard.dbmi.avillach.dictionaryetl.concept;

import jakarta.persistence.*;

@Entity
@Table(name = "concept_node_meta")
public class ConceptMetadataModel {
    @Id
    @SequenceGenerator(name = "conceptMetaSeq", sequenceName = "concept_node_meta_concept_node_meta_id_seq", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "conceptMetaSeq")
    @Column(name = "concept_node_meta_id")
    private Long conceptNodeMetaId;

    @Column(name = "concept_node_id")
    private Long conceptNodeId;

    @Column(name = "key")
    private String key;

    @Column(name = "value")
    private String value;

    public ConceptMetadataModel() {

    }

    public ConceptMetadataModel(Long conceptNodeId, String key, String value) {
        this.conceptNodeId = conceptNodeId;
        this.key = key;
        this.value = value;
    }

    public Long getConceptMetaId() {
        return this.conceptNodeMetaId;
    }

    public void setConceptMetaId(Long conceptNodeMetaId) {
        this.conceptNodeMetaId = conceptNodeMetaId;
    }

    public Long getConceptNodeId() {
        return this.conceptNodeId;
    }

    public void setConceptNodeId(Long conceptNodeId) {
        this.conceptNodeId = conceptNodeId;
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
