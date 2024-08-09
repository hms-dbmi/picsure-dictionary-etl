package edu.harvard.dbmi.avillach.dictionaryetl.consent;

import jakarta.persistence.*;

@Entity
@Table(name = "consent")
public class ConsentModel {
    @Id
    @SequenceGenerator(name = "consentNodeSeq", sequenceName = "consent_node_consent_node_id_seq", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "consentNodeSeq")
    @Column(name = "consent_id")
    private Long consentId;

    @Column(name = "dataset_id")
    private Long datasetId;

    @Column(name = "consent_code")
    private String consentCode;

    @Column(name = "description")
    private String description;

    @Column(name = "authz")
    private String authz;

    @Column(name = "participant_count")
    private Long participantCount;

    @Column(name = "variable_count")
    private Long variableCount;

    @Column(name = "sample_count")
    private Long sampleCount;

    public Long getConsentId() {
        return this.consentId;
    }

    public void setConsentId(Long consentId) {
        this.consentId = consentId;
    }

    public Long getDatasetId() {
        return this.datasetId;
    }

    public void setDatasetId(Long datasetId) {
        this.datasetId = datasetId;
    }

    public String getConsentCode() {
        return this.consentCode;
    }

    public void setConsentCode(String consentCode) {
        this.consentCode = consentCode;
    }

    public String getDescription() {
        return this.description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getAuthz() {
        return this.authz;
    }

    public void setAuthz(String authz) {
        this.authz = authz;
    }

    public Long getParticipantCount() {
        return this.participantCount;
    }

    public void setParticipantCount(Long participantCount) {
        this.participantCount = participantCount;
    }

    public Long getVariableCount() {
        return this.variableCount;
    }

    public void setVariableCount(Long variableCount) {
        this.variableCount = variableCount;
    }

    public Long getSampleCount() {
        return this.sampleCount;
    }

    public void setSampleCount(Long sampleCount) {
        this.sampleCount = sampleCount;
    }

    public ConsentModel(Long datasetId, String consentCode, String description, String authz, Long participantCount,
            Long variableCount, Long sampleCount) {

        this.datasetId = datasetId;
        this.consentCode = consentCode;
        this.description = description;
        this.authz = authz;
        this.participantCount = participantCount;
        this.variableCount = variableCount;
        this.sampleCount = sampleCount;
    }

    public ConsentModel() {

    }

    @Override
    public String toString() {
        return "{" +
                " consentId='" + getConsentId() + "'" +
                ", datasetId='" + getDatasetId() + "'" +
                ", consentCode='" + getConsentCode() + "'" +
                ", description='" + getDescription() + "'" +
                ", authz='" + getAuthz() + "'" +
                ", participantCount='" + getParticipantCount() + "'" +
                ", variableCount='" + getVariableCount() + "'" +
                ", sampleCount='" + getSampleCount() + "'" +
                "}";
    }

}
