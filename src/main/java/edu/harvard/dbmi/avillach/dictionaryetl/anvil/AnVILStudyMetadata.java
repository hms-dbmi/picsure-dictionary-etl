package edu.harvard.dbmi.avillach.dictionaryetl.anvil;

import edu.harvard.dbmi.avillach.dictionaryetl.consent.ConsentModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetMetadataKeys;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetMetadataModel;
import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetModel;

import java.util.ArrayList;
import java.util.List;

/**
 * This model is used to parse the AnVIL Dataset that has been curated by a member of our team
 */
public class AnVILStudyMetadata {

    private String abbreviation; // Example: GTEx
    private String name; // Example: A Genomic Atlas of Systemic Interindividual Epigenetic Variation in Humans (GTEx)
    private String clinicalVariables; // Example: 11
    private String participants; // Example: 194
    private String samplesSequenced; // Example: 830
    private String link; // Example: https://anvilproject.org/data/studies/phs001746
    private String accession; // Example: phs001746.v2.p2
    private String phsVal; // Example: phs001746 Note: (parsed from accession at ingestion)
    private String studyFocus; // Example: epigenetic variation (Possibly, Unspecified)

    public AnVILStudyMetadata() {
    }

    public AnVILStudyMetadata(String abbreviation, String name, String clinicalVariables, String participants, String samplesSequenced, String link, String accession, String studyFocus) {
        this.abbreviation = abbreviation;
        this.name = name;
        this.clinicalVariables = clinicalVariables;
        this.participants = participants;
        this.samplesSequenced = samplesSequenced;
        this.link = link;
        this.accession = accession;
        this.studyFocus = studyFocus;
    }

    public DatasetModel generateDataset() {
        DatasetModel datasetModel = new DatasetModel();
        datasetModel.setAbbreviation(this.getAbbreviation());
        datasetModel.setRef(this.getPhsVal());
        datasetModel.setFullName(this.getName());
        return datasetModel;
    }

    public ConsentModel generateConsent() {
        ConsentModel consentModel = new ConsentModel();
        consentModel.setSampleCount(Long.valueOf(this.samplesSequenced));
        consentModel.setParticipantCount(Long.valueOf(this.participants));
        consentModel.setVariableCount(Long.valueOf(this.clinicalVariables));
        consentModel.setConsentCode("");
        consentModel.setDescription("");
        consentModel.setAuthz("");
        return consentModel;
    }

    public List<DatasetMetadataModel> generateDatasetMetadata() {
        List<DatasetMetadataModel> metadata = new ArrayList<>();
        String[] splitAccession = this.accession.split("\\.");
        metadata.add(new DatasetMetadataModel(DatasetMetadataKeys.version.name(), splitAccession[1]));
        metadata.add(new DatasetMetadataModel(DatasetMetadataKeys.phase.name(), splitAccession[2]));
        metadata.add(new DatasetMetadataModel(DatasetMetadataKeys.study_accession.name(), this.accession));
        metadata.add(new DatasetMetadataModel(DatasetMetadataKeys.study_link.name(), this.link));
        metadata.add(new DatasetMetadataModel(DatasetMetadataKeys.study_focus.name(), this.studyFocus));

        return metadata;
    }

    public String getAbbreviation() {
        return abbreviation;
    }

    public void setAbbreviation(String abbreviation) {
        this.abbreviation = abbreviation;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getClinicalVariables() {
        return clinicalVariables;
    }

    public void setClinicalVariables(String clinicalVariables) {
        this.clinicalVariables = clinicalVariables;
    }

    public String getParticipants() {
        return participants;
    }

    public void setParticipants(String participants) {
        this.participants = participants;
    }

    public String getSamplesSequenced() {
        return samplesSequenced;
    }

    public void setSamplesSequenced(String samplesSequenced) {
        this.samplesSequenced = samplesSequenced;
    }

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public String getAccession() {
        return accession;
    }

    public void setAccession(String accession) {
        this.accession = accession;
    }

    public String getStudyFocus() {
        return studyFocus;
    }

    public void setStudyFocus(String studyFocus) {
        this.studyFocus = studyFocus;
    }

    public String getPhsVal() {
        return phsVal;
    }

    public void setPhsVal(String phsVal) {
        this.phsVal = phsVal;
    }

}
