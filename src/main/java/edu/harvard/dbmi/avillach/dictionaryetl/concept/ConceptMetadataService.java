package edu.harvard.dbmi.avillach.dictionaryetl.concept;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Service
public class ConceptMetadataService {

    private final ConceptMetadataRepository conceptMetadataRepository;

    @Autowired
    public ConceptMetadataService(ConceptMetadataRepository conceptMetadataRepository) {
        this.conceptMetadataRepository = conceptMetadataRepository;
    }

    public ConceptMetadataModel save(ConceptMetadataModel conceptMetadataModel) {
        return this.conceptMetadataRepository.save(conceptMetadataModel);
    }

    public List<ConceptMetadataModel> findByConceptID(Long conceptID) {
        if (conceptID == null) {
            return List.of();
        }

        return this.conceptMetadataRepository.findByConceptNodeId(conceptID);
    }

    public Optional<ConceptMetadataModel> findByID(Long conceptMetaID) {
        return this.conceptMetadataRepository.findById(conceptMetaID);
    }

    public void deleteAll() {
        this.conceptMetadataRepository.deleteAll();
    }


    /**
     * Returns a list of all the metadata keys in the database for the concept node
     * The keys are sorted in alphabetical order
     * @return A list of all the metadata keys
     */
    public List<String> allMetadataKeys() {
        List<String> keyValues = this.conceptMetadataRepository.findAllKeyValues();
        Collections.sort(keyValues);
        return keyValues;
    }

    public List<String> findMetadataKeysByDatasetID(Long[] datasetIDs) {
        List<String> metadata = this.conceptMetadataRepository.findByDatasetID(datasetIDs);
        Collections.sort(metadata);
        return metadata;
    }

}
