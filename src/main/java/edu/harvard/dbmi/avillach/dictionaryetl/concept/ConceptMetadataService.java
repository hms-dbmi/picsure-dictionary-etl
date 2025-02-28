package edu.harvard.dbmi.avillach.dictionaryetl.concept;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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
        return this.conceptMetadataRepository.findByConceptNodeId(conceptID);
    }

    public Optional<ConceptMetadataModel> findByID(Long conceptMetaID) {
        return this.conceptMetadataRepository.findById(conceptMetaID);
    }

    public void deleteAll() {
        this.conceptMetadataRepository.deleteAll();
    }

    

}
