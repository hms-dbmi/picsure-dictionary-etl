package edu.harvard.dbmi.avillach.dictionaryetl.consent;

import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ConsentService {

    private final ConsentRepository consentRepository;

    public ConsentService(ConsentRepository consentRepository) {
        this.consentRepository = consentRepository;
    }

    public List<ConsentModel> findByDatasetID(Long datasetId) {
        return consentRepository.findByDatasetId(datasetId);
    }

    public int deleteByDatasetRef(String ref) {
        return consentRepository.deleteByDatasetRef(ref);
    }
}
