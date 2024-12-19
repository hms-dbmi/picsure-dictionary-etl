package edu.harvard.dbmi.avillach.dictionaryetl.concept;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Service
public class ConceptService {

    private final ConceptRepository conceptRepository;

    @Autowired
    public ConceptService(ConceptRepository conceptRepository) {
        this.conceptRepository = conceptRepository;
    }

    public ConceptModel save(ConceptModel conceptModel) {
        return this.conceptRepository.save(conceptModel);
    }

    public Optional<ConceptModel> findByConcept(String conceptPath) {
        return this.conceptRepository.findByConceptPath(conceptPath);
    }

    public List<ConceptModel> findAll() {
        return this.conceptRepository.findAll();
    }

    public void deleteAll() {
        this.conceptRepository.deleteAll();
    }
}
