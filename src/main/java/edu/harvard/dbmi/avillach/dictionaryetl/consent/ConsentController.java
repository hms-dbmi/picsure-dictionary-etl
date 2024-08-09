package edu.harvard.dbmi.avillach.dictionaryetl.consent;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import edu.harvard.dbmi.avillach.dictionaryetl.dataset.DatasetRepository;

@CrossOrigin(origins = "http://localhost:8081")
@RestController
@RequestMapping("/api")
public class ConsentController {
    @Autowired
    ConsentRepository consentRepository;

    @Autowired
    DatasetRepository datasetRepository;

    @GetMapping("/consent")
    public ResponseEntity<List<ConsentModel>> getAllConsentModels(@RequestParam(required = false) String datasetRef) {
        try {
            List<ConsentModel> consentModels = new ArrayList<ConsentModel>();

            if (datasetRef == null) {
                // get all consents in dictionary
                consentRepository.findAll().forEach(consentModels::add);
            } else {
                // get all consents in specific dataset
                Long datasetId = datasetRepository.findByDatasetRef(datasetRef).get().getDatasetId();
                consentRepository.findByDatasetId(datasetId).forEach(consentModels::add);

            }
            if (consentModels.isEmpty()) {
                return new ResponseEntity<>(HttpStatus.NO_CONTENT);
            }
            return new ResponseEntity<>(consentModels, HttpStatus.OK);
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PutMapping("/consent")
    public ResponseEntity<ConsentModel> updateConsent(@RequestParam String datasetRef, @RequestParam String consentCode,
            @RequestParam String description, @RequestParam String authz, @RequestParam Long participantCount,
            @RequestParam Long variableCount, @RequestParam Long sampleCount) {
        Long datasetId = datasetRepository.findByDatasetRef(datasetRef).get().getDatasetId();
        Optional<ConsentModel> consentData = consentRepository.findByConsentCodeAndDatasetId(consentCode, datasetId);

        if (consentData.isPresent()) {
            // update already existing consent
            ConsentModel existingConsent = consentData.get();
            existingConsent.setDatasetId(datasetId);
            existingConsent.setConsentCode(consentCode);
            existingConsent.setDescription(description);
            existingConsent.setAuthz(authz);
            existingConsent.setParticipantCount(participantCount);
            existingConsent.setVariableCount(variableCount);
            existingConsent.setSampleCount(sampleCount);
            return new ResponseEntity<>(consentRepository.save(existingConsent), HttpStatus.OK);
        } else {
            // add new consent when consent not present in data
            try {
                ConsentModel newConsent = consentRepository
                        .save(new ConsentModel(datasetId, consentCode,
                                description, authz, participantCount,
                                variableCount, sampleCount));
                return new ResponseEntity<>(newConsent, HttpStatus.CREATED);
            } catch (Exception e) {
                System.out.println(e.getLocalizedMessage());
                return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
            }
        }

    }

    @DeleteMapping("/consent")
    public ResponseEntity<ConsentModel> deleteConsent(@RequestParam String consentCode,
            @RequestParam String datasetRef) {

        Long datasetId = datasetRepository.findByDatasetRef(datasetRef).get().getDatasetId();
        Optional<ConsentModel> consentData = consentRepository.findByConsentCodeAndDatasetId(consentCode, datasetId);

        if (consentData.isPresent()) {
            consentRepository.delete(consentData.get());
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        } else {
            // add new consent when consent not present in data
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }
}
